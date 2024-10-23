#ifndef _LET_FRAMEWORK_H_
#define _LET_FRAMEWORK_H_
#include"topk/topk.h"
#include"USketch/UnivMon.h"
#include"topk/E-LETSketch/elastic.h"
#include"topk/H-LETSketch/HeavyGuarding.h"
#include"topk/F-LETSketch/frequent.h"
#include"topk/S-LETSketch/SpaceSaving.h"
#include<queue>
#include<thread>
//#include <zmq.hpp>
class LETFramework
{
public:
	static constexpr int level_num = 14;
	int heavy_sketch_mem;
	int univmon_mem;
	TopkPart* topk;
	USketchPart* univmon;
	uint32_t type;
	uint32_t d;
	double gama;
	uint32_t synchronized;
	LETFramework(int _heavy_sketch_mem, int _univmon_mem, uint32_t _type = 0, uint32_t _d = 1, double cu_ratio = 0.9, bool _synchronized = 0) :type(_type), heavy_sketch_mem(_heavy_sketch_mem),
		univmon_mem(_univmon_mem), d(_d), gama(1 - cu_ratio), synchronized(_synchronized) {
		switch (type)
		{
		case 0:topk = new ElasticSketchBased(heavy_sketch_mem); break;
		case 1:topk = new SpaceSavingBased(heavy_sketch_mem); break;
		case 2:topk = new HeavyGuardianBased(heavy_sketch_mem); break;
		case 3:topk = new FrequentBased(heavy_sketch_mem); break;
		default:
			break;
		}
		univmon = new USketchPart(univmon_mem, (heavy_sketch_mem) ? d : 5, gama);
	}
	LETFramework(const LETFramework& _refer) :type(_refer.type), heavy_sketch_mem(_refer.heavy_sketch_mem),
		univmon_mem(_refer.univmon_mem), d(_refer.d), gama(_refer.gama) {
		switch (type)
		{
		case 0:topk = new ElasticSketchBased(heavy_sketch_mem); break;
		case 1:topk = new SpaceSavingBased(heavy_sketch_mem); break;
		case 2:topk = new HeavyGuardianBased(heavy_sketch_mem); break;
		case 3:topk = new FrequentBased(heavy_sketch_mem); break;
		default:
			break;
		}
		univmon = new USketchPart(univmon_mem, (heavy_sketch_mem) ? d : 5, gama);
	}
	~LETFramework() {
		delete topk;
		delete univmon;
	}
	void clear() {
		topk->clear();
		univmon->clear();
	}
	void initial()
	{
		topk->initial();
		univmon->initial();
	}
	void initial(const LETFramework& _refer)
	{
		topk->initial(_refer.topk);
		univmon->initial(_refer.univmon);
	}
	pair<key_type, int> heavy_sketch_insert(key_type key, int f = 1) {
		if (heavy_sketch_mem == 0)
			return pair<key_type, int>(key, f);

		key_type swap_key;
		uint32_t swap_val = 0;

		int result = topk->insert(key, swap_key, swap_val, f);
		switch (result)
		{
		case 0: return pair<key_type, int>(0, 0);
		case 1: return pair<key_type, int>(swap_key, GetCounterVal(swap_val));
		case 2: return pair<key_type, int>(key, f);
		default:
			printf("error return value !\n");
			exit(1);
		}
	}

	void insert(key_type key, int f = 1) {
		pair<key_type, int> swap_pair = heavy_sketch_insert(key, f);
		if (univmon_mem != 0 && swap_pair.second != 0) {
			univmon->insert(swap_pair.first, swap_pair.second);
		}
	}
	void batch_insert(vector<key_type>& dataset)
	{
		if (synchronized == 0)
		{
			for (auto key : dataset)
			{
				insert(key);
			}
		}
		else
		{
			int hwm = 1e9;
			// 创建ZeroMQ上下文
			zmq::context_t context;

			// 创建ZeroMQ套接字，使用inproc模式
			zmq::socket_t senderSocket(context, ZMQ_PUB);
			zmq::socket_t receiverSocket(context, ZMQ_SUB);
			// 设置发送套接字的HWM
			senderSocket.setsockopt(ZMQ_SNDHWM, hwm);
			// 设置接收套接字的HWM
			receiverSocket.setsockopt(ZMQ_SNDHWM, hwm);

			// 绑定套接字到内部通信协议
			senderSocket.bind("inproc://channel");
			receiverSocket.connect("inproc://channel");
			// 订阅者订阅所有消息
			receiverSocket.setsockopt(ZMQ_SUBSCRIBE, "", 0);

			uint32_t cnt = 0, idx = 0;
			thread th2(&LETFramework::light_part_insert, this, std::ref(receiverSocket));
			pair<key_type, int> swap_pair;
			cout << "start\n";
			for (auto& key : dataset)
			{
				swap_pair = heavy_sketch_insert(key);
				if (univmon_mem != 0 && swap_pair.second != 0) {
					senderSocket.send(zmq::message_t(&swap_pair, sizeof(swap_pair)));
				}
			}
			swap_pair = make_pair(0, 0);
			senderSocket.send(zmq::message_t(&swap_pair, sizeof(swap_pair)));
			th2.join();
			// 关闭套接字
			senderSocket.close();
			receiverSocket.close();

			// 等待上下文终止
			context.close();
		}
	}
	void heavy_part_insert(zmq::socket_t& recvsocket, zmq::socket_t& sendsocket)
	{
		uint32_t cnt = 0;
		zmq::message_t message;
		while (true)
		{
			recvsocket.recv(&message);
			key_type key = *(key_type*)(message.data());
			if (key == 0)
			{
				break;
			}
			pair<key_type, int> swap_pair = heavy_sketch_insert(key);
			if (univmon_mem != 0 && swap_pair.second != 0) {
				sendsocket.send(zmq::message_t(&swap_pair, sizeof(swap_pair)));
				//socket1.send(zmq::message_t(&swap_pair, sizeof(swap_pair)));
			}
		}
	}
	void light_part_insert(zmq::socket_t& socket)
	{
		uint32_t cnt = 0;
		zmq::message_t message;
		while (true)
		{
			socket.recv(&message);
			pair<key_type, int> swap_pair = *(pair<key_type, int>*)(message.data());
			if (swap_pair.first == 0 && swap_pair.second == 0)
			{
				break;
			}
			univmon->insert(swap_pair.first, swap_pair.second);
		}
	}
	HashMap heavySketch_query_all() const
	{
		HashMap res0 = topk->query_all();
		HashMap res;
		for (auto p : res0) {
			int32_t heavy_result = p.second;
			res[p.first] = GetCounterVal(heavy_result);
		}
		return res;
	}
	double query(key_type key)
	{
		uint32_t heavy_result = (heavy_sketch_mem) ? topk->query(key) : 0;
		if (heavy_result == 0)
		{
			int light_result = univmon->query(key);
			return (int)GetCounterVal(heavy_result) + light_result;
		}
		return heavy_result;
	}
	double gsum(double (*g)(double)) {
		double heavy_sketch_gsum = 0;
		HashMap res = heavySketch_query_all();
		for (auto p : res) {
			heavy_sketch_gsum += g(p.second);
		}

		double univmon_gsum = univmon_mem == 0 ? 0 : univmon->gsum(g);
		return heavy_sketch_gsum + univmon_gsum;
	}
	double gsum_subset(double (*g)(double), const SubsetCheck* check, double real_sum) {
		double heavy_sketch_gsum = 0;
		HashMap all_res = topk->query_all();
		double sum = 0;
		for (auto p : all_res) {//[key, val]
			if ((*check)(p.first))
				sum += g(p.second);
		}

		double univmon_gsum = univmon_mem == 0 ? 0 : univmon->gsum_subset(g, check);
		return heavy_sketch_gsum + univmon_gsum;
	}
	void get_heavy_hitters(int threshold, vector<pair<string, uint32_t>>& results) {
		vector<pair<key_type, uint32_t>> univmon_result;
		HashMap mp = heavySketch_query_all();
		univmon->get_heavy_hitters(threshold, univmon_result);
		for (auto kv : univmon_result)
			mp[kv.first] = kv.second;
		for (auto kv : mp)
			if (kv.second >= threshold)
				results.push_back(make_pair(string((const char*)&kv.first, 4), kv.second));
	}
	static double gsum_add(const LETFramework& x, const LETFramework& y, double (*g)(double)) {
		double heavy_sketch_gsum = 0;
		HashMap res0 = x.heavySketch_query_all(), res1 = y.heavySketch_query_all();
		for (auto p : res1) { //[key, val]
			res0[p.first] += p.second;
		}
		double sum = 0;
		for (auto p : res0) { //[key, val]
			heavy_sketch_gsum += g(p.second);
		}
		double univmon_gsum = USketchPart::gsum_add(x.univmon, y.univmon, g, level_num);
		return univmon_gsum + heavy_sketch_gsum;
	}

	static double gsum_mul(const LETFramework& x, const LETFramework& y, double (*g)(double)) {
		double heavy_sketch_gsum = 0;
		HashMap res0 = x.heavySketch_query_all(), res1 = y.heavySketch_query_all();
		unordered_map<key_type, double> res;
		for (auto p : res1) { //[key, val]
			if (res0.count(p.first) > 0)
				res[p.first] = (double)(res0[p.first]) * (double)(res1[p.first]);
		}
		for (auto p : res) { // [key,val]
			heavy_sketch_gsum += g(p.second);
		}
		double univmon_gsum = USketchPart::gsum_mul(x.univmon, y.univmon, g, level_num);
		return univmon_gsum + heavy_sketch_gsum;
	}
};

#endif // _LET_FRAMEWORK_H_
