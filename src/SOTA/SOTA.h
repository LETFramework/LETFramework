#ifndef _SOTA_H
#define _SOTA_H
#include"ElasticSketch/ElasticSketch.h"
#include"maxlogOPH/maxlogOPH.h"
#include"JoinSketch/Choose_Ske.h"
class SOTA
{
	uint32_t mem_in_byte;
	ElasticSketch* elastic;
	MaxLogOPH* maxlogoph;
	Sketch* js;
public:
	SOTA(uint32_t _mem_in_byte):mem_in_byte(_mem_in_byte){
		uint32_t each_mem = mem_in_byte / 3;
		elastic = new ElasticSketch(int(0.25 * each_mem / 64), each_mem);
		maxlogoph = new MaxLogOPH(each_mem);
		js = Choose_Sketch(each_mem, 3, rand());
	}
	SOTA(const SOTA& _refer) :SOTA(_refer.mem_in_byte){}
	void initial()
	{
		maxlogoph->initial();
	}
	void initial(const SOTA& _refer)
	{
		maxlogoph->initial(_refer.maxlogoph);
	}
	void insert(key_type key)
	{
		elastic->insert(key);
		maxlogoph->insert(key);
		js->Insert(key);
	}
	void clear()
	{
		elastic->clear();
		delete js;
		js = Choose_Sketch(mem_in_byte / 3, 3, rand());
	}
	double cos(const SOTA& B)
	{
		auto g = [](double x)->double {return x * x; };
		auto g1 = [](double x)->double {return x; };
		return js->Join(B.js, g1) / sqrt(js->gsum(g)) / sqrt(B.js->gsum(g));
	}
	double innerProduction(const SOTA& B)
	{
		auto g1 = [](double x)->double {return x; };
		return js->Join(B.js, g1);
	}
	double jaccard(const SOTA& B)
	{
		return maxlogoph->jaccard(B.maxlogoph);
	}
	double query(key_type key)
	{
		return elastic->query(key);
	}
	double gsum(double (*g)(double))
	{
		return elastic->gsum(g);
	}
	void get_heavy_hitters(int threshold, vector<pair<string, uint32_t>>& results)
	{
		elastic->get_heavy_hitters(threshold, results);
	}
	~SOTA()
	{
		delete elastic;
		delete js;
		delete maxlogoph;
	}
};
#endif // !_sota_H
