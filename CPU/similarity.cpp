#pragma GCC optimize(3,"Ofast","inline")
#include <iostream>
#include "./common/trace.h"
#include "./LETFramework/LETFramework.h"
#include"./SOTA/SOTA.h"
#include"common/config.h"
#include <fstream>
#include <cstring>
#include <stdio.h>
#include <sstream>
#include <fstream>
using namespace std;

double topk_ratio, count_ratio, lambda;
uint32_t total_mem, d;
string dataset_path, output_file;
uint32_t mem_start, mem_stop, mem_step;
const int read_num = -1;
const int split_range = 10000000;
enum DATA_TYPE {
	CAIDA,
	zipf,
	webdocs
}dataset_type;
enum TASK
{
	COSINE,
	JACCARD,
	INNER_PRODUCTION
}task;
double g1_sqr(double x) {
	return (x > 0) ? x * x : 0;
}
double g1_one(double x) {
	return (x > 0) ? 1 : 0;
}
double g1_sum(double x) {
	return (x > 0) ? x : 0;
}
double g2_mul(double x, double y) {
	return (x > 0 && y > 0) ? x * y : 0;
}

double gt_gsum(double (*g)(double), vector<pair<int, key_type>>& gt) {
	double sum = 0;
	for (auto p : gt) { //[val, key]
		sum += g(p.first);
	}
	return sum;
}

double gt_gsum(double (*g)(double, double), vector<pair<int, key_type>>& gt0,
	vector<pair<int, key_type>>& gt1) {
	unordered_map<key_type, int> mp;
	for (auto p : gt1) {//[val, key]
		mp[p.second] = p.first;
	}
	double sum = 0;
	for (auto p : gt0) { //[val, key]
		sum += g(p.first, mp[p.second]);
	}
	return sum;
}

double calc_cos(double join_sqr, double sqr1, double sqr2) {
	return (join_sqr - sqr1 - sqr2) / 2 / sqrt(sqr1) / sqrt(sqr2);
}

double calc_jaccard(double join, double sz1, double sz2) {
	return join / (sz1 + sz2 - join);
}

double calc_innerProduction(double join_sqr, double sqr1, double sqr2)
{
	return (join_sqr - sqr1 - sqr2) / 2;
}

double gt_cos(vector<pair<int, key_type>>& gt0, vector<pair<int, key_type>>& gt1) {
	double s1 = gt_gsum(g2_mul, gt0, gt1);
	double s2 = gt_gsum(g1_sqr, gt0);
	double s3 = gt_gsum(g1_sqr, gt1);
	return s1 / sqrt(s2) / sqrt(s3);
}

double gt_jaccard(vector<pair<int, key_type>>& gt0, vector<pair<int, key_type>>& gt1) {
	double join = gt_gsum([](double x, double y)->double {return x != 0 && y != 0; }, gt0, gt1);
	double unionn = gt0.size() + gt1.size() - join;
	return join / unionn;
}

double gt_innerProduction(vector<pair<int, key_type>>& gt0, vector<pair<int, key_type>>& gt1) {
	return gt_gsum(g2_mul, gt0, gt1);
}

void split_dataset(vector<key_type>& dataset, vector<key_type>& dataset0,
	vector<key_type>& dataset1) {
	for (int i = 0; i < dataset.size(); i++) {
		if ((i / split_range) % 2 == 0) {
			dataset0.push_back(dataset[i]);
		}
		else {
			dataset1.push_back(dataset[i]);
		}
	}
}
void gsum1(uint32_t total_mem, uint32_t elastic_mem, uint32_t univmon_mem, double count_ratio, uint32_t d,
	vector<key_type>& dataset0, vector<key_type>& dataset1, double real_sum,
	double& avg_eu_err, double& avg_hu_err, double& avg_su_err, double& avg_lu_err, double& avg_univmon_err, double& avg_sota_err) {
	int T = 5;
	avg_eu_err = avg_hu_err = avg_su_err = avg_lu_err = avg_sota_err = avg_univmon_err = 0;

	LETFramework eu(elastic_mem / 2, univmon_mem / 2, 0, d, count_ratio), eu1(eu);
	LETFramework hu(elastic_mem / 2, univmon_mem / 2, 2, d, count_ratio), hu1(hu);
	LETFramework su(elastic_mem / 2, univmon_mem / 2, 1, d, count_ratio), su1(su);
	LETFramework lu(elastic_mem / 2, univmon_mem / 2, 3, d, count_ratio), lu1(lu);
	LETFramework univmon(0, total_mem / 2), univmon1(univmon);
	SOTA sota(total_mem / 2, 4.0 / 8, 3.0 / 8, 1.0 / 8), sota1(sota);
	srand(time(0));
	for (int i = 0; i < T; i++) {
		eu.initial();
		eu1.initial(eu);
		hu.initial();
		hu1.initial(hu);
		su.initial();
		su1.initial(su);
		lu.initial();
		lu1.initial(lu);
		univmon.initial();
		univmon1.initial(univmon);
		sota.initial();
		sota1.initial(sota);
		int insert_cnt = 0;
		for (auto key : dataset0) {
			eu.insert(key);
			hu.insert(key);
			su.insert(key);
			lu.insert(key);
			univmon.insert(key);
			sota.insert(key);
		}
		insert_cnt = 0;
		for (auto key : dataset1) {
			eu1.insert(key);
			hu1.insert(key);
			su1.insert(key);
			lu1.insert(key);
			univmon1.insert(key);
			sota1.insert(key);
		}
		double eu_sum = 0, sota_sum = 0, hu_sum = 0, su_sum = 0, lu_sum = 0, univmon_sum = 0;
		switch (task)
		{
		case COSINE:
			eu_sum = calc_cos(LETFramework::gsum_add(eu, eu1, g1_sqr), eu.gsum(g1_sqr), eu1.gsum(g1_sqr));
			hu_sum = calc_cos(LETFramework::gsum_add(hu, hu1, g1_sqr), hu.gsum(g1_sqr), hu1.gsum(g1_sqr));
			su_sum = calc_cos(LETFramework::gsum_add(su, su1, g1_sqr), su.gsum(g1_sqr), su1.gsum(g1_sqr));
			lu_sum = calc_cos(LETFramework::gsum_add(lu, lu1, g1_sqr), lu.gsum(g1_sqr), lu1.gsum(g1_sqr));
			univmon_sum = calc_cos(LETFramework::gsum_add(univmon, univmon1, g1_sqr), univmon.gsum(g1_sqr), univmon1.gsum(g1_sqr));
			sota_sum = sota.cos(sota1);
			break;
		case JACCARD:
			eu_sum = calc_jaccard(LETFramework::gsum_mul(eu, eu1, g1_one), eu.gsum(g1_one), eu1.gsum(g1_one));
			hu_sum = calc_jaccard(LETFramework::gsum_mul(hu, hu1, g1_one), hu.gsum(g1_one), hu1.gsum(g1_one));
			su_sum = calc_jaccard(LETFramework::gsum_mul(su, su1, g1_one), su.gsum(g1_one), su1.gsum(g1_one));
			lu_sum = calc_jaccard(LETFramework::gsum_mul(lu, lu1, g1_one), lu.gsum(g1_one), lu1.gsum(g1_one));
			univmon_sum = calc_jaccard(LETFramework::gsum_mul(univmon, univmon1, g1_one), univmon.gsum(g1_one), univmon1.gsum(g1_one));
			sota_sum = sota.jaccard(sota1);
			break;
		case INNER_PRODUCTION:
			eu_sum = calc_innerProduction(LETFramework::gsum_add(eu, eu1, g1_sqr), eu.gsum(g1_sqr), eu1.gsum(g1_sqr));
			hu_sum = calc_innerProduction(LETFramework::gsum_add(hu, hu1, g1_sqr), hu.gsum(g1_sqr), hu1.gsum(g1_sqr));
			su_sum = calc_innerProduction(LETFramework::gsum_add(su, su1, g1_sqr), su.gsum(g1_sqr), su1.gsum(g1_sqr));
			lu_sum = calc_innerProduction(LETFramework::gsum_add(lu, lu1, g1_sqr), lu.gsum(g1_sqr), lu1.gsum(g1_sqr));
			univmon_sum = calc_innerProduction(LETFramework::gsum_add(univmon, univmon1, g1_sqr), univmon.gsum(g1_sqr), univmon1.gsum(g1_sqr));
			sota_sum = sota.innerProduction(sota1);
			break;
		default:
			break;
		}

		double eu_err, sota_err, hu_err, su_err, lu_err, univmon_err;

		eu_err = fabs(eu_sum - real_sum) / real_sum;
		hu_err = fabs(hu_sum - real_sum) / real_sum;
		su_err = fabs(su_sum - real_sum) / real_sum;
		lu_err = fabs(lu_sum - real_sum) / real_sum;
		univmon_err = fabs(univmon_sum - real_sum) / real_sum;
		sota_err = fabs(sota_sum - real_sum) / real_sum;

		avg_eu_err += eu_err;
		avg_hu_err += hu_err;
		avg_su_err += su_err;
		avg_lu_err += lu_err;
		avg_univmon_err += univmon_err;
		avg_sota_err += sota_err;

		eu.clear();
		eu1.clear();
		hu.clear();
		hu1.clear();
		su.clear();
		su1.clear();
		lu.clear();
		lu1.clear();
		univmon.clear();
		univmon1.clear();
		sota.clear();
		sota1.clear();
	}
	avg_eu_err /= T;
	avg_hu_err /= T;
	avg_su_err /= T;
	avg_lu_err /= T;
	avg_univmon_err /= T;
	avg_sota_err /= T;
}

void gsum(uint32_t total_mem, double topk_ratio, double count_ratio, uint32_t d, ofstream& out) {
	cout << "********MEM = " << total_mem << "********" << endl;
	uint32_t elastic_mem = total_mem * topk_ratio, univmon_mem = total_mem - elastic_mem;
	vector<key_type> dataset0, dataset1;
	vector<key_type> dataset;
	int read_num = -1;
	switch (dataset_type)
	{
	case CAIDA:dataset = loadCAIDA18(read_num, dataset_path.c_str()); break;
	case zipf:dataset = readFile_zipf(dataset_path.c_str()); break;
	case webdocs:dataset = loadWebdocs(read_num, dataset_path.c_str()); break;
	default:
		break;
	}
	split_dataset(dataset, dataset0, dataset1);
	vector<pair<int, key_type>> gt0 = groundtruth(dataset0);
	vector<pair<int, key_type>> gt1 = groundtruth(dataset1);
	double real_sum = 0;
	switch (task)
	{
	case COSINE:
		real_sum = gt_cos(gt0, gt1);
		break;
	case JACCARD:
		real_sum = gt_jaccard(gt0, gt1);
		break;
	case INNER_PRODUCTION:
		real_sum = gt_innerProduction(gt0, gt1);
		break;
	default:
		break;
	}

	cout << "total mem = " << total_mem << endl;
	cout << "elastic mem = " << elastic_mem << endl;
	cout << "univmon mem = " << univmon_mem << endl;
	double eu_err, sota_err, hu_err, su_err, lu_err, univmon_err;
	gsum1(total_mem, elastic_mem, univmon_mem, count_ratio, d, dataset0, dataset1, real_sum, eu_err, hu_err, su_err, lu_err, univmon_err, sota_err);
	cout << "real: " << real_sum << endl;
	cout << "E-LETFramework: " << eu_err << endl;
	cout << "H-LETFramework:" << hu_err << endl;
	cout << "S-LETFramework:" << su_err << endl;
	cout << "L-LETFramework:" << lu_err << endl;
	cout << "Univmon:" << univmon_err << endl;
	cout << "SOTA: " << sota_err << endl;
	out << total_mem / 1000 << "," << eu_err << "," << hu_err << "," << su_err << "," << lu_err
		<< "," << univmon_err << "," << sota_err << "," << endl;
}
void config(char* argv[])
{
	map<string, string> m_mapConfigInfo;
	ConfigFileInit(m_mapConfigInfo, argv[0]);
	dataset_path = argv[1];
	dataset_type = DATA_TYPE(stoi(string(argv[2])));
	string s = argv[3], echo;
	if (s.find("cosine") != -1)
	{
		task = TASK::COSINE;
		echo = "cosine";
	}
	else if (s.find("jaccard") != -1)
	{
		task = TASK::JACCARD;
		echo = "jaccard";
	}
	else if (s.find("innerProduction") != -1)
	{
		task = TASK::INNER_PRODUCTION;
		echo = "innerProduction";
	}
	output_file = to_string(dataset_type) + "_similarity_" + echo + ".csv";
	lambda = stod(getValueString(m_mapConfigInfo, "lambda"));
	//total_mem = getValueInt(m_mapConfigInfo, "total_mem");
	mem_start = stoi(getValueString(m_mapConfigInfo, "mem_start"));
	mem_stop = stoi(getValueString(m_mapConfigInfo, "mem_stop"));
	mem_step = stoi(getValueString(m_mapConfigInfo, "mem_step"));
	topk_ratio = stod(getValueString(m_mapConfigInfo, "topk"));
	count_ratio = stod(getValueString(m_mapConfigInfo, "countSketch"));
	d = getValueInt(m_mapConfigInfo, "d");
	cout << "config list:" << endl;
	switch (dataset_type)
	{
	case CAIDA:cout << "CAIDA:" << dataset_path << endl; break;
	case zipf:cout << "zipf:" << dataset_path << endl; break;
	case webdocs:cout << "webdocs:" << dataset_path << endl; break;
	default:
		break;
	}
	cout << "g = " << echo << endl;
	cout << "lambda = " << lambda << endl;
	//cout << "total_mem = " << total_mem << endl;
	cout << "mem_start = " << mem_start << endl;
	cout << "mem_stop = " << mem_stop << endl;
	cout << "mem_step = " << mem_step << endl;
	cout << "total_mem = " << total_mem << endl;
	cout << "topk_ratio = " << topk_ratio << endl;
	cout << "countSketchRatio = " << count_ratio << endl;
	cout << "d = " << d << endl;
	cout << "result output = " << output_file << endl;
	cout << "------------------------------------" << endl;
}
int main(int argc, char* argv[]) {
	config(argv + 1);
	ofstream out("../result/" + output_file);
	out << "MEM(KB)" << "," << "E-LETFramework" << "," << "H-LETFramework" << ","
		<< "S-LETFramework" << "," << "L-LETFramework" << ","
		<< "univmon" << "," << "sota" << "," << endl;
	for (uint32_t total_mem = mem_start; total_mem < mem_stop; total_mem += mem_step)
	{
		gsum(total_mem, topk_ratio, count_ratio, d, out);
	}
	out.close();
	return 0;
}