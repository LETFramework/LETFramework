#pragma GCC optimize(3,"Ofast","inline")
#include <iostream>
#include "common/config.h"
#include "./common/trace.h"
#include "./LETFramework/LETFramework.h"
#include"./SOTA/SOTA.h"
#include <fstream>
#include <cstring>
#include <stdio.h>
#include <sstream>
#include <fstream>
using namespace std;

double topk_ratio, count_ratio, lambda;
uint32_t mem_start, mem_stop, mem_step, d;
string dataset_path, output_file;
enum DATA_TYPE {
	CAIDA,
	zipf,
	webdocs
}dataset_type;
double (*g)(double);
double cardinality(double x) {
	return x > 0 ? 1 : 0;
}
double sum(double x)
{
	return x >= 0 ? x : 0;
}
double entropy(double x) {
	return (x > 0) ? x * log2(x) : 0;
}
double sqr(double x)
{
	return x >= 0 ? x * x : 0;
}

double gt_gsum(double (*g)(double), vector<pair<int, key_type>>& gt) {
	double sum = 0;
	for (auto p : gt) { // [val,key]
		sum += g(p.first);
	}
	return sum;
}

double gt_gsum_subset(double (*g)(double), const vector<pair<int, key_type>>& gt, const SubsetCheck* check) {
	double sum = 0;
	for (auto p : gt) {// [val,key]
		if ((*check)(p.second)) {
			sum += g(p.first);
		}
	}
	return sum;
}
void gsum1_subset(uint32_t total_mem, uint32_t elastic_mem, uint32_t univmon_mem, double count_ratio, uint32_t d,
	vector<key_type>& dataset, double (*g)(double), vector<pair<int, key_type>>& gt_subset,
	double& avg_eu_err, double& avg_hu_err, double& avg_su_err, double& avg_lu_err, double& avg_univmon_err, double& avg_sota_err) {
	int T = 5;
	avg_eu_err = avg_hu_err = avg_su_err = avg_lu_err = avg_sota_err = avg_univmon_err = 0;
	LETFramework eu(elastic_mem, univmon_mem, 0, d, count_ratio);
	LETFramework hu(elastic_mem, univmon_mem, 2, d, count_ratio);
	LETFramework su(elastic_mem, univmon_mem, 1, d, count_ratio);
	LETFramework lu(elastic_mem, univmon_mem, 3, d, count_ratio);
	LETFramework univmon(0, total_mem);
	SOTA sota(total_mem, 4.0 / 8, 3.0 / 8, 1.0 / 8);
	for (int i = 0; i < T; i++) {
		eu.initial();
		hu.initial();
		su.initial();
		lu.initial();
		univmon.initial();
		sota.initial();

		for (auto key : dataset) {
			eu.insert(key);
			hu.insert(key);
			su.insert(key);
			lu.insert(key);
			univmon.insert(key);
			sota.insert(key);
		}
		double hu_err = 0, su_err = 0, lu_err = 0, eu_err = 0, sota_err = 0, univmon_err = 0;
		for (auto p : gt_subset)
		{
			double real_sum = g(p.first);

			double eu_sum = g(eu.query(p.second));
			double hu_sum = g(hu.query(p.second));
			double su_sum = g(su.query(p.second));
			double lu_sum = g(lu.query(p.second));
			double univmon_sum = g(univmon.query(p.second));
			double sota_sum = g(sota.query(p.second));

			eu_err += fabs(eu_sum - real_sum);
			hu_err += fabs(hu_sum - real_sum);
			su_err += fabs(su_sum - real_sum);
			lu_err += fabs(lu_sum - real_sum);
			univmon_err += fabs(univmon_sum - real_sum);
			sota_err += fabs(sota_sum - real_sum);
		}
		eu_err /= gt_subset.size();
		hu_err /= gt_subset.size();
		su_err /= gt_subset.size();
		lu_err /= gt_subset.size();
		univmon_err /= gt_subset.size();
		sota_err /= gt_subset.size();

		avg_eu_err += eu_err;
		avg_hu_err += hu_err;
		avg_su_err += su_err;
		avg_lu_err += lu_err;
		avg_univmon_err += univmon_err;
		avg_sota_err += sota_err;

		eu.clear();
		hu.clear();
		su.clear();
		lu.clear();
		univmon.clear();
		sota.clear();
	}
	avg_eu_err /= T;
	avg_hu_err /= T;
	avg_su_err /= T;
	avg_lu_err /= T;
	avg_univmon_err /= T;
	avg_sota_err /= T;
}

void gsum(uint32_t total_mem, double topk_ratio, double count_ratio, uint32_t d, double (*g)(double), ofstream& out) {
	cout << "********MEM = " << total_mem << "********" << endl;
	uint32_t elastic_mem = total_mem * topk_ratio, univmon_mem = total_mem - elastic_mem;
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
	vector<pair<int, key_type>> gt = groundtruth(dataset);
	double real_sum = gt_gsum(g, gt);

	double eu_err, hu_err, su_err, lu_err, univmon_err, sota_err;
	gsum1_subset(total_mem, elastic_mem, univmon_mem, count_ratio, d, dataset, g, gt, eu_err, hu_err, su_err, lu_err, univmon_err, sota_err);
	cout << "E-LETFramework: " << eu_err << endl;
	cout << "H-LETFramework:" << hu_err << endl;
	cout << "S-LETFramework:" << su_err << endl;
	cout << "L-LETFramework:" << lu_err << endl;
	cout << "Univmon:" << univmon_err << endl;
	cout << "SOTA:" << sota_err << endl;
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
	if (s.find("cardinality") != -1)
	{
		g = &cardinality;
		echo = "cardinality";
	}
	else if (s.find("sum") != -1)
	{
		g = &sum;
		echo = "sum";
	}
	else if (s.find("sqr") != -1)
	{
		g = &sqr;
		echo = "self-join";
	}
	else if (s.find("entropy") != -1)
	{
		g = &entropy;
		echo = "entropy";
	}
	output_file = to_string(dataset_type) + "_frequencyEstimation_" + echo + ".csv";
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
	//cout << "total_mem = " << total_mem << endl;
	cout << "mem_start = " << mem_start << endl;
	cout << "mem_stop = " << mem_stop << endl;
	cout << "mem_step = " << mem_step << endl;
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
		gsum(total_mem, topk_ratio, count_ratio, d, g, out);
	}
	out.close();
	return 0;
}