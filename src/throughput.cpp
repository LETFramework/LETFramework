#include <iostream>
#include "common/config.h"
#include "./common/trace.h"
#include "LETFramework/LETFramework.h"
#include "SOTA/SOTA.h"
#ifdef WIN32
#include<Windows.h>
template<typename Arg0>
double test(Arg0& sketch, vector<key_type>& dataset)
{
    LARGE_INTEGER nFreq;

    LARGE_INTEGER t1;

    LARGE_INTEGER t2;
    QueryPerformanceFrequency(&nFreq);

    QueryPerformanceCounter(&t1);
    for (auto key : dataset) {
        sketch.insert(key);
    }
    QueryPerformanceCounter(&t2);

    return (t2.QuadPart - t1.QuadPart) / (double)nFreq.QuadPart;
}
#else
#include<time.h>
template<typename Arg0>
double test(Arg0& sketch, vector<key_type>& dataset)
{
    timespec time1, time2;
    clock_gettime(CLOCK_MONOTONIC, &time1);
    for (auto key : dataset) {
        sketch.insert(key);
    }
    clock_gettime(CLOCK_MONOTONIC, &time2);

    return 1.0 * (time2.tv_sec - time1.tv_sec) + 1.0 * (time2.tv_nsec - time1.tv_nsec) / 1000000000;
}
#endif // WIN32
#include <fstream>
#include <cstring>
#include <stdio.h>
#include <sstream>
#include <fstream>

using namespace std;
double topk_ratio, count_ratio, lambda;
uint32_t total_mem, d;
string dataset_path;
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


double gt_gsum(double (*g)(double), vector<pair<int, key_type>> &gt) {
    double sum = 0;
    for (auto p: gt) {
        sum += g(p.first);
    }
    return sum;
}
void gsum1(uint32_t total_mem, uint32_t elastic_mem, uint32_t univmon_mem, double count_ratio, uint32_t d,
    vector<key_type>& dataset, double (*g)(double),double real_sum, 
    double& avg_eu_err,double& avg_hu_err, double& avg_su_err, double& avg_lu_err,double& avg_univmon_err,double& avg_sota_err) {
    int T = 5;
    avg_eu_err = avg_hu_err = avg_su_err = avg_lu_err = avg_sota_err = avg_univmon_err = 0;
    LETFramework eu(elastic_mem, univmon_mem, 0, d, count_ratio);
    LETFramework hu(elastic_mem, univmon_mem, 2, d, count_ratio);
    LETFramework su(elastic_mem, univmon_mem, 1, d, count_ratio);
    LETFramework lu(elastic_mem, univmon_mem, 3, d, count_ratio);
    LETFramework univmon(0, total_mem);
    SOTA sota(total_mem);
    for (int i = 0; i < T; i++) {
        eu.initial();
        hu.initial();
        su.initial();
        lu.initial();
        univmon.initial();
        sota.initial();
        

        double eu_err = 1.0 * dataset.size() / test(eu, dataset) /1000000.0;
        double hu_err = 1.0 * dataset.size() / test(hu, dataset) /1000000.0;
        double su_err = 1.0 * dataset.size() / test(su, dataset) /1000000.0;
        double lu_err = 1.0 * dataset.size() / test(lu, dataset) /1000000.0;
        double univmon_err = 1.0 * dataset.size() / test(univmon, dataset) /1000000.0;
        double sota_err = 1.0 * dataset.size() / test(sota, dataset) /1000000.0;

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

void  gsum(uint32_t total_mem, double topk_ratio, double count_ratio,uint32_t d,double (*g)(double)) {
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
    gsum1(total_mem, elastic_mem, univmon_mem,count_ratio, d, dataset, g, real_sum, eu_err, hu_err, su_err, lu_err, univmon_err, sota_err);
    cout << "E-LETFramework: " << eu_err<<"MIPS" << endl;
    cout << "H-LETFramework:" << hu_err << "MIPS" << endl;
    cout << "S-LETFramework:" << su_err << "MIPS" << endl;
    cout << "L-LETFramework:" << lu_err << "MIPS" << endl;
    cout << "Univmon:" << univmon_err << "MIPS" << endl;
    cout << "SOTA:" << sota_err << "MIPS" << endl;
}
void config(char* argv[])
{
    map<string, string> m_mapConfigInfo;
    ConfigFileInit(m_mapConfigInfo, argv[0]);
    dataset_path = argv[1];
    dataset_type = DATA_TYPE(stoi(string(argv[2])));
    string s = argv[3],echo;
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
    lambda = stod(getValueString(m_mapConfigInfo, "lambda"));
    total_mem = getValueInt(m_mapConfigInfo, "total_mem");
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
    cout << "total_mem = " << total_mem << endl;
    cout << "topk_ratio = " << topk_ratio << endl;
    cout << "countSketchRatio = " << count_ratio << endl;
    cout << "d = " << d << endl;
    cout << "------------------------------------" << endl;
}
int main(int argc, char* argv[]) {
    config(argv+1);
    gsum(total_mem, topk_ratio, count_ratio, d, g);
    return 0;
}