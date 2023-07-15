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
void heavyhitterCheck(const vector<pair<string, uint32_t>>& predict,const vector<pair<double, key_type>>& truth,double& err)
{
    uint32_t num = 0;
    HashMap mp;
    for (auto kv : predict)
        mp[*(key_type*)kv.first.c_str()] = kv.second;
    for(auto kv:truth)
        if (mp.find(kv.second) != mp.end())
        {
            num++;
        }
    double p = 1.0 * num / predict.size(), r = 1.0 * num / truth.size();
    err = 2 * p * r / (p + r);
}
void gsum1_subset(uint32_t total_mem, uint32_t elastic_mem, uint32_t univmon_mem, double count_ratio, uint32_t d, 
    vector<key_type>& dataset, double (*g)(double), const vector<pair<double, key_type>>& gt_subset, int threshold,
    double& avg_eu_err, double& avg_hu_err, double& avg_su_err, double& avg_lu_err,double& avg_univmon_err,double& avg_sota_err) {
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

        for (auto key : dataset) {
            eu.insert(key);
            hu.insert(key);
            su.insert(key);
            lu.insert(key);
            univmon.insert(key);
            sota.insert(key);
        }
        double hu_err, su_err, lu_err, eu_err, sota_err, univmon_err;

        vector<pair<string, uint32_t>> hu_hh, su_hh, lu_hh, eu_hh, univmon_hh, sota_hh;
        eu.get_heavy_hitters(threshold,eu_hh);
        hu.get_heavy_hitters(threshold,hu_hh);
        su.get_heavy_hitters(threshold,su_hh);
        lu.get_heavy_hitters(threshold,lu_hh);
        univmon.get_heavy_hitters(threshold,univmon_hh);
        sota.get_heavy_hitters(threshold,sota_hh);

        heavyhitterCheck(eu_hh, gt_subset,eu_err);
        heavyhitterCheck(hu_hh, gt_subset, hu_err);
        heavyhitterCheck(su_hh, gt_subset, su_err);
        heavyhitterCheck(lu_hh, gt_subset, lu_err);
        heavyhitterCheck(univmon_hh, gt_subset, univmon_err);
        heavyhitterCheck(sota_hh, gt_subset, sota_err);


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

vector<pair<double, key_type>> get_gt_subset(const vector<pair<int, key_type>>& gt, double (*g)(double),int& threshold,double lambda) {
    unordered_map<key_type, double> mp;
    double sum = 0;
    for (auto p : gt) { // [val,key]
        sum += g(p.first);
        mp[p.second] += g(p.first);
    }
    threshold = sum * lambda;
    vector<pair<double, key_type>> gt_subset;
    for (auto p : mp) { //[key,val]
        if (p.second > threshold)
        {
            gt_subset.push_back(make_pair(p.second, p.first));
        }
    }
    sort(gt_subset.begin(), gt_subset.end(), greater<pair<double, key_type>>());
    return gt_subset;
}

void gsum_subset(uint32_t total_mem, double topk_ratio, double count_ratio, uint32_t d, double (*g)(double),double lambda) {
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
    int threshold;
    vector<pair<int, key_type>> gt = groundtruth(dataset);
    vector<pair<double, key_type>> gt_subset = get_gt_subset(gt, g, threshold, lambda);
    cout << "total mem = " << total_mem << endl;
    cout << "elastic mem = " << elastic_mem << endl;
    cout << "univmon mem = " << univmon_mem << endl;


    double eu_err, hu_err, su_err, lu_err, univmon_err, sota_err;
    gsum1_subset(total_mem, elastic_mem, univmon_mem, count_ratio, d, dataset, g, gt_subset, threshold, eu_err, hu_err, su_err, lu_err, univmon_err, sota_err);
    cout << "E-LETFramework: " << eu_err << endl;
    cout << "H-LETFramework:" << hu_err << endl;
    cout << "S-LETFramework:" << su_err << endl;
    cout << "L-LETFramework:" << lu_err << endl;
    cout << "Univmon:" << univmon_err << endl;
    cout << "SOTA:" << sota_err << endl;
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
    gsum_subset(total_mem, topk_ratio, count_ratio, d, g, lambda);
    return 0;
}