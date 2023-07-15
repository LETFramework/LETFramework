#ifndef UNIVMON_H_INCLUDED
#define UNIVMON_H_INCLUDED

#include "CountHeap.h"
#include <ctime>
#include <cstdlib>
#include <vector>
#include <unordered_map>
class USketchPart
{
public:
    typedef CountHeap<key_len> HeavyHitterDetector;
    HeavyHitterDetector** sketches;
    BOBHash32** hash;
    int element_num = 0;
    uint8_t level,d;
    double gama;
    uint64_t mem_in_bytes;
    USketchPart(uint64_t _mem_in_bytes,uint32_t _d, double _gama,uint8_t _level=14):level(_level),mem_in_bytes(_mem_in_bytes),d(_d),gama(_gama){
        sketches = new HeavyHitterDetector * [level];
        hash = new BOBHash32 * [level];
    }
    void initial(const USketchPart* _refer)
    {
        if (mem_in_bytes == 0)
            return;
        int mem_for_sk = int(mem_in_bytes);
        int mem = int(mem_for_sk / level);
        for (int i = 0; i < level; i++)
        {
            sketches[i] = new HeavyHitterDetector(*(_refer->sketches[i]));
            hash[i] = new BOBHash32(*_refer->hash[i]);
        }
    }
    void initial()
    {
        if (mem_in_bytes == 0)
            return;
        int mem_for_sk = int(mem_in_bytes);
        int mem = int(mem_for_sk / level);
        vector<uint32_t> rd_list = BOBHash32::get_random_prime_index_list(level);
        for (int i = 0; i < level; i++)
        {
            sketches[i] = new HeavyHitterDetector((1 - gama) * mem, gama * mem, d);
            hash[i] = new BOBHash32(rd_list[i]);
        }
    }
    ~USketchPart(){
        delete hash;
        delete sketches;
    }
    void clear() {
        if (mem_in_bytes == 0)
            return;
        for (int i = 0; i < level; i++)
        {
            delete sketches[i];
            delete hash[i];
        }
    }
    void insert(key_type key, int f=1) {
        insert((uint8_t*)&key, f);
    }
    void insert(uint8_t* key, int f=1){
        element_num += f;
        int hash_val;
        sketches[0]->insert(key, f);
        for(int i=1; i<level; i++)
        {
            hash_val = hash[i]->run((const char*)key, key_len) % 2;
            if(hash_val)
            {
                sketches[i]->insert(key, f);
            }
            else
            {
                break;
            }
        }
    }
    double gsum(double (*g)(double))
    {
        int hash_val, coe;
        vector<pair<string, int>> result;
        vector<double> Y(level);
        for(int i = level -1; i>=0; i--)
        {
            sketches[i]->get_top_k_with_frequency(result);
            Y[i] = (i == level - 1) ? 0 : 2*Y[i+1];
            for(auto kv: result)
            {
                if(kv.second == 0)
                {
                    continue;
                }
                hash_val = (i == level - 1)? 1 : hash[i+1]->run(kv.first.c_str(), key_len) % 2;
                coe = (i == level - 1) ? 1 : (1 - 2*hash_val);
                Y[i] += coe*g(double(kv.second));
            }
        }
        return Y[0];
    }
    double gsum_subset(double (*g)(double), const SubsetCheck* check)
    {
        int hash_val, coe;
        vector<pair<string, int>> result;
        vector<double> Y(level);
        for (int i = level - 1; i >= 0; i--)
        {
            sketches[i]->get_top_k_with_frequency(result);
            Y[i] = (i == level - 1) ? 0 : 2 * Y[i + 1];
            for (auto kv : result)
            {
                if (kv.second == 0)
                {
                    continue;
                }
                key_type key = str_to_key(kv.first.c_str());
                if (!(*check)(key))
                    continue;
                hash_val = (i == level - 1) ? 1 : hash[i + 1]->run(kv.first.c_str(), key_len) % 2;
                coe = (i == level - 1) ? 1 : (1 - 2 * hash_val);
                Y[i] += coe * g(double(kv.second));
            }
        }
        return Y[0];
    }
    double get_cardinality()
    {
        return gsum([](double x){return 1.0;});
    }

    double get_entropy()
    {
        double sum = gsum([](double x){return x==0? 0 : x* std::log2(x);});
        return std::log2(element_num) - sum / element_num;
    }

    void get_heavy_hitters(uint32_t threshold, std::vector<pair<uint32_t, uint32_t> >& ret)
    {
        unordered_map<std::string, uint32_t> results;
        vector<std::pair<std::string, int>> vec_top_k;
        for (int i = level - 1; i >= 0; --i) {
            sketches[i]->get_top_k_with_frequency(vec_top_k);
            for (auto kv: vec_top_k) {
                results[kv.first] = max(results[kv.first], (uint32_t)kv.second);
            }
        }

        ret.clear();
        for (auto & kv: results) {
            if (kv.second >= threshold) {
                ret.emplace_back(make_pair(*(uint32_t *)(kv.first.c_str()), kv.second));
            }
        }
    }
    static double gsum_add(const USketchPart* x, const USketchPart* y, double (*g)(double),int level)
    {
        double hash_val, coe;
        vector<pair<string, int>> result0, result1;
        vector<double> Y(level);
        for (int i = level - 1; i >= 0; i--)
        {
            unordered_map<string, int>  mp;
            x->sketches[i]->get_top_k_with_frequency(result0);
            y->sketches[i]->get_top_k_with_frequency( result1);
            for (auto p : result0) { // [key,val]
                mp[p.first] = p.second;
            }
            for (auto p : result1) { // [key,val]
                mp[p.first] += p.second;
            }
            Y[i] = (i == level - 1) ? 0 : 2 * Y[i + 1];
            HeavyHitterDetector* combine_sketch = new HeavyHitterDetector(*(x->sketches[i]));
            HeavyHitterDetector::join(*(x->sketches[i]), *(y->sketches[i]), *combine_sketch);
            for (auto p : mp) // [key,val]
            {
                double estimate = combine_sketch->query((uint8_t*)p.first.c_str());

                hash_val = (i == level - 1) ? 1 : x->hash[i + 1]->run(p.first.c_str(), key_len) % 2;
                coe = (i == level - 1) ? 1 : (1 - 2 * hash_val);
                Y[i] += coe * g(double(estimate));
            }
            delete combine_sketch;
        }
        return Y[0];
    }

    static double gsum_mul(const USketchPart* x, const USketchPart* y, double (*g)(double), int level)
    {
        double hash_val, coe;
        vector<pair<string, int>> result0, result1;
        vector<double> Y(level);
        for (int i = level - 1; i >= 0; i--)
        {
            unordered_map<string, int> mp0, mp;
            x->sketches[i]->get_top_k_with_frequency( result0);
            y->sketches[i]->get_top_k_with_frequency( result1);
            for (auto p : result0) { //[key,val]
                mp0[p.first] = p.second;
            }
            for (auto p : result1) {
                if (mp0.count(p.first) > 0)
                    mp[p.first] = mp0[p.first] * p.second;
            }
            Y[i] = (i == level - 1) ? 0 : 2 * Y[i + 1];
            for (auto p : mp)//[key,val]
            {
                if (p.second <= 0)
                {
                    continue;
                }
                hash_val = (i == level - 1) ? 1 : x->hash[i + 1]->run(p.first.c_str(), key_len) % 2;
                coe = (i == level - 1) ? 1 : (1 - 2 * hash_val);
                Y[i] += coe * g(double(p.second));
            }
        }
        return Y[0];
    }
    double query(key_type key) const
    {
        return max(0.0,sketches[0]->query((uint8_t*)&key));
    }
};

#endif // UNIVMON_H_INCLUDED
