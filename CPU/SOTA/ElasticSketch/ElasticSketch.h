#ifndef _ELASTIC_SKETCH_H_
#define _ELASTIC_SKETCH_H_

#include "./HeavyPart.h"
#include "./LightPart.h"


class ElasticSketch
{
    int heavy_mem;
    int light_mem;
    int bucket_num;
    typedef uint32_t key_type;
    HeavyPart* heavy_part;
    LightPart* light_part;

public:
    ElasticSketch(int _bucket_num, int tot_memory_in_bytes) :bucket_num(_bucket_num) {
        int heavy_mem = bucket_num * COUNTER_PER_BUCKET * 8;
        int light_mem = tot_memory_in_bytes - heavy_mem;
        heavy_part = new HeavyPart(bucket_num);
        light_part = new LightPart(light_mem);
    }
    ~ElasticSketch() {
        delete heavy_part;
        delete light_part;
    }
    void clear() {
        heavy_part->clear();
        light_part->clear();
    }

    void insert(uint8_t* key, int f = 1)
    {
        uint8_t swap_key[KEY_LENGTH_4];
        uint32_t swap_val = 0;
        int result = heavy_part->insert(key, swap_key, swap_val, f);
        switch (result)
        {
        case 0: return;
        case 1: {
            if (HIGHEST_BIT_IS_1(swap_val))
                light_part->insert(swap_key, GetCounterVal(swap_val));
            else
                light_part->swap_insert(swap_key, swap_val);
            return;
        }
        case 2: light_part->insert(key, 1);  return;
        default:
            printf("error return value !\n");
            exit(1);
        }
    }

    int query(uint8_t* key)
    {
        uint32_t heavy_result = heavy_part->query(key);
        if (heavy_result == 0 || HIGHEST_BIT_IS_1(heavy_result))
        {
            int light_result = light_part->query(key);
            return (int)GetCounterVal(heavy_result) + light_result;
        }
        return heavy_result;
    }

    int get_bucket_num() { return heavy_part->get_bucket_num(); }

    void get_heavy_hitters(int threshold, vector<pair<string, uint32_t>>& results)
    {
        for (int i = 0; i < bucket_num; ++i)
            for (int j = 0; j < MAX_VALID_COUNTER; ++j)
            {
                uint32_t key = heavy_part->buckets[i].key[j];
                int val = query((uint8_t*)&key);
                if (val >= threshold) {
                    results.push_back(make_pair(string((const char*)&key, 4), val));
                }
            }
    }

    void insert(key_type key, int f = 1) {
        insert((uint8_t*)&key, f);
    }
    int query(key_type key) {
        return query((uint8_t*)&key);
    }
    double gsum(double (*g)(double))
    {
        int tot = 0;
        double entr = 0;

        light_part->get_gsum(tot, entr, g);

        for (int i = 0; i < bucket_num; ++i)
            for (int j = 0; j < MAX_VALID_COUNTER; ++j)
            {
                uint8_t key[KEY_LENGTH_4];
                *(uint32_t*)key = heavy_part->buckets[i].key[j];
                int val = heavy_part->buckets[i].val[j];

                int ex_val = light_part->query(key);

                if (HIGHEST_BIT_IS_1(val) && ex_val)
                {
                    val += ex_val;

                    tot -= ex_val;

                    entr -= g(ex_val);
                }
                val = GetCounterVal(val);
                if (val)
                {
                    tot += val;
                    entr += g(val);
                }
            }
        return entr;
    }
};

#endif // _ELASTIC_SKETCH_H_
