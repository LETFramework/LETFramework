#ifndef _HeavyGuardingBASED_H
#define _HeavyGuardingBASED_H

class HeavyGuardianBased :public TopkPart
{
#define HK_b 1.08
#define JUDGE_IF_SWAP(min_val, guard_val) ((guard_val) > (min_val))
public:
    uint32_t* nCounters;
    HeavyGuardianBased(uint32_t _mem_in_bytes) :TopkPart(_mem_in_bytes) {
        nCounters = new uint32_t[bucket_num];
    };
    ~HeavyGuardianBased()
    {
        delete nCounters;
    }
    void initial()
    {
        TopkPart::initial();
        memset(nCounters, 0, sizeof(nCounters));
    }
   void initial(const TopkPart* _refer)
    {
       TopkPart::initial(_refer);
       memset(nCounters, 0, sizeof(nCounters));
    }
    uint32_t query(key_type key) {
        int pos = hsh.run(key) % bucket_num;
        for (int i = 0; i < MAX_VALID_COUNTER; ++i)
            if (buckets[pos].key[i] == key)
                return buckets[pos].val[i];
        return 0;
    }
    int insert(key_type key, key_type& swap_key, uint32_t& swap_val, uint32_t f = 1) {
        int pos = hsh.run(key) % bucket_num;
        uint32_t min_counter_val;
        int min_counter;

        do{
            int matched = -1, empty = -1;
            min_counter = 0;
            min_counter_val = GetCounterVal(buckets[pos].val[0]);
            for(int i = 0; i < COUNTER_PER_BUCKET; i++){
                if(buckets[pos].key[i] == key){
                    matched = i;
                    break;
                }
                if(buckets[pos].key[i] == 0 && empty == -1)
                    empty = i;
                if(min_counter_val > GetCounterVal(buckets[pos].val[i])){
                    min_counter = i;
                    min_counter_val = GetCounterVal(buckets[pos].val[i]);
                }
            }

            if(matched != -1){
                buckets[pos].val[matched] += f;
                return 0;
            }

            if(empty != -1){
                buckets[pos].key[empty] = key;
                buckets[pos].val[empty] = f;
                return 0;
            }
        }while(0);

        uint32_t guard_val = nCounters[pos];
        if (!(rand() % int(pow(HK_b, min_counter_val - nCounters[pos]))))
        {
            guard_val = UPDATE_GUARD_VAL(guard_val);
        }
        

        if(!JUDGE_IF_SWAP(GetCounterVal(min_counter_val), guard_val)){
            nCounters[pos] = guard_val;
            return 2;
        }

        swap_key = buckets[pos].key[min_counter];
        swap_val = buckets[pos].val[min_counter];

        nCounters[pos] = 0;
        buckets[pos].key[min_counter] = key;
        buckets[pos].val[min_counter] = f;

        return 1;
    }
    HashMap query_all() const {
        HashMap mp;
        for (int i = 0; i < bucket_num; i++) {
            for (int j = 0; j < MAX_VALID_COUNTER; j++) {
                if (buckets[i].key[j] != 0) {
                    mp[buckets[i].key[j]] = buckets[i].val[j];
                }
            }
        }
        return mp;
    }
};

#endif
