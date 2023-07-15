#ifndef _SpaceSavingBASED_H
#define _SpaceSavingBASED_H
struct BucketNode;

struct BucketValNode
{
    BucketValNode* prev = NULL;
    BucketValNode* next = NULL;
    BucketNode* first = NULL;
    uint32_t val = 0;
};

struct BucketNode
{
    BucketNode* prev = NULL;
    BucketNode* next = NULL;
    BucketValNode* parent = NULL;
    Bucket* bucket;
};


class SpaceSavingBased :public TopkPart
{
    unordered_map<key_type, BucketNode*> hsh;
    BucketNode* val_nodes; 
    BucketValNode* tail;
    uint32_t count;
    void add_bucket(BucketNode* bucket)
    {
        if (bucket->parent->next==NULL || bucket->parent->next->val != bucket->parent->val + 1)
        {
            BucketValNode* ptr = new BucketValNode;
            ptr->val = bucket->parent->val + 1;
            ptr->next = bucket->parent->next;
            if (ptr->next!=NULL)
                ptr->next->prev = ptr;
            ptr->prev = bucket->parent;
            bucket->parent->next = ptr;
        }
        auto initValNode = bucket->parent, newValNode = bucket->parent->next;
        if (bucket->next!=NULL)
            bucket->next->prev = bucket->prev;
        if (bucket->prev!=NULL)
            bucket->prev->next = bucket->next;
        if (bucket->prev == NULL)
        {
            initValNode->first = bucket->next;
        }
        bucket->next = newValNode->first;
        if (newValNode->first!=NULL)
            newValNode->first->prev = bucket;
        bucket->prev = NULL;
        newValNode->first = bucket;        
        bucket->parent = newValNode;
        if (initValNode->first == NULL)
        {
            if (initValNode->next!=NULL)
                initValNode->next->prev = initValNode->prev;
            if (initValNode->prev!=NULL)
                initValNode->prev->next = initValNode->next;
            if (initValNode == tail)
            {
                tail = initValNode->next;
            }
            delete initValNode;
        }

    }
    int append_new_key(key_type key, key_type& swap_key, uint32_t& swap_val, uint32_t f = 1)
    {
        BucketNode* bucket = tail->first;
        uint32_t min_val = bucket->bucket->val[0], pos = 0;
        for (size_t i = 0; i < COUNTER_PER_BUCKET; i++)
        {
            if (GetCounterVal(bucket->bucket->val[i]) < min_val)
            {
                min_val = GetCounterVal(bucket->bucket->val[i]);
                pos = i;
            }
        }
        swap_key = bucket->bucket->key[pos];
        swap_val = bucket->bucket->val[pos];
        if (swap_key)
        {
            hsh.erase(swap_key);
            bucket->bucket->val[pos] = f;
        }
        else
        {
            bucket->bucket->val[pos] = f;
        }
        bucket->bucket->key[pos] = key;
        hsh[key] = bucket;
        add_bucket(tail->first);
        return (swap_val > 0);
    }
public:
    SpaceSavingBased(uint32_t _mem_in_bytes):TopkPart(_mem_in_bytes){};
    void initial()
    {
        count = 0;
        TopkPart::initial();
        tail = new BucketValNode;
        BucketNode* ptr = new BucketNode;
        ptr->bucket = buckets + 0;
        ptr->parent = tail;
        tail->first = ptr;
        for (size_t i = 1; i < bucket_num; i++)
        {
            BucketNode* ptr1 = new BucketNode;
            ptr->next = ptr1;
            ptr1->prev = ptr;
            ptr1->parent = tail;
            ptr1->bucket = buckets + i;
            ptr = ptr1;
        }
        int t = 1;
    }
    void initial(const TopkPart* _refer)
    {
        TopkPart::initial(_refer);
        initial();
    }
    void clear()
    {
        BucketValNode* ptr;
        BucketNode* ptr1;
        for (ptr = tail; ptr != NULL; ptr = tail)
        {
            for (auto bucket = ptr->first; bucket != NULL; bucket = ptr1)
            {
                ptr1 = bucket->next;
                delete bucket;
            }
            tail = ptr->next;
            delete ptr;
        }
        tail = NULL;
        hsh.clear();
    }
    uint32_t query(key_type key) {
        if (hsh.find(key) != hsh.end())
        {
            auto bucket = hsh[key];
            for (size_t i = 0; i < COUNTER_PER_BUCKET; i++)
            {
                if (bucket->bucket->key[i]==key)
                {
                    return bucket->bucket->val[i];

                }
            }
        }
        return 0;
    }

    int insert(key_type key, key_type& swap_key, uint32_t& swap_val, uint32_t f = 1) {
        uint32_t min_counter_val, min_bucket_val;
        int min_counter, min_bucket;
        
        if (hsh.find(key) != hsh.end())
        {
            auto bucket = hsh[key];
            for (size_t i = 0; i < COUNTER_PER_BUCKET; i++)
            {
                if (bucket->bucket->key[i] == key)
                {
                    bucket->bucket->val[i]++;
                    break;
                }
            }
            add_bucket(bucket);
            return 0;
        }
        else
        {
            return append_new_key(key, swap_key, swap_val);
        }

    }

    HashMap query_all() const {
        HashMap mp;
        for (auto SCounter = tail; SCounter != NULL; SCounter = SCounter->next)
        {
            for (auto bucket = SCounter->first; bucket != NULL; bucket = bucket->next)
            {
                for (size_t index = 0; index < COUNTER_PER_BUCKET; index++)
                {
                    
                    mp[bucket->bucket->key[index]] = bucket->bucket->val[index];
                }
            }
        }
        return mp;
    }
};

#endif //STREAMCLASSIFIER_SPACESAVING_H
