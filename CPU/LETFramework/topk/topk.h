#ifndef _HeavySketch_H
#define _HeavySketch_H
#include "../../common/BOBHash32.h"
#include "param.h"
class TopkPart {
public:
	uint32_t bucket_num;
	Bucket* buckets;
	BOBHash32 hsh;
	uint32_t rnd;
	TopkPart(uint32_t _mem_in_bytes) :bucket_num(_mem_in_bytes / sizeof(Bucket)) {
		buckets = new Bucket[bucket_num];
	}
	TopkPart(const TopkPart* _refer) :bucket_num(_refer->bucket_num) {
		buckets = new Bucket[bucket_num];
	}
	virtual void initial()
	{
		memset(buckets, 0, sizeof(Bucket) * bucket_num);
		rnd = BOBHash32::get_random_prime_index();
		hsh.initialize(rnd);
	}
	virtual void initial(const TopkPart* _refer)
	{
		memset(buckets, 0, sizeof(Bucket) * bucket_num);
		rnd = _refer->rnd;
		hsh.initialize(rnd);
	}
	virtual void clear()
	{
	}
	~TopkPart()
	{
		delete buckets;
	}
	virtual int insert(key_type key, key_type& swap_key, uint32_t& swap_val, uint32_t f = 1) = 0;
	virtual HashMap query_all() const = 0;
	virtual uint32_t query(key_type key) = 0;
};
#endif