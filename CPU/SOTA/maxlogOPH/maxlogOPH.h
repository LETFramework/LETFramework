#ifndef _MAXLOGOPT_H
#define _MAXLOGOPT_H
#include"../../common/Util.h"
#include "../../common/BOBHash32.h"
struct Tuple
{
	uint32_t val;
	key_type key;
	bool unique;
};
class MaxLogOPH
{
public:
	const uint32_t totalShingles = 0xFFFFFFFF;
	Tuple* max_hash;
	uint32_t k = 0;
	BOBHash32 hsh;
	uint32_t rnd = 0;
	MaxLogOPH(uint32_t _mem_in_byte)
	{
		k = _mem_in_byte / sizeof(Tuple);
		max_hash = new Tuple[k];
	}
	~MaxLogOPH()
	{
		delete[] max_hash;
	}
	void initial()
	{
		for (size_t i = 0; i < k; i++)
		{
			max_hash[i].key = max_hash[i].val = max_hash[i].unique = 0;
		}
		rnd = BOBHash32::get_random_prime_index();
		hsh.initialize(rnd);
	}
	void initial(MaxLogOPH* _refer)
	{
		for (size_t i = 0; i < k; i++)
		{
			max_hash[i].key = max_hash[i].val = max_hash[i].unique = 0;
		}
		rnd = _refer->rnd;
		hsh.initialize(rnd);
	}
	void insert(key_type key)
	{
		uint32_t temp = hsh.run(key) % totalShingles;
		size_t bucket_pos = temp % k;
		double log_temp = -log2(1.0 * temp / totalShingles);
		double hash_val = floor(log_temp);
		if (hash_val > max_hash[bucket_pos].val || max_hash[bucket_pos].key == 0)
		{
			max_hash[bucket_pos].val = hash_val;
			max_hash[bucket_pos].unique = 1;
			max_hash[bucket_pos].key = key;
		}
		else if (hash_val == max_hash[bucket_pos].val && max_hash[bucket_pos].key != key)
		{
			max_hash[bucket_pos].unique = 0;
		}
	}
	double jaccard(MaxLogOPH* B)
	{
		uint32_t con = 0;
		for (size_t i = 0; i < k; i++)
		{
			if (max_hash[i].val > B->max_hash[i].val && (max_hash[i].unique == 1))
			{
				con++;
			}
			else if (max_hash[i].val < B->max_hash[i].val && (B->max_hash[i].unique == 1))
			{
				con++;
			}
		}
		double num = (double)k;
		return 1.0 - con / (num * 0.7213);
	}
};
#endif // !_MAXLOGOPT_H
