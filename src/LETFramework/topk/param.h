#ifndef _PARAM_H_
#define _PARAM_H_

#include "../../common/Util.h"
#include <string.h>
#include <stdint.h>
#include <random>
#include <string>
#include <memory>
#include <iostream>
#include <cmath>
#include <math.h>

#define COUNTER_PER_BUCKET 7
#define MAX_VALID_COUNTER 7

#define ALIGNMENT 64

#define COUNTER_PER_WORD 8
#define BIT_TO_DETERMINE_COUNTER 3
#define K_HASH_WORD 1


#define KEY_LENGTH_4 4
#define KEY_LENGTH_13 13

#define CONSTANT_NUMBER 2654435761u
#define CalculateBucketPos(fp) (((fp) * CONSTANT_NUMBER) >> 15)
#define GetCounterVal(val) ((uint32_t)((val) & 0x7FFFFFFF))
#define UPDATE_GUARD_VAL(guard_val) ((guard_val) + 1)

#define SWAP_MIN_VAL_THRESHOLD 5

#define HIGHEST_BIT_IS_1(val) ((val) & 0x80000000)

struct Bucket
{
	key_type key[COUNTER_PER_BUCKET];
	uint32_t val[COUNTER_PER_BUCKET];
};


#endif
