#include "Sketch.h"
#include "FAGMS.h"
#include "JoinSketch_ver3.h"
#include "AGMS.h"
#include "SkimSketch.h"
int outerseed;
Sketch* Choose_Sketch(uint32_t w, uint32_t d, uint32_t hash_seed = 1000,int id=0){
	return new Classifier(w,d,hash_seed);
}
