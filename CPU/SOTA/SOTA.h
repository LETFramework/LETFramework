#ifndef _SOTA_H
#define _SOTA_H
#include"ElasticSketch/ElasticSketch.h"
#include"maxlogOPH/maxlogOPH.h"
#include"JoinSketch/Choose_Ske.h"
#include"Salsa/SalsaCM.h"
#include"WavingSketch/WavingSketch.h"
#include<thread>
class SOTA
{
	uint32_t mem_in_byte;
	ElasticSketch* elastic;
	MaxLogOPH* maxlogoph;
	Sketch* js;
	SalsaCM* salsa;
	WavingSketch<8, 16>* waving;
	double es_ratio, js_ratio, maxlog_ratio, salsa_ratio, waving_ratio;
	uint32_t synchronized;
public:
	SOTA(uint32_t _mem_in_byte, double _es_ratio = 1.0 / 3, double _js_ratio = 1.0 / 3, double _maxlog_ratio = 1.0 / 3,
		double _salsa_ratio = 0, double _waving_ratio = 0, uint32_t _synchronized = 0) :
		mem_in_byte(_mem_in_byte), es_ratio(_es_ratio), js_ratio(_js_ratio),
		maxlog_ratio(_maxlog_ratio), salsa_ratio(_salsa_ratio), waving_ratio(_waving_ratio), synchronized(_synchronized) {
		if (es_ratio > 1e-6)
		{
			elastic = new ElasticSketch(int(0.25 * mem_in_byte * es_ratio / 64), mem_in_byte * es_ratio);
			salsa = NULL;
			waving = NULL;
		}
		else
		{
			elastic = NULL;
			salsa = new SalsaCM(mem_in_byte * salsa_ratio / 3, 3);
			waving = new WavingSketch<8, 16>(mem_in_byte * waving_ratio / (8 * 8 + 16 * 2));
		}
		maxlogoph = new MaxLogOPH(mem_in_byte * maxlog_ratio);
		js = Choose_Sketch(mem_in_byte * js_ratio, 3, rand());
	}
	SOTA(const SOTA& _refer) :SOTA(_refer.mem_in_byte, _refer.es_ratio, _refer.js_ratio,
		_refer.maxlog_ratio, _refer.salsa_ratio, _refer.waving_ratio) {}
	void initial()
	{
		maxlogoph->initial();
	}
	void initial(const SOTA& _refer)
	{
		maxlogoph->initial(_refer.maxlogoph);
	}
	void insert(key_type key)
	{
		if (elastic != NULL)
		{
			elastic->insert(key);
		}
		else
		{
			salsa->Insert((const char*)&key);
			waving->Insert(key);
		}
		js->Insert(key);
		maxlogoph->insert(key);
	}
	void batch_insert(vector<key_type>& dataset)
	{
		if (synchronized == 0)
		{
			for (auto key : dataset)
			{
				insert(key);
			}
		}
		else
		{
			vector<thread> ths;
			ths.push_back(thread(&SOTA::elastic_insert, this, dataset));
			ths.push_back(thread(&SOTA::maxlogoph_insert, this, dataset));
			ths.push_back(thread(&SOTA::js_insert, this, dataset));
			for (auto& th : ths)
			{
				th.join();
			}
		}
	}
	void elastic_insert(vector<key_type> dataset)
	{
		for (auto key : dataset)
		{
			elastic->insert(key);
		}
	}
	void js_insert(vector<key_type> dataset)
	{
		for (auto key : dataset)
		{
			js->Insert(key);
		}
	}
	void maxlogoph_insert(vector<key_type> dataset)
	{
		for (auto key : dataset)
		{
			maxlogoph->insert(key);
		}
	}
	void clear()
	{
		if (elastic != NULL)
		{
			elastic->clear();
		}
		else
		{
			delete salsa;
			delete waving;
			salsa = new SalsaCM(mem_in_byte * salsa_ratio / 3, 3);
			waving = new WavingSketch<8, 16>(mem_in_byte * waving_ratio / (8 * 8 + 16 * 2));
		}
		delete js;
		js = Choose_Sketch(mem_in_byte * js_ratio, 3, rand());
		delete maxlogoph;
		maxlogoph = new MaxLogOPH(mem_in_byte * maxlog_ratio);
	}
	double cos(const SOTA& B)
	{
		auto g = [](double x)->double {return x * x; };
		auto g1 = [](double x)->double {return x; };
		return js->Join(B.js, g1) / sqrt(js->gsum(g)) / sqrt(B.js->gsum(g));
	}
	double innerProduction(const SOTA& B)
	{
		auto g1 = [](double x)->double {return x; };
		return js->Join(B.js, g1);
	}
	double jaccard(const SOTA& B)
	{
		return maxlogoph->jaccard(B.maxlogoph);
	}
	double query(key_type key)
	{
		if (elastic != NULL)
		{
			return elastic->query(key);
		}
		else
		{
			return salsa->Query((const char*)&key);
		}
	}
	double gsum(double (*g)(double))
	{
		if (elastic != NULL)
		{
			return elastic->gsum(g);
		}
		else
		{
			//  cardinality salsa,else waving
			if (g(1e3) == 1)
			{
				return salsa->get_cardinality();
			}
			else
			{
				return waving->gsum(g);
			}
		}
	}
	void get_heavy_hitters(int threshold, vector<pair<string, uint32_t>>& results)
	{
		if (elastic != NULL)
		{
			elastic->get_heavy_hitters(threshold, results);
		}
		else
		{
			waving->get_heavy_hitters(threshold, results);
		}
	}
	~SOTA()
	{
		if (elastic != NULL)
		{
			delete elastic;
		}
		else
		{
			delete waving;
			delete salsa;
		}
		delete js;
		delete maxlogoph;
	}
};
#endif // !_sota_H
