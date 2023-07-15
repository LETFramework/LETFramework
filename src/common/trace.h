#ifndef _TRACE_H_
#define _TRACE_H_

#include "Util.h"
#include <iostream>
#include <map>
#include<fstream>
using namespace std;

vector<key_type> loadCAIDA18(int read_num = -1, 
const char* filename =
"../src/130000.dat"
){
	printf("Open %s \n", filename);
	FILE* pf = fopen(filename, "rb");
	if(!pf){
		printf("%s not found!\n", filename);
		exit(-1);
	}

	vector<key_type> vec;
	double ftime=-1;
	char trace[30];
	int i = 0;
	while(fread(trace, 1, 21, pf)){
		TUPLES u;
		memcpy(u.data, trace, 13);
		double ttime = *(double*) (trace+13);
		if(ftime<0)ftime=ttime;
		vec.push_back(u.srcIP());
		if(++i == read_num) break;
	}
	printf("load %d packets\n", i);
	fclose(pf);
	return vec;
}

vector<pair<int, key_type>> groundtruth(vector<key_type> &input, int read_num = -1){
	map<uint32_t, int> cnt;
	int i = 0;
	for(auto tkey: input){
		++cnt[tkey];
		if(++i == read_num) break;
	}
	vector<pair<int, key_type>> ans;
	for(auto flow: cnt)
		ans.push_back({flow.second, flow.first});
	sort(ans.begin(), ans.end(), greater<pair<int, key_type>>());
	printf("there are %d flows\n", (int)ans.size());
	return ans;
}


vector<key_type> readFile_zipf(const char* filename="../src/zipf_1.0.dat", int length = 4, int MAX_ITEM = INT32_MAX) {
	ifstream inFile(filename, ios::binary);
	vector<key_type> items;
	if (!inFile.is_open())
		cout << "File fail." << endl;

	int max_freq = 0;
	char* key=new char[length];
	for (int i = 0; i < MAX_ITEM; ++i)
	{
		inFile.read(key, length);
		if (inFile.gcount() < length)
			break;
		items.push_back(*(key_type*)string(key, key_len).c_str());
	}
	inFile.close();

	printf("Open %s \n", filename);
	printf("load %d packets\n", items.size());
	delete key;
	return items;
}
vector<key_type> loadWebdocs(int read_num = -1,
	const char* filename =
	"../src/webdocs_timestamp00.dat"
) {
	printf("Open %s \n", filename);
	FILE* pf = fopen(filename, "rb");
	if (!pf) {
		printf("%s not found!\n", filename);
		exit(-1);
	}

	vector<key_type> vec;
	double ftime = -1;
	char trace[30];
	int i = 0;
	while (fread(trace, 1, 16, pf)) {
		key_type key = *(key_type*)trace;
		double ttime = *(double*)(trace + 8);
		if (ftime < 0)ftime = ttime;
		vec.push_back(key);
		if (++i == read_num) break;
	}
	printf("load %d packets\n", i);
	fclose(pf);
	return vec;
}
#endif // _TRACE_H_

