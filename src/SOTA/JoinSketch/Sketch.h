class Sketch {
public:
	virtual void Insert(const char* str)=0;
	void Insert(key_type key) { Insert((const char*)&key); }
	virtual int Query(const char* str)const=0;
	virtual long double Join(Sketch* other)=0;
	virtual long double Join(Sketch* other,double(*g)(double))=0;
	virtual long double gsum(double(*g)(double))=0;
	virtual bool CheckHeavy(const char* str){
		return 0;
	}
};