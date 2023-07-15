#pragma once
#include <list>
#include <map>
#include <string>
#include <fstream>
using namespace std;


int ConfigFileInit(map<string, string>& m_mapConfigInfo, string path)
{
	ifstream configFile;
	configFile.open(path.c_str());
	string str_line;
	if (configFile.is_open())
	{
		while (!configFile.eof())
		{
			getline(configFile, str_line);
			if (str_line.find('#') == 0) 
			{
				continue;
			}
			size_t pos = str_line.find('=');
			string str_key = str_line.substr(0, pos);
			string str_value = str_line.substr(pos + 1);
			m_mapConfigInfo.insert(pair<string, string>(str_key, str_value));
		}
	}
	else
	{
		return -1;
	}
	configFile.close();
	return 1;
}
string getValueString(map<string, string> m_mapConfigInfo, string key)
{
	map<string, string>::iterator iter_configMap;
	iter_configMap = m_mapConfigInfo.find(key);
	if (iter_configMap != m_mapConfigInfo.end()) {
		return iter_configMap->second;
	}
	return "";
}
int getValueInt(map<string, string> m_mapConfigInfo, string key)
{
	string s = getValueString(m_mapConfigInfo, key);
	char* end;
	int i = static_cast<int>(strtol(s.c_str(), &end, 10));
	return i;
}
int write_conf(string path, string key, string value)
{
	ofstream write;
	write.open(path, ios::app);
	if (!write.is_open()) {
		//return -1;
	}
	write << endl;
	write << key + "=" + value;
	write.close();
	return 1;
}
int delete_conf(string path, string key)
{
	ifstream in;
	in.open(path);
	if (!in.is_open()) return -1;
	string strFileData = "";
	string str_line = "";
	while (!in.eof())
	{
		getline(in, str_line);
		size_t pos = str_line.find('=');
		string str_key = str_line.substr(0, pos);
		if (str_key == key || str_line == "\n" || str_line == "" || str_line == " ")
		{
			strFileData += "\n";
		}
		else {
			strFileData += str_line;
			strFileData += "\n";
		}
	}
	in.close();

	ofstream out;
	out.open(path);
	out.flush();
	out << strFileData;
	out.close();
	return 1;
}
int change_conf(string path, string key, string value) {
	delete_conf(path, key);
	write_conf(path, key, value);
	return 1;
}