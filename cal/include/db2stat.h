#if !defined(__DB2STAT_H__)
#define __DB2STAT_H__

#include "dbc_internal.h"

namespace ai
{
namespace app
{

using namespace std;
using namespace ai::scci;

struct thread_para_t
{
	string thread_id;
	string para_name;

	bool operator<(const thread_para_t& rhs) const {
		if (thread_id < rhs.thread_id)
			return true;
		else if (thread_id > rhs.thread_id)
			return false;

		return para_name < rhs.para_name;
	}
};

class db2stat
{
public:
	db2stat();
	~db2stat();

	void run(int argc, char **argv);

private:
	void load_adaptor(const string& stat_key, const string& thread_id);
	void load_resource(const string& stat_key, const string& thread_id);
	void load_source(const string& stat_key, const string& thread_id);
	void load_invalid(const string& stat_key, const string& thread_id);
	void load_error(const string& stat_key, const string& thread_id);
	void load_global(const string& stat_key);
	void load_input(const string& stat_key);
	void load_stat(const string& stat_key, const string& thread_id);
	void load_stat_src_record(const string& stat_key, const string& thread_id);
	void load_stat_element(const string& stat_key, const string& thread_id);
	void load_stat_element_item(const string& stat_key, const string& stat_type);
	void load_para();
	void load_para_default();
	string get_value(const string& thread_id, const string& para_name);

	ofstream ofs;
	Generic_Database *db;
	string prefix;
	map<int, string> table_map;
	map<thread_para_t, string> para_map;
	map<string, string> para_default_map;
	map<int, string> fields;
};

}
}

#endif

