#if !defined(__DB2CAL_H__)
#define __DB2CAL_H__

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

class db2cal
{
public:
	db2cal();
	~db2cal();

	void run(int argc, char **argv);

private:
	void load_service(const string& file_type, const string& thread_id);
	void load_adaptor(const string& file_type, const string& thread_id);
	void load_resource(const string& file_type, const string& thread_id);
	void load_source(const string& file_type, const string& thread_id);
	void load_invalid(const string& file_type, const string& thread_id);
	void load_error(const string& file_type, const string& thread_id);
	void load_global(const string& file_type);
	void load_input(const string& file_type);
	void load_output(const string& file_type);
	void load_distribute(const string& file_type, const string& thread_id);
	void load_distribute1(const string& file_type, const string& thread_id);
	void load_distribute3(const string& file_type, const string& thread_id);
	void load_distribute_item(const string& file_type);
	void load_shm_tables();
	void load_tables(const string& thread_id);
	void load_aliases();
	void load_alias(int table_id);
	void load_para();
	void load_para_default();
	string get_value(const string& thread_id, const string& para_name);
	string get_thread_id(const string& file_type);

	ofstream ofs;
	Generic_Database *db;
	bool server_flag;
	int stage;
	string prefix;
	map<int, string> table_map;
	map<thread_para_t, string> para_map;
	map<string, string> para_default_map;
};

}
}

#endif

