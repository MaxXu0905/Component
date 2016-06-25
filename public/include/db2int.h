#if !defined(__DB2INT_H__)
#define __DB2INT_H__

#include "dbc_internal.h"

namespace ai
{
namespace app
{

using namespace std;
using namespace ai::scci;

struct log_bps_table_t
{
	string column_name;
	int column_length;

	bool operator==(const log_bps_table_t& c) const
	{
		return (c.column_name == column_name);
	}
};

struct distribute_item_t
{
	std::string file_type;
	std::string distribute_rule;
};

class db2int
{
public:
	db2int();
	~db2int();

	void run(int argc, char **argv);

private:
	void get_summary_global();
	void get_code_distribute_map();
	void load_service(const string& file_type);
	void load_adaptor(const string& file_type);
	void load_resource(const string& file_type, int type);
	void load_pattern(const string& file_type, string& pattern);
	void load_thread_pattern(const string& thread_id, string& pattern);
	void load_source(const string& file_type);
	void load_target(const string& file_type);
	void load_invalid(const string& file_type);
	void load_error(const string& file_type, bool specify_table);
	void load_global(const string& file_type);
	void load_input(const string& file_type);
	string load_args(const string& file_type);
	void load_output(const string& file_type);
	void load_summary(const string& file_type);
	void load_log_global();
	void load_distribute(const string& file_type);
	void load_distribute_item(const string& file_type, int record_serial);
	bool in_distribute_map(const string& file_type, const string& rule_file_name);
	void load_process(const string& file_type);
	void load_record_flag();
	string parse_rule(const string& rule);

	ofstream ofs;
	Generic_Database *db;
	map<string, int> record_flag_map;
	map<int, string> global_map;
	map<int, string> output_map;
	vector<log_bps_table_t> log_global_vec;
	vector<distribute_item_t> distribute_items;
	bool server_flag;
	string prefix;
	string column_name;
	int column_length;
};

}
}

#endif

