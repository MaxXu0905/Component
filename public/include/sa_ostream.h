#if !defined(__SA_OSTREAM_H__)
#define __SA_OSTREAM_H__

#include "sa_output.h"
#include "sql_control.h"
#include "struct_dynamic.h"

namespace ai
{
namespace app
{

using namespace std;
using namespace ai::sg;

// Use struct but not union because string type can't be used in union.
struct value_type
{
	int int_value;
	long long_value;
	float float_value;
	double double_value;
	string string_value;
};

// Head/tail statistics for an output file.
class stat_file
{
public:
	stat_file(const vector<dst_field_t>& fields_);
	~stat_file();

	// Statistics values.
	void statistics(const char **input, const char **global, const char **output);

	vector<value_type> values;

private:
	// Fill value at first time.
	void init(const char **input, const char **global, const char **output);
	// Fill value after first time.
	void set(const char **input, const char **global, const char **output);

	const vector<dst_field_t>& fields;
	bool first;
};

class distr_file
{
public:
	distr_file(const dst_file_t& dst_file_, vector<slave_dblog_t>& slaves_, int idx_);
	~distr_file();

	slave_dblog_t& slave_log();

	const dst_file_t& dst_file;
	ofstream ofs;
	stat_file *hfile;
	stat_file *tfile;

private:
	std::vector<slave_dblog_t>& slaves;
	int idx;
};

class sa_ostream : public sa_output
{
public:
	sa_ostream(sa_base& _sa, int _flags);
	~sa_ostream();

	void init();
	void init2();
	void open();
	int write(int input_idx);
	void flush(bool completed);
	void close();
	void complete();
	void clean();
	void recover();
	void dump(ostream& os);
	void rollback(const string& raw_file_name, int file_sn);
	void global_rollback();

private:
	// Write body record for external interface.
	void write(const char **input, const char **global, const char **in, distr_file& file, const dst_record_t& dst_record);
	// Write body record for internal interface.
	void write(const char **input, const char **global, const char **in, distr_file& file, const vector<dst_field_t>& fields);
	// Write head/tail record
	void write_head_tail(const char **global, const char **in, distr_file& file,
		stat_file& sfile, const dst_record_t& dst_record);
	// Write head record
	void write_head(const char **global, const char **in, distr_file& file);
	// Set field format
	void set_format(ostream& os, const dst_field_t& field);

	typedef boost::unordered_map<string, boost::shared_ptr<distr_file> > file_map_t;

	const vector<dst_file_t> *dst_files;
	file_map_t file_map;
	vector<int> cond_idx;
	vector<int> file_idx;
	vector<compiler::func_type> cond_funs;
	vector<compiler::func_type> file_funs;
};

}
}

#endif

