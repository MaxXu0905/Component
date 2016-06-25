#if !defined(__DBLOG_H__)
#define __DBLOG_H__

#include "sa_struct.h"
#include "sa_manager.h"

namespace ai
{
namespace app
{

using namespace ai::sg;
using namespace ai::scci;
using namespace std;

int const ERROR_TYPE_SUCCESS = 0;
int const ERROR_TYPE_PARTIAL = 1;
int const ERROR_TYPE_FATAL = 2;

class dblog_manager;

struct master_dblog_t
{
public:
	master_dblog_t();
	void clear();

	bool insert_flag;
	int record_count;
	int record_normal;
	int record_invalid;
	int record_error;
	size_t break_point;
	size_t break_normal;
	size_t break_invalid;
	size_t break_error;
	int error_type;
	int completed;
};

struct slave_dblog_t
{
	slave_dblog_t();

	bool insert_flag;
	int file_serial;
	std::string file_name;
	int record_count;
	size_t break_point;
};

struct dblog_item_t
{
	master_dblog_t master;
	std::vector<slave_dblog_t> slaves;

	void clear();
};

struct file_log_t
{
	file_log_t();

	int file_count;
	int file_normal;
	int file_fatal;
	int file_dup;
	long record_count;
	long record_normal;
	long record_invalid;
	long record_error;
};

class dblog_manager : public sa_manager
{
public:
	static dblog_manager& instance(sat_ctx *SAT);

 	void select();
	void update();
 	void insert();
	void complete();
	void get_sn();
	void dump(ostream& os);

private:
	dblog_manager();
	virtual ~dblog_manager();

	struct_dynamic *master_select;
	struct_dynamic *master_insert;
	struct_dynamic *master_update;
	struct_dynamic *slave_select;
	struct_dynamic *slave_insert;
	struct_dynamic *slave_update;
	struct_dynamic *seq_select;
	struct_dynamic *completed_update;

	Generic_Statement *master_select_stmt;
	Generic_Statement *master_insert_stmt;
	Generic_Statement *master_update_stmt;
	Generic_Statement *slave_select_stmt;
	Generic_Statement *slave_insert_stmt;
	Generic_Statement *slave_update_stmt;
	Generic_Statement *seq_select_stmt;
	Generic_Statement *completed_stmt;

	friend class sat_ctx;
};

}
}

#endif

