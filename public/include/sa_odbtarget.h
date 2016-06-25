#if !defined(__SA_ODBTARGET_H__)
#define __SA_ODBTARGET_H__

#include "sa_output.h"
#include "sql_control.h"
#include "struct_dynamic.h"

namespace ai
{
namespace app
{

using namespace ai::sg;
using namespace ai::scci;

class sa_odbtarget : public sa_output
{
public:
	sa_odbtarget(sa_base& _sa, int _flags);
	~sa_odbtarget();

	int write(int input_idx);
	void flush(bool completed);
	void close();
	void clean();
	void recover();
	void dump(std::ostream& os);
	void rollback(const std::string& raw_file_name, int file_sn);

private:
	void create_stmt();

	std::string table_name;
	int per_records;
	int rollback_type;
	std::string rollback_script;
	Generic_Statement *stmt;
	struct_dynamic *data;
	Generic_Statement *del_stmt;
	struct_dynamic *del_data;
	int update_count;
	bool rollback_called;
};

}
}

#endif

