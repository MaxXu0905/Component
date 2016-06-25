#if !defined(__SA_ODBSOURCE_H__)
#define __SA_ODBSOURCE_H__

#include "sa_output.h"
#include "sql_control.h"
#include "struct_dynamic.h"

namespace ai
{
namespace app
{

using namespace ai::sg;
using namespace ai::scci;

class sa_odbsource : public sa_output
{
public:
	sa_odbsource(sa_base& _sa, int _flags);
	~sa_odbsource();

	int write(int input_idx);
	void flush(bool completed);
	void close();
	void clean();
	void recover();
	void dump(std::ostream& os);
	void rollback(const std::string& raw_file_name, int file_sn);

private:
	void create_stmt();

	struct db_item_t
	{
		Generic_Statement *stmt;
		struct_dynamic *data;
		Generic_Statement *del_stmt;
		struct_dynamic *del_data;

		db_item_t() {
			stmt = NULL;
			data = NULL;
			del_stmt = NULL;
			del_data = NULL;
		}
	};

	std::vector<std::string> tables;
	int per_records;
	int rollback_type;
	std::string rollback_script;
	std::string table_prefix;
	std::vector<db_item_t> items;
	Generic_Statement *stmt;
	int record_serial;
	std::string table_name;
	int update_count;
	bool rollback_called;
};

}
}

#endif

