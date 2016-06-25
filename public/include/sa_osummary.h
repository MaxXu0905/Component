#if !defined(__SA_OSUMMARY_H__)
#define __SA_OSUMMARY_H__

#include "sa_output.h"
#include "sql_control.h"
#include "struct_dynamic.h"

namespace ai
{
namespace app
{

using namespace std;
using namespace ai::sg;
using namespace ai::scci;

class sa_osummary : public sa_output
{
public:
	sa_osummary(sa_base& _sa, int _flags);
	~sa_osummary();

	void flush(bool completed);
	void close();
	void clean();
	void recover();
	void dump(std::ostream& os);
	void rollback(const std::string& raw_file_name, int file_sn);

private:
	void create_stmt(int idx);
	void set_data(int idx);
	void execute_update();
	std::string get_binder(const sa_summary_field_t& field);

	struct osummary_item_t
	{
		Generic_Statement *insert_stmt;
		struct_dynamic *insert_data;
		Generic_Statement *update_stmt;
		struct_dynamic *update_data;
		Generic_Statement *delete_stmt;
		struct_dynamic *delete_data;

		osummary_item_t() {
			insert_stmt = NULL;
			insert_data = NULL;
			update_stmt = NULL;
			update_data = NULL;
			delete_stmt = NULL;
			delete_data = NULL;
		}
	};

	vector<osummary_item_t> items;
	bool rollback_called;
};

}
}

#endif

