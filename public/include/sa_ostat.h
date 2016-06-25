#if !defined(__SA_OSTAT_H__)
#define __SA_OSTAT_H__

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

int const SA_INSERT_MASK = 0x1;
int const SA_UPDATE_MASK = 0x2;

class sa_ostat : public sa_output
{
public:
	sa_ostat(sa_base& _sa, int _flags);
	~sa_ostat();

	void init();
	void init2();
	void open();
	int write(int input_idx);
	void flush(bool completed);
	void close();
	void clean();
	void recover();
	void dump(std::ostream& os);
	void rollback(const string& raw_file_name, int file_sn);

private:
	void create_stmt(int idx);
	void set_data(int input_idx, int idx);
	void execute_update();
	void execute_update(int idx);
	void destroy();

	struct ostat_item_t
	{
		string table_name;
		Generic_Statement *insert_stmt;
		struct_dynamic *insert_data;
		Generic_Statement *update_stmt;
		struct_dynamic *update_data;
		vector<int> keys;
		int *sort_array;
 		int do_update;

		ostat_item_t() {
			insert_stmt = NULL;
			insert_data = NULL;
			update_stmt = NULL;
			update_data = NULL;
			sort_array = NULL;
			do_update = 0;
		}
	};

	vector<int> cond_idx;
	vector<int> table_idx;
	vector<compiler::func_type> cond_funs;
	vector<compiler::func_type> table_funs;
	vector<ostat_item_t> items;
	vector<bool> rollback_called;
};

}
}

#endif

