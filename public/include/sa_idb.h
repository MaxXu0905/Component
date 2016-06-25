#if !defined(__SA_IDB_H__)
#define __SA_IDB_H__

#include "sa_struct.h"
#include "sa_input.h"
#include "sql_control.h"

namespace ai
{
namespace app
{

using namespace ai::sg;
using namespace ai::scci;
namespace bi = boost::interprocess;

int const OPTIONS_PER_ROWS = 0x1;
int const OPTIONS_PER_LOOP = 0x2;

class sa_idb : public sa_input
{
public:
	sa_idb(sa_base& _sa, int _flags);
	~sa_idb();

	void init();
	void init2();
	bool open();
	void flush(bool completed);
	void close();
	void clean();
	void recover();
	int read();

protected:
	struct sa_sql_struct_t
	{
		std::string sql_str;
		std::vector<int> binds;
		Generic_Statement *stmt;
		struct_dynamic *data;

		sa_sql_struct_t() {
			stmt = NULL;
			data = NULL;
		}
	};

	bool to_fields(const input_record_t& input_record, Generic_ResultSet *rset);
	bool set_sqls(std::vector<sa_sql_struct_t>& sqls, const std::string& value);
	bool set_binds(sa_sql_struct_t& sql);

	// 参数列表
	int options;							// 选项
	time_t align_time;						// 开始处理的相对时间点
	int max_rows;							// 一次处理的最大记录数
	int per_rows;							// 模拟一个文件处理的最大记录数
	std::vector<sa_sql_struct_t> pre_sql;	// 处理前的执行语句
	sa_sql_struct_t loop_sql;				// 导出记录的循环语句
	std::vector<sa_sql_struct_t> exp_sql;	// 导出语句
	std::vector<sa_sql_struct_t> post_sql;	// 处理后的执行语句
	int sleep_time;							// 处理一次的间隔时间

	// 成员操作变量
	bool first_time;
	Generic_ResultSet *loop_rset;			// 导出记录的循环结果集
	Generic_ResultSet *exp_rset;			// 导出结果集
	Generic_Statement *stmt;				// 当前导出语句
	int exp_idx;							// 当前导出语句索引
	datetime sysdate;						// 当前时间
	int rows;								// 当前循环处理记录数
	bool real_eof;							// 真正的处理结束
};

}
}

#endif

