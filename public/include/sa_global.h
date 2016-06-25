#if !defined(__SA_GLOBAL_H__)
#define __SA_GLOBAL_H__

#include <sqlite3.h>
#include "sap_ctx.h"
#include "sat_ctx.h"

namespace ai
{
namespace app
{

class sa_global : public sa_manager
{
public:
	static sa_global& instance(sat_ctx *SAT);

	void open();
	void load();
	void save();
	void clean();

private:
	sa_global();
	virtual ~sa_global();

	void close();

	master_dblog_t& master;

	pid_t open_pid;
	std::string db_name;
	// SQLite数据库连接
	sqlite3 *db;
	sqlite3_stmt *update_stmt;
	sqlite3_stmt *select_stmt;
	int global_size;
	char *global_record;
	bool is_open;

	friend class sat_ctx;
};

}
}

#endif

