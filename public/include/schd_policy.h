#if !defined(__SCHD_POLICY_H__)
#define __SCHD_POLICY_H__

#include <sqlite3.h>
#include "sap_ctx.h"
#include "sat_ctx.h"

namespace ai
{
namespace app
{

class schd_entry_t;

class schd_policy : public sa_manager
{
public:
	static schd_policy& instance();
	virtual ~schd_policy();

	void load();
	void save(const std::string& entry_name, const schd_entry_t& entry);
	void reset();

private:
	schd_policy();

	void open();

	// SQLite数据库连接
	sqlite3 *db;
	sqlite3_stmt *insert_stmt;
	sqlite3_stmt *update_stmt;
	sqlite3_stmt *select_stmt;
	bool is_open;

	static void init_once();

	static boost::once_flag once_flag;
	static schd_policy *_instance;
};

}
}

#endif

