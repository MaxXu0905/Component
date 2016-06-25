#include "dup_svc.h"

using namespace std;
int const DUP_PREFIX = 1002000;

namespace ai
{
namespace app
{

dup_svc::dup_svc()
{
	DUP = dup_ctx::instance();
}

dup_svc::~dup_svc()
{
}

void dup_svc::pre_extract_input()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	sqlite3 *& db = DUP->_DUP_sqlite3_db;
	if (rows > 1 && sqlite3_exec(db, "begin", NULL, NULL, NULL) != SQLITE_OK)
		throw bad_msg(__FILE__, __LINE__, SGESYSTEM, (_("ERROR: Can't start transaction")).str(APPLOCALE));
}

void dup_svc::post_extract_input()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	sqlite3 *& db = DUP->_DUP_sqlite3_db;
	if (rows > 1 && sqlite3_exec(db, "commit", NULL, NULL, NULL) != SQLITE_OK)
		throw bad_msg(__FILE__, __LINE__, SGESYSTEM, (_("ERROR: Can't commit transaction")).str(APPLOCALE));
}

void dup_svc::get_table(string &title)
{
	string sql = "select name from sqlite_master where type='table'";
	sqlite3 *& db = DUP->_DUP_sqlite3_db;
	char **dbResult;
	int nRow,nColumn,index;
	char *errmsg = NULL;
	string s;
	int result = sqlite3_get_table(db,sql.c_str(), &dbResult, &nRow, &nColumn, &errmsg);
	if (SQLITE_OK == result) {
		index = nColumn;
		stringstream ss;
		
		ss << nRow;

		
		for (int i=0; i<nRow; i++) {
			for (int j=0; j<nColumn; j++) {
				s += dbResult[j];
				s += ":";
				s += dbResult[index];
				s += "\n";
				index ++;
			}
		}
	}
	
	GPP->write_log(__FILE__, __LINE__, (_("INFO: DUP tables {1},{2} .") % title % s).str(APPLOCALE));
	sqlite3_free_table(dbResult);
	
}

bool dup_svc::handle_meta()
{
	bool retval = false;
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", &retval);
#endif

	sqlite3 *& db = DUP->_DUP_sqlite3_db;
	int len = rqst->placeholder;
	char *data = reinterpret_cast<char *>(&rqst->placeholder) + sizeof(int);
	char *data_end = data + len;
/*  内存方式，不能关闭
	DUP->close_sqlite();
	if (!DUP->open_sqlite())
		return retval;
*/
	
	string info_title = "before";
	//get_table(info_title);
	while (data < data_end) {
		string table_name = data;
		string sql = "drop table if exists ";
		sql += table_name;
		data += table_name.length() + 1;
		//先终止prepare的语句，否则不能drop
		boost::unordered_map<std::string, dup_stmt_t>& stmts = DUP->_DUP_sqlite3_stmts;
		boost::unordered_map<string, dup_stmt_t>::iterator iter = stmts.find(table_name);
		if (iter != stmts.end()) {
			dup_stmt_t& item = iter->second;
			sqlite3_finalize(item.insert_stmt);
			sqlite3_finalize(item.select_stmt);
			stmts.erase(iter);
		}

		int status = sqlite3_exec(db, sql.c_str(), NULL, NULL, NULL);
		if (status != SQLITE_OK && status != SQLITE_ERROR) {
			GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't drop {1}, {2}, retval={3}") % sql % sqlite3_errmsg(db) % retval).str(APPLOCALE));
			return retval;
		}
	}
	info_title = "after";
	//get_table(info_title);

	retval = true;
	return retval;
}

bool dup_svc::handle_record(int output_idx)
{
	bool retval = false;
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, (_("output_idx={1}") % output_idx).str(APPLOCALE), &retval);
#endif

	sqlite3 *& db = DUP->_DUP_sqlite3_db;
	boost::unordered_map<std::string, dup_stmt_t>& stmts = DUP->_DUP_sqlite3_stmts;
	sqlite3_stmt *insert_stmt;
	sqlite3_stmt *select_stmt;
	int retcode;

	//如果key为空，表示不查重
	if (input[DUP_TABLE_IDX][0] == '\0') {
		status[output_idx] = 0;
		return true;
	}		

	if (!DUP->_DUP_enable_cross) {
		char *table_name = input[DUP_TABLE_IDX];

		// 创建Statement
		boost::unordered_map<string, dup_stmt_t>::iterator iter = stmts.find(table_name);
		if (iter == stmts.end()) {
			dup_stmt_t item;
			string sql;

			sql = (boost::format("insert into %1%(id, key) values(:1, :2)") % table_name).str();
			if (sqlite3_prepare_v2(db, sql.c_str(), -1, &insert_stmt, NULL) != SQLITE_OK) {
				if (DUP->_DUP_auto_create) {
					if (!create_table(table_name)) {
						GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't create table {1}, {2}") % table_name % sqlite3_errmsg(db)).str(APPLOCALE));
						return retval;
					}

					retcode = sqlite3_reset(insert_stmt);
					if (retcode != SQLITE_OK && retcode != SQLITE_CONSTRAINT) {
						GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't reset insert statement, {1}") % sqlite3_errmsg(db)).str(APPLOCALE));
						return retval;
					}

					if (sqlite3_prepare_v2(db, sql.c_str(), -1, &insert_stmt, NULL) != SQLITE_OK) {
						GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't prepare {1}, {2}") % sql % sqlite3_errmsg(db)).str(APPLOCALE));
						return retval;
					}
				} else {
					GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't prepare {1}, {2}") % sql % sqlite3_errmsg(db)).str(APPLOCALE));
					return retval;
				}
			}

			sql = (boost::format("select 0 from %1% where key = :1 and id = :2") % table_name).str();
			if (sqlite3_prepare_v2(db, sql.c_str(), -1, &select_stmt, NULL) != SQLITE_OK) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't prepare {1}, {2}") % sql % sqlite3_errmsg(db)).str(APPLOCALE));
				sqlite3_finalize(insert_stmt);
				return retval;
			}

			item.insert_stmt = insert_stmt;
			item.select_stmt = select_stmt;
			stmts[table_name] = item;
		} else {
			insert_stmt = iter->second.insert_stmt;
			select_stmt = iter->second.select_stmt;
		}

		retcode = sqlite3_reset(insert_stmt);
		if (retcode != SQLITE_OK && retcode != SQLITE_CONSTRAINT) {
			GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't reset insert statement, {1}") % sqlite3_errmsg(db)).str(APPLOCALE));
			return retval;
		}

		char *id = input[DUP_ID_IDX];
		char *key = input[DUP_KEY_IDX];

		if (sqlite3_bind_text(insert_stmt, 1, id, -1, SQLITE_STATIC) != SQLITE_OK) {
			GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't bind insert statement, {1}") % sqlite3_errmsg(db)).str(APPLOCALE));
			return retval;
		}

		if (sqlite3_bind_text(insert_stmt, 2, key, -1, SQLITE_STATIC) != SQLITE_OK) {
			GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't bind insert statement, {1}") % sqlite3_errmsg(db)).str(APPLOCALE));
			return retval;
		}

TRY_AGAIN1:
		switch (sqlite3_step(insert_stmt)) {
		case SQLITE_DONE:		// 正确
			status[output_idx] = 0;
			break;
		case SQLITE_BUSY:		// 系统忙，重试
			boost::this_thread::yield();
			goto TRY_AGAIN1;
		case SQLITE_CONSTRAINT:	// 重单
			switch (check_data(select_stmt, id, key)) {
			case -1:	// 异常
				return retval;
			case 0:		// 重单
				status[output_idx] = DUP_PREFIX;
				break;
			case 1:		// 不是重单
				status[output_idx] = 0;
				break;
			}
			break;
		default:
			GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't step insert statement, {1}") % sqlite3_errmsg(db)).str(APPLOCALE));
			return retval;
		}
	} else {
		char *table_name = input[DUP_TABLE_IDX];
		time_t begin_time = static_cast<time_t>(atol(input[DUP_BEGIN_IDX]));
		time_t end_time = static_cast<time_t>(atol(input[DUP_END_IDX]));

		// 创建Statement
		boost::unordered_map<string, dup_stmt_t>::iterator iter = stmts.find(table_name);
		if (iter == stmts.end()) {
			dup_stmt_t item;
			string sql;

			sql = (boost::format("insert into %1%(id, key, begin, end) values(:1, :2, :3, :4)") % table_name).str();
			if (sqlite3_prepare_v2(db, sql.c_str(), -1, &insert_stmt, NULL) != SQLITE_OK) {
				if (DUP->_DUP_auto_create) {
					if (!create_table(table_name)) {
						GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't create table {1}, {2}") % table_name % sqlite3_errmsg(db)).str(APPLOCALE));
						return retval;
					}

					retcode = sqlite3_reset(insert_stmt);
					if (retcode != SQLITE_OK && retcode != SQLITE_CONSTRAINT) {
						GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't reset insert statement, {1}") % sqlite3_errmsg(db)).str(APPLOCALE));
						return retval;
					}

					if (sqlite3_prepare_v2(db, sql.c_str(), -1, &insert_stmt, NULL) != SQLITE_OK) {
						GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't prepare {1}, {2}") % sql % sqlite3_errmsg(db)).str(APPLOCALE));
						return retval;
					}
				} else {
					GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't prepare {1}, {2}") % sql % sqlite3_errmsg(db)).str(APPLOCALE));
					return retval;
				}
			}

			sql = (boost::format("select id from %1% where key = :1 and (begin >= :2 and begin < :3 or end > :2 and end <= :3)") % table_name).str();
			if (sqlite3_prepare_v2(db, sql.c_str(), -1, &select_stmt, NULL) != SQLITE_OK) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't prepare {1}, {2}") % sql % sqlite3_errmsg(db)).str(APPLOCALE));
				sqlite3_finalize(insert_stmt);
				return retval;
			}

			item.insert_stmt = insert_stmt;
			item.select_stmt = select_stmt;
			stmts[table_name] = item;
		} else {
			insert_stmt = iter->second.insert_stmt;
			select_stmt = iter->second.select_stmt;
		}

		retcode = sqlite3_reset(insert_stmt);
		if (retcode != SQLITE_OK && retcode != SQLITE_CONSTRAINT) {
			GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't reset insert statement, {1}") % sqlite3_errmsg(db)).str(APPLOCALE));
			return retval;
		}

		char *id = input[DUP_ID_IDX];
		char *key = input[DUP_KEY_IDX];

		switch (check_cross_data(select_stmt, begin_time, end_time, id, key)) {
		case -1:	// 异常
			return retval;
		case 0:		// 重单
			status[output_idx] = DUP_PREFIX;
			retval = true;
			return retval;
		case 1:		// 不是重单
			status[output_idx] = 0;
			break;
		}

		if (sqlite3_bind_text(insert_stmt, 1, id, -1, SQLITE_STATIC) != SQLITE_OK) {
			GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't bind insert statement, {1}") % sqlite3_errmsg(db)).str(APPLOCALE));
			return retval;
		}

		if (sqlite3_bind_text(insert_stmt, 2, key, -1, SQLITE_STATIC) != SQLITE_OK) {
			GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't bind insert statement, {1}") % sqlite3_errmsg(db)).str(APPLOCALE));
			return retval;
		}

		if (sqlite3_bind_int(insert_stmt, 3, begin_time) != SQLITE_OK) {
			GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't bind insert statement, {1}") % sqlite3_errmsg(db)).str(APPLOCALE));
			return retval;
		}

		if (sqlite3_bind_int(insert_stmt, 4, end_time) != SQLITE_OK) {
			GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't bind insert statement, {1}") % sqlite3_errmsg(db)).str(APPLOCALE));
			return retval;
		}

TRY_AGAIN2:
		switch (sqlite3_step(insert_stmt)) {
		case SQLITE_DONE:		// 正确
			break;
		case SQLITE_BUSY:		// 系统忙，重试
			boost::this_thread::yield();
			goto TRY_AGAIN2;
		default:
			GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't step insert statement, {1}") % sqlite3_errmsg(db)).str(APPLOCALE));
			return retval;
		}
	}

	retval = true;
	return retval;
}

// 检查记录是否为重单
// -1: 错误
// 0:  重单
// 1: 不是重单
int dup_svc::check_data(sqlite3_stmt *stmt, const char *id, const char *key)
{
	int retval = -1;
#if defined(DEBUG)
	scoped_debug<int> debug(500, __PRETTY_FUNCTION__, (_("id={1}, key={2}") % id % key).str(APPLOCALE), &retval);
#endif
	sqlite3 *& db = DUP->_DUP_sqlite3_db;

	if (sqlite3_reset(stmt) != SQLITE_OK) {
		GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't reset select statement, {1}") % sqlite3_errmsg(db)).str(APPLOCALE));
		return retval;
	}

	if (sqlite3_bind_text(stmt, 1, key, -1, SQLITE_STATIC) != SQLITE_OK) {
		GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't bind select statement, {1}") % sqlite3_errmsg(db)).str(APPLOCALE));
		return retval;
	}

	if (sqlite3_bind_text(stmt, 2, id, -1, SQLITE_STATIC) != SQLITE_OK) {
		GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't bind select statement, {1}") % sqlite3_errmsg(db)).str(APPLOCALE));
		return retval;
	}

TRY_AGAIN3:
	switch (sqlite3_step(stmt)) {
	case SQLITE_DONE:		// 找不到记录
		retval = 0;
		return retval;
	case SQLITE_ROW:		// 存在记录
		retval = 1;
		return retval;
	case SQLITE_BUSY:		// 系统忙，重试
		boost::this_thread::yield();
		goto TRY_AGAIN3;
	default:
		GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't step select statement, {1}") % sqlite3_errmsg(db)).str(APPLOCALE));
		return retval;
	}
}

// 检查交叉记录是否为重单
// -1: 错误
// 0:  重单
// 1: 不是重单
int dup_svc::check_cross_data(sqlite3_stmt *stmt, time_t begin_time, time_t end_time, const char *id, const char *key)
{
	int retval = -1;
#if defined(DEBUG)
	scoped_debug<int> debug(500, __PRETTY_FUNCTION__, (_("begin_time={1}, end_time={2}, id={3}, key={4}") % begin_time % end_time % id % key).str(APPLOCALE), &retval);
#endif
	sqlite3 *& db = DUP->_DUP_sqlite3_db;

	if (sqlite3_reset(stmt) != SQLITE_OK) {
		GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't reset select statement, {1}") % sqlite3_errmsg(db)).str(APPLOCALE));
		return retval;
	}

	if (sqlite3_bind_text(stmt, 1, key, -1, SQLITE_STATIC) != SQLITE_OK) {
		GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't bind select statement, {1}") % sqlite3_errmsg(db)).str(APPLOCALE));
		return retval;
	}

	if (sqlite3_bind_int(stmt, 2, begin_time) != SQLITE_OK) {
		GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't bind select statement, {1}") % sqlite3_errmsg(db)).str(APPLOCALE));
		return retval;
	}

	if (sqlite3_bind_int(stmt, 3, end_time) != SQLITE_OK) {
		GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't bind select statement, {1}") % sqlite3_errmsg(db)).str(APPLOCALE));
		return retval;
	}

TRY_AGAIN4:
	switch (sqlite3_step(stmt)) {
	case SQLITE_ROW:		// 存在记录
		{
			const char *orig_id = reinterpret_cast<const char *>(sqlite3_column_text(stmt, 0));
			if (strcmp(orig_id, id) == 0) {
				retval = 1;
				return retval;
			} else {
				retval = 0;
				return retval;
			}
		}
	case SQLITE_BUSY:		// 系统忙，重试
		boost::this_thread::yield();
		goto TRY_AGAIN4;
	case SQLITE_DONE:	// 找不到记录
		retval = 0;
		return retval;
	default:
		GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't step select statement, {1}") % sqlite3_errmsg(db)).str(APPLOCALE));
		return retval;
	}
}

bool dup_svc::create_table(const string& table_name)
{
	bool retval = false;
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, (_("table_name={1}") % table_name).str(APPLOCALE), &retval);
#endif
	sqlite3 *& db = DUP->_DUP_sqlite3_db;

	if (!DUP->_DUP_enable_cross) {
		// 第一次执行失败，可能没有创建表
		string csql = (boost::format("create table %1%(id text, key text)") % table_name).str();
		if (sqlite3_exec(db, csql.c_str(), NULL, NULL, NULL) != SQLITE_OK) {
			GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't create {1}, {2}") % csql % sqlite3_errmsg(db)).str(APPLOCALE));
			return retval;
		}

		csql = (boost::format("create unique index idx_%1% on %1%(key)") % table_name).str();
		if (sqlite3_exec(db, csql.c_str(), NULL, NULL, NULL) != SQLITE_OK) {
			GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't create {1}, {2}") % csql % sqlite3_errmsg(db)).str(APPLOCALE));
			return retval;
		}
	} else {
		// 第一次执行失败，可能没有创建表
		string csql = (boost::format("create table %1%(id text, key text, begin integer, end integer)") % table_name).str();
		if (sqlite3_exec(db, csql.c_str(), NULL, NULL, NULL) != SQLITE_OK) {
			GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't create {1}, {2}") % csql % sqlite3_errmsg(db)).str(APPLOCALE));
			return retval;
		}

		csql = (boost::format("create unique index idx_%1% on %1%(key, begin)") % table_name).str();
		if (sqlite3_exec(db, csql.c_str(), NULL, NULL, NULL) != SQLITE_OK) {
			GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't create {1}, {2}") % csql % sqlite3_errmsg(db)).str(APPLOCALE));
			return retval;
		}
	}

	retval = true;
	return retval;
}

}
}

