#include "sa_global.h"

namespace ai
{
namespace app
{

using namespace ai::sg;

sa_global& sa_global::instance(sat_ctx *SAT)
{
	sa_global *& _instance = reinterpret_cast<sa_global *&>(SAT->_SAT_mgr_array[SA_GLOBAL_MANAGER]);
	if (_instance == NULL)
		_instance = new sa_global();
	return *_instance;
}

void sa_global::open()
{
	if (is_open) {
		// 进程号没有改变，不需要重新打开
		if (SAT->_SAT_last_pid == open_pid)
			return;

		close();
		// 删除原文件，忽略错误
		(void)::remove(db_name.c_str());
	}

	// 打开缓存文件
	gpenv& env_mgr = gpenv::instance();
	string appdir = env_mgr.getenv("APPROOT");

	string cache_dir = appdir + "/.Component";
	if (::mkdir(cache_dir.c_str(), 0700) == -1 && errno != EEXIST)
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Can't create directory {1}") % cache_dir).str(APPLOCALE));

	cache_dir += "/sa";
	if (::mkdir(cache_dir.c_str(), 0700) == -1 && errno != EEXIST)
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Can't create directory {1}") % cache_dir).str(APPLOCALE));

	db_name = cache_dir + "/global_" + boost::lexical_cast<string>(SAT->_SAT_last_pid) + ".sqlite3";

	// 连接数据库
	if (sqlite3_open_v2(db_name.c_str(), &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, NULL) != SQLITE_OK)
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Can't open database: {1}") % sqlite3_errmsg(db)).str(APPLOCALE));

	// 关闭同步方式
	if (sqlite3_exec(db, "PRAGMA synchronous=0", NULL, NULL, NULL) != SQLITE_OK)
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: PRAGMA synchronous=0 failed: {1}") % sqlite3_errmsg(db)).str(APPLOCALE));

	string sql = "create table global(data text, record_count integer)";
	(void)sqlite3_exec(db, sql.c_str(), NULL, NULL, NULL);

	sql = "insert into global(data, record_count) values('', 0)";
	(void)sqlite3_exec(db, sql.c_str(), NULL, NULL, NULL);

	sql = "update global set data = :1, record_count = :2";
	if (sqlite3_prepare_v2(db, sql.c_str(), -1, &update_stmt, NULL) != SQLITE_OK)
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Can't prepare {1}, {2}") % sql % sqlite3_errmsg(db)).str(APPLOCALE));

	sql = "select data, record_count from global";
	if (sqlite3_prepare_v2(db, sql.c_str(), -1, &select_stmt, NULL) != SQLITE_OK)
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Can't prepare {1}, {2}") % sql % sqlite3_errmsg(db)).str(APPLOCALE));

	global_size = 0;
	const vector<global_field_t>& global_fields = SAP->_SAP_global.global_fields;
	for (int i = FIXED_GLOBAL_SIZE; i < global_fields.size(); i++)
		global_size += global_fields[i].field_size + 1;

	global_record = new char[global_size];
	open_pid = SAT->_SAT_last_pid;
	is_open = true;
}

void sa_global::load()
{
	if (sqlite3_reset(select_stmt) != SQLITE_OK)
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Can't reset select statement, {1}") % sqlite3_errmsg(db)).str(APPLOCALE));

	while (1) {
		switch (sqlite3_step(select_stmt)) {
		case SQLITE_ROW:		// 存在记录
			{
				int record_count = sqlite3_column_int(select_stmt, 1);
				if (record_count != master.record_count)
					throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: records in sqlite {1} is not consistent with in database {2}, please delete database log") % record_count % master.record_count).str(APPLOCALE));

				const char *data = reinterpret_cast<const char *>(sqlite3_column_text(select_stmt, 0));
				const vector<global_field_t>& global_fields = SAP->_SAP_global.global_fields;
				for (int i = FIXED_GLOBAL_SIZE; i < global_fields.size(); i++) {
					memcpy(SAT->_SAT_global[i], data, global_fields[i].field_size + 1);
					data += global_fields[i].field_size + 1;
				}
				return;
			}
		case SQLITE_BUSY:		// 系统忙，重试
			boost::this_thread::yield();
			continue;
		case SQLITE_DONE:	// 找不到记录
			throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Can't get data from select statement, {1}") % sqlite3_errmsg(db)).str(APPLOCALE));
		default:
			throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Can't step select statement, {1}") % sqlite3_errmsg(db)).str(APPLOCALE));
		}
	}
}

void sa_global::save()
{
	int retcode = sqlite3_reset(update_stmt);
	if (retcode != SQLITE_OK && retcode != SQLITE_CONSTRAINT)
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Can't reset update statement, {1}") % sqlite3_errmsg(db)).str(APPLOCALE));

	char *record = global_record;
	const vector<global_field_t>& global_fields = SAP->_SAP_global.global_fields;
	for (int i = FIXED_GLOBAL_SIZE; i < global_fields.size(); i++) {
		memcpy(record, SAT->_SAT_global[i], global_fields[i].field_size + 1);
		record += global_fields[i].field_size + 1;
	}

	if (sqlite3_bind_text(update_stmt, 1, global_record, global_size, SQLITE_STATIC) != SQLITE_OK)
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Can't bind update statement, {1}") % sqlite3_errmsg(db)).str(APPLOCALE));

	if (sqlite3_bind_int(update_stmt, 2, master.record_count) != SQLITE_OK)
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Can't bind update statement, {1}") % sqlite3_errmsg(db)).str(APPLOCALE));

	while (1) {
		switch (sqlite3_step(update_stmt)) {
		case SQLITE_DONE:		// 正确
			return;
		case SQLITE_BUSY:		// 系统忙，重试
			boost::this_thread::yield();
			continue;
		default:
			throw bad_msg(__FILE__, __LINE__, 0,(_("ERROR: Can't step insert statement, {1}") % sqlite3_errmsg(db)).str(APPLOCALE));
		}
	}
}

void sa_global::clean()
{
	close();

	if (!db_name.empty()) {
		::remove(db_name.c_str());
		db_name.clear();
	}
}

sa_global::sa_global()
	: master(SAT->_SAT_dblog[0].master)
{
	open_pid = 0;
	db = NULL;
	update_stmt = NULL;
	select_stmt = NULL;
	global_record = NULL;
	is_open = false;
}

sa_global::~sa_global()
{
	close();
}

void sa_global::close()
{
	delete []global_record;
	global_record = NULL;

	if (update_stmt != NULL) {
		sqlite3_finalize(update_stmt);
		update_stmt = NULL;
	}

	if (select_stmt != NULL) {
		sqlite3_finalize(select_stmt);
		select_stmt = NULL;
	}

	if (db != NULL) {
		sqlite3_close(db);
		db = NULL;
	}

	open_pid = 0;
	is_open = false;
}

}
}

