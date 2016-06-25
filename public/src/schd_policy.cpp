#include "schd_policy.h"
#include "schd_ctx.h"

namespace ai
{
namespace app
{

using namespace ai::sg;

boost::once_flag schd_policy::once_flag = BOOST_ONCE_INIT;
schd_policy * schd_policy::_instance = NULL;

schd_policy& schd_policy::instance()
{
	if (_instance == NULL)
		boost::call_once(once_flag, schd_policy::init_once);
	return *_instance;
}

schd_policy::~schd_policy()
{
	if (select_stmt != NULL) {
		sqlite3_finalize(select_stmt);
		select_stmt = NULL;
	}

	if (update_stmt != NULL) {
		sqlite3_finalize(update_stmt);
		update_stmt = NULL;
	}

	if (insert_stmt != NULL) {
		sqlite3_finalize(insert_stmt);
		insert_stmt = NULL;
	}

	if (db != NULL) {
		sqlite3_close(db);
		db = NULL;
	}
}

void schd_policy::load()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	if (!is_open)
		open();

	if (sqlite3_reset(select_stmt) != SQLITE_OK)
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Can't reset select statement, {1}") % sqlite3_errmsg(db)).str(APPLOCALE));

	schd_ctx *SCHD = schd_ctx::instance();
	while (1) {
		switch (sqlite3_step(select_stmt)) {
		case SQLITE_ROW:		// 存在记录
			{
				string entry_name = reinterpret_cast<const char *>(sqlite3_column_text(select_stmt, 0));

				map<string, schd_entry_t>::iterator iter = SCHD->_SCHD_entries.find(entry_name);
				if (iter == SCHD->_SCHD_entries.end()) {
					GPP->write_log(__FILE__, __LINE__, (_("ERROR: entry_name {1} not found in configuration") % entry_name).str(APPLOCALE));
					continue;
				}

				schd_entry_t& entry = iter->second;
				entry.policy = sqlite3_column_int(select_stmt, 1);
				entry.file_count = sqlite3_column_int(select_stmt, 2);
				entry.file_pending = sqlite3_column_int(select_stmt, 3);
				entry.status = sqlite3_column_int(select_stmt, 4);
				entry.retries = sqlite3_column_int(select_stmt, 5);
				entry.undo_id = sqlite3_column_int(select_stmt, 6);
				break;
			}
		case SQLITE_BUSY:		// 系统忙，重试
			boost::this_thread::yield();
			continue;
		case SQLITE_DONE:	// 找不到记录
			return;
		default:
			throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Can't step select statement, {1}") % sqlite3_errmsg(db)).str(APPLOCALE));
		}
	}
}

void schd_policy::save(const string& entry_name, const schd_entry_t& entry)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, (_("entry_name={1}, policy={2}, file_count={3}, status={4}") % entry_name % entry.policy % entry.file_count % entry.status).str(APPLOCALE), NULL);
#endif

	int retcode;

	if (!is_open)
		open();

	retcode = sqlite3_reset(insert_stmt);
	if (retcode != SQLITE_OK && retcode != SQLITE_CONSTRAINT)
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Can't reset insert statement, {1}") % sqlite3_errmsg(db)).str(APPLOCALE));

	if (sqlite3_bind_text(insert_stmt, 1, entry_name.c_str(), -1, SQLITE_STATIC) != SQLITE_OK)
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Can't bind insert statement, {1}") % sqlite3_errmsg(db)).str(APPLOCALE));

	if (sqlite3_bind_int(insert_stmt, 2, entry.policy) != SQLITE_OK)
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Can't bind insert statement, {1}") % sqlite3_errmsg(db)).str(APPLOCALE));

	if (sqlite3_bind_int(insert_stmt, 3, entry.file_count) != SQLITE_OK)
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Can't bind insert statement, {1}") % sqlite3_errmsg(db)).str(APPLOCALE));

	if (sqlite3_bind_int(insert_stmt, 4, entry.file_pending) != SQLITE_OK)
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Can't bind insert statement, {1}") % sqlite3_errmsg(db)).str(APPLOCALE));

	if (sqlite3_bind_int(insert_stmt, 5, entry.status) != SQLITE_OK)
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Can't bind insert statement, {1}") % sqlite3_errmsg(db)).str(APPLOCALE));

	if (sqlite3_bind_int(insert_stmt, 6, entry.retries) != SQLITE_OK)
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Can't bind insert statement, {1}") % sqlite3_errmsg(db)).str(APPLOCALE));

	if (sqlite3_bind_int(insert_stmt, 7, entry.undo_id) != SQLITE_OK)
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Can't bind insert statement, {1}") % sqlite3_errmsg(db)).str(APPLOCALE));

	while (1) {
		switch (sqlite3_step(insert_stmt)) {
		case SQLITE_DONE:		// 正确
			return;
		case SQLITE_BUSY:			// 系统忙，重试
			boost::this_thread::yield();
			continue;
		default:
			break;
		}

		break;
	}

	retcode = sqlite3_reset(update_stmt);
	if (retcode != SQLITE_OK && retcode != SQLITE_CONSTRAINT)
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Can't reset update statement, {1}") % sqlite3_errmsg(db)).str(APPLOCALE));

	if (sqlite3_bind_int(update_stmt, 1, entry.policy) != SQLITE_OK)
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Can't bind update statement, {1}") % sqlite3_errmsg(db)).str(APPLOCALE));

	if (sqlite3_bind_int(update_stmt, 2, entry.file_count) != SQLITE_OK)
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Can't bind update statement, {1}") % sqlite3_errmsg(db)).str(APPLOCALE));

	if (sqlite3_bind_int(update_stmt, 3, entry.file_pending) != SQLITE_OK)
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Can't bind update statement, {1}") % sqlite3_errmsg(db)).str(APPLOCALE));

	if (sqlite3_bind_int(update_stmt, 4, entry.status) != SQLITE_OK)
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Can't bind update statement, {1}") % sqlite3_errmsg(db)).str(APPLOCALE));

	if (sqlite3_bind_int(update_stmt, 5, entry.retries) != SQLITE_OK)
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Can't bind update statement, {1}") % sqlite3_errmsg(db)).str(APPLOCALE));

	if (sqlite3_bind_int(update_stmt, 6, entry.undo_id) != SQLITE_OK)
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Can't bind update statement, {1}") % sqlite3_errmsg(db)).str(APPLOCALE));

	if (sqlite3_bind_text(update_stmt, 7, entry_name.c_str(), -1, SQLITE_STATIC) != SQLITE_OK)
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

void schd_policy::reset()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	if (!is_open)
		open();

	string sql= "delete from entry_policy";
	if (sqlite3_exec(db, sql.c_str(), NULL, NULL, NULL) != SQLITE_OK)
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Can't delete {1}, {2}") % sql % sqlite3_errmsg(db)).str(APPLOCALE));
}

schd_policy::schd_policy()
{
	db = NULL;
	insert_stmt = NULL;
	update_stmt = NULL;
	select_stmt = NULL;
	is_open = false;
}

void schd_policy::open()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	schd_ctx *SCHD = schd_ctx::instance();
	sgc_ctx *SGC = SGT->SGC();
	sg_bboard_t *bbp = SGC->_SGC_bbp;
	sg_bbparms_t& bbparms = bbp->bbparms;
	gpenv& env_mgr = gpenv::instance();
	string appdir = env_mgr.getenv("APPROOT");

	string db_name = appdir + "/.Component";
	if (::mkdir(db_name.c_str(), 0700) == -1 && errno != EEXIST)
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Can't create directory {1}") % db_name).str(APPLOCALE));

	db_name += "/";
	db_name += boost::lexical_cast<string>(bbparms.ipckey & ((1 << MCHIDSHIFT) - 1));
	if (::mkdir(db_name.c_str(), 0700) == -1 && errno != EEXIST)
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Can't create directory {1}") % db_name).str(APPLOCALE));

	db_name += "/schd_clt_";
	db_name += SCHD->_SCHD_suffixURI;
	db_name += ".db";

	// 连接数据库
	if (sqlite3_open_v2(db_name.c_str(), &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, NULL) != SQLITE_OK)
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Can't open database: {1}") % sqlite3_errmsg(db)).str(APPLOCALE));

	// 关闭同步方式
	if (sqlite3_exec(db, "PRAGMA synchronous=0", NULL, NULL, NULL) != SQLITE_OK)
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: PRAGMA synchronous=0 failed: {1}") % sqlite3_errmsg(db)).str(APPLOCALE));

	string sql= "create table entry_policy(entry_name text, policy integer, file_count integer, file_pending integer, status integer, retries integer, undo_id integer, primary key(entry_name))";
	(void)sqlite3_exec(db, sql.c_str(), NULL, NULL, NULL);

	sql = "insert into entry_policy(entry_name, policy, file_count, file_pending, status, retries, undo_id) values(:1, :2, :3, :4, :5, :6, :7)";
	if (sqlite3_prepare_v2(db, sql.c_str(), -1, &insert_stmt, NULL) != SQLITE_OK)
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Can't prepare {1}, {2}") % sql % sqlite3_errmsg(db)).str(APPLOCALE));

	sql = "update entry_policy set policy = :1, file_count = :2, file_pending = :3, status = :4, retries = :5 and undo_id = :6  where entry_name = :7";
	if (sqlite3_prepare_v2(db, sql.c_str(), -1, &update_stmt, NULL) != SQLITE_OK)
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Can't prepare {1}, {2}") % sql % sqlite3_errmsg(db)).str(APPLOCALE));

	sql = "select entry_name, policy, file_count, file_pending, status, retries, undo_id from entry_policy";
	if (sqlite3_prepare_v2(db, sql.c_str(), -1, &select_stmt, NULL) != SQLITE_OK)
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Can't prepare {1}, {2}") % sql % sqlite3_errmsg(db)).str(APPLOCALE));

	is_open = true;
}

void schd_policy::init_once()
{
	_instance = new schd_policy();
}

}
}

