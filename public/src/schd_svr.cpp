#include "schd_svr.h"

using namespace ai::sg;

namespace ai
{
namespace app
{

namespace po = boost::program_options;

schd_svr::schd_svr()
	: api_mgr(sg_api::instance(SGT))
{
	SCHD = schd_ctx::instance();
}

schd_svr::~schd_svr()
{
}

int schd_svr::svrinit(int argc, char **argv)
{
	int retval = -1;
#if defined(DEBUG)
	scoped_debug<int> debug(1000, __PRETTY_FUNCTION__, (_("argc={1}") % argc).str(APPLOCALE), &retval);
#endif

	string directory;
	int polltime = 0;
	string encoding;

	po::options_description desc((_("Allowed options")).str(APPLOCALE));
	desc.add_options()
		("help", (_("produce help message")).str(APPLOCALE).c_str())
		("brokerURI,B", po::value<string>(&SCHD->_SCHD_brokerURI)->default_value(DFLT_BROKER_URI), (_("specify JMS broker URI")).str(APPLOCALE).c_str())
		("suffixURI,S", po::value<string>(&SCHD->_SCHD_suffixURI)->required(), (_("specify JMS producer/consumer/logger topic URI suffix")).str(APPLOCALE).c_str())
		("log2jms,e", (_("enable LOG to JMS process")).str(APPLOCALE).c_str())
		("directory,d", po::value<string>(&directory), (_("specify directories to watch")).str(APPLOCALE).c_str())
		("polltime,t", po::value<int>(&polltime), (_("specify seconds to check log")).str(APPLOCALE).c_str())
		("encoding,E", po::value<string>(&encoding), (_("specify encoding of log content")).str(APPLOCALE).c_str())
	;

	po::variables_map vm;

	try {
		po::store(po::parse_command_line(argc, argv, desc), vm);

		if (vm.count("help")) {
			std::cout << desc << std::endl;
			GPP->write_log((_("INFO: {1} exit normally in help mode.") % GPP->_GPP_procname).str(APPLOCALE));
			retval = 0;
			return retval;
		}

		po::notify(vm);
	} catch (po::required_option& ex) {
		std::cout << ex.what() << std::endl;
		std::cout << desc << std::endl;
		SGT->_SGT_error_code = SGEINVAL;
		GPP->write_log(__FILE__, __LINE__, (_("ERROR: {1} start failure, {2}.") % GPP->_GPP_procname % ex.what()).str(APPLOCALE));
		return retval;
	}

	SCHD->_SCHD_producerURI = PREFIX_PRODUCER_URI;
	SCHD->_SCHD_producerURI += SCHD->_SCHD_suffixURI;
	SCHD->set_service();

	// 设置环境变量
	gpenv& env_mgr = gpenv::instance();
	string str = (boost::format("SASUFFIXURI=%1%") % SCHD->_SCHD_suffixURI).str();
	env_mgr.putenv(str.c_str());

	sgc_ctx *SGC = SGT->SGC();
	sg_bboard_t *bbp = SGC->_SGC_bbp;
	sg_bbparms_t& bbparms = bbp->bbparms;
	string appdir = env_mgr.getenv("APPROOT");

	string cache_dir = appdir + "/.Component";
	if (::mkdir(cache_dir.c_str(), 0700) == -1 && errno != EEXIST) {
		SGT->_SGT_error_code = SGEOS;
		GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't create directory {1}") % cache_dir).str(APPLOCALE));
		return retval;
	}

	cache_dir += "/";
	cache_dir += boost::lexical_cast<string>(bbparms.ipckey & ((1 << MCHIDSHIFT) - 1));
	if (::mkdir(cache_dir.c_str(), 0700) == -1 && errno != EEXIST) {
		SGT->_SGT_error_code = SGEOS;
		GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't create directory {1}") % cache_dir).str(APPLOCALE));
		return retval;
	}

	SCHD->_SCHD_db_name = cache_dir;
	SCHD->_SCHD_db_name += "/schd_svr_";
	SCHD->_SCHD_db_name += SCHD->_SCHD_suffixURI;
	SCHD->_SCHD_db_name += ".db";

	// 连接数据库
	if (sqlite3_open_v2(SCHD->_SCHD_db_name.c_str(), &SCHD->_SCHD_sqlite3_db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, NULL) != SQLITE_OK) {
		GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't open database: {1}") % sqlite3_errmsg(SCHD->_SCHD_sqlite3_db)).str(APPLOCALE));
		return retval;
	}

	// 关闭同步方式
	if (sqlite3_exec(SCHD->_SCHD_sqlite3_db, "PRAGMA synchronous=0", NULL, NULL, NULL) != SQLITE_OK) {
		GPP->write_log(__FILE__, __LINE__, (_("ERROR: PRAGMA synchronous=0 failed: {1}") % sqlite3_errmsg(SCHD->_SCHD_sqlite3_db)).str(APPLOCALE));
		return retval;
	}

	// 设置PERSIST方式
	if (sqlite3_exec(SCHD->_SCHD_sqlite3_db, "PRAGMA journal_mode=PERSIST", NULL, NULL, NULL) != SQLITE_OK) {
		GPP->write_log(__FILE__, __LINE__, (_("ERROR: PRAGMA journal_mode=PERSIST failed: {1}") % sqlite3_errmsg(SCHD->_SCHD_sqlite3_db)).str(APPLOCALE));
		return retval;
	}

	// 准备SQL语句
	if (!prepare())
		return retval;

	// 加载状态数据
	if (!select())
		return retval;

	SCHD->connect();

	// 启动日志转换进程
	if (vm.count("log2jms")) {
		sys_func& func_mgr = sys_func::instance();
		string command = "run_log2jms -B \"";
		command += SCHD->_SCHD_brokerURI;
		command += "\" -S ";
		command += SCHD->_SCHD_suffixURI;

		if (!directory.empty()) {
			command += " -d ";
			command += directory;
		}

		if (polltime > 0) {
			command += " -t ";
			command += boost::lexical_cast<string>(polltime);
		}

		if (!encoding.empty()) {
			command += " -e ";
			command += encoding;
		}

		command += " -b";
		func_mgr.new_task(command.c_str(), NULL, 0);
	}

	// 发布特有的服务
	if (!advertise())
		return retval;

	GPP->write_log((_("INFO: {1} -g {2} -p {3} started successfully.") % GPP->_GPP_procname % SGC->_SGC_proc.grpid % SGC->_SGC_proc.svrid).str(APPLOCALE));
	retval = 0;
	return retval;
}

int schd_svr::svrfini()
{
	int retval = -1;
#if defined(DEBUG)
	scoped_debug<int> debug(1000, __PRETTY_FUNCTION__, "", &retval);
#endif

	sgc_ctx *SGC = SGT->SGC();
	GPP->write_log((_("INFO: {1} -g {2} -p {3} stopped successfully.") % GPP->_GPP_procname % SGC->_SGC_proc.grpid % SGC->_SGC_proc.svrid).str(APPLOCALE));
	retval = 0;
	return retval;
}

bool schd_svr::prepare()
{
	bool retval = false;
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", &retval);
#endif

	sqlite3 *& db = SCHD->_SCHD_sqlite3_db;
	string sql;

	sql = "create table schd_table(pid integer, entry_name text, hostname text, status integer, start_time integer, stop_time integer, primary key(pid, start_time))";
	// 忽略创建失败
	(void)sqlite3_exec(db, sql.c_str(), NULL, NULL, NULL);

	// 创建Statement
	sql = "select pid, entry_name, hostname, status, start_time, stop_time from schd_table";
	if (sqlite3_prepare_v2(db, sql.c_str(), -1, &SCHD->_SCHD_select_stmt, NULL) != SQLITE_OK) {
		GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't prepare {1}, {2}") % sql % sqlite3_errmsg(db)).str(APPLOCALE));
		return retval;
	}

	sql = "insert into schd_table(pid, entry_name, hostname, status, start_time, stop_time) values(:1, :2, :3, :4, :5, :6)";
	if (sqlite3_prepare_v2(db, sql.c_str(), -1, &SCHD->_SCHD_insert_stmt, NULL) != SQLITE_OK) {
		GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't prepare {1}, {2}") % sql % sqlite3_errmsg(db)).str(APPLOCALE));
		return retval;
	}

	sql = "update schd_table set status = :1, stop_time = :2 where pid = :3 and start_time = :4";
	if (sqlite3_prepare_v2(db, sql.c_str(), -1, &SCHD->_SCHD_update_stmt, NULL) != SQLITE_OK) {
		GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't prepare {1}, {2}") % sql % sqlite3_errmsg(db)).str(APPLOCALE));
		return retval;
	}

	sql = "delete from schd_table where pid = :1 and start_time = :2";
	if (sqlite3_prepare_v2(db, sql.c_str(), -1, &SCHD->_SCHD_delete_stmt, NULL) != SQLITE_OK) {
		GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't prepare {1}, {2}") % sql % sqlite3_errmsg(db)).str(APPLOCALE));
		return retval;
	}

	retval = true;
	return retval;
}

bool schd_svr::select()
{
	bool retval = false;
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", &retval);
#endif

	schd_agent_proc_t item;
	sqlite3 *& db = SCHD->_SCHD_sqlite3_db;
	sqlite3_stmt *& stmt = SCHD->_SCHD_select_stmt;

	if (sqlite3_reset(stmt) != SQLITE_OK) {
		GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't reset select statement, {1}") % sqlite3_errmsg(db)).str(APPLOCALE));
		return retval;
	}

	while (1) {
		switch (sqlite3_step(stmt)) {
		case SQLITE_ROW:		// 存在记录
			item.pid = sqlite3_column_int(stmt, 0);
			strcpy(item.entry_name, reinterpret_cast<const char *>(sqlite3_column_text(stmt, 1)));
			strcpy(item.hostname, reinterpret_cast<const char *>(sqlite3_column_text(stmt, 2)));
			item.status = sqlite3_column_int(stmt, 3);
			item.start_time = sqlite3_column_int64(stmt, 4);
			item.stop_time = sqlite3_column_int64(stmt, 5);
			SCHD->_SCHD_managed_procs.insert(item);
			continue;
		case SQLITE_BUSY:		// 系统忙，重试
			boost::this_thread::yield();
			continue;
		case SQLITE_DONE:		// 找不到记录
			break;
		default:
			GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't step select statement, {1}") % sqlite3_errmsg(db)).str(APPLOCALE));
			return retval;
		}

		break;
	}

	retval = true;
	return retval;
}

bool schd_svr::advertise()
{
	bool retval = false;
#if defined(DEBUG)
	scoped_debug<bool> debug(1000, __PRETTY_FUNCTION__, "", &retval);
#endif

	sg_svcdsp_t *svcdsp = SGP->_SGP_svcdsp;
	if (svcdsp == NULL || svcdsp->index == -1) {
		GPP->write_log(__FILE__, __LINE__, (_("ERROR: No operation provided on sgcompile time" )).str(APPLOCALE));
		return retval;
	}

	string class_name = svcdsp->class_name;
	sg_api& api_mgr = sg_api::instance(SGT);

	if (!api_mgr.advertise(SCHD->_SCHD_svc_name, class_name)) {
		GPP->write_log(__FILE__, __LINE__, (_("ERROR: Failed to advertise operation {1}") % SCHD->_SCHD_svc_name).str(APPLOCALE));
		return retval;
	}

	retval = true;
	return retval;
}

}
}

