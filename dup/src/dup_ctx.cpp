#include "dup_ctx.h"

namespace ai
{
namespace app
{

using namespace ai::sg;
typedef boost::tokenizer<boost::char_separator<char> > tokenizer;

boost::once_flag dup_ctx::once_flag = BOOST_ONCE_INIT;
dup_ctx * dup_ctx::_instance = NULL;

dup_ctx * dup_ctx::instance()
{
	if (_instance == NULL)
		boost::call_once(once_flag, dup_ctx::init_once);
	return _instance;
}

bool dup_ctx::get_config()
{
	string config;
	sg_api& api_mgr = sg_api::instance();
	gpp_ctx *GPP = gpp_ctx::instance();
	sgt_ctx *SGT = sgt_ctx::instance();
	bool retval = false;
#if defined(DEBUG)
	scoped_debug<bool> debug(1000, __PRETTY_FUNCTION__, "", &retval);
#endif

	string key = string("DUP.") + _DUP_svc_key;
	// 获取DUPCHK配置
	if (!api_mgr.get_config(config, key, _DUP_version)) {
		SGT->_SGT_error_code = SGESYSTEM;
		GPP->write_log(__FILE__, __LINE__, (_("ERROR: Failure to load DUP configuration")).str(APPLOCALE));
		return retval;
	}

	// 加载配置
	try {
		load_config(config);
	} catch (exception& ex) {
		SGT->_SGT_error_code = SGESYSTEM;
		GPP->write_log(__FILE__, __LINE__, ex.what());
		return retval;
	}

	retval = true;
	return retval;
}

void dup_ctx::set_map(field_map_t& data_map) const
{
	data_map[DUP_TABLE_NAME] = DUP_TABLE_IDX;
	data_map[DUP_ID_NAME] = DUP_ID_IDX;
	data_map[DUP_KEY_NAME] = DUP_KEY_IDX;

	if (_DUP_enable_cross) {
		data_map[DUP_BEGIN_NAME] = DUP_BEGIN_IDX;
		data_map[DUP_END_NAME] = DUP_END_IDX;
	}
}

void dup_ctx::set_svcname(int partition_id, string& svc_name) const
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	svc_name = "DUP_";
	svc_name += _DUP_svc_key;
	svc_name += "_";
	svc_name += boost::lexical_cast<string>(partition_id);
}

bool dup_ctx::open_sqlite()
{
	bool retval = false;
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", &retval);
#endif
	gpp_ctx *GPP = gpp_ctx::instance();

	// 连接数据库
	if (_DUP_memorydb) {		
		if (sqlite3_open_v2(":memory:", &_DUP_sqlite3_db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, NULL) != SQLITE_OK) {
			GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't open database: {1}") % sqlite3_errmsg(_DUP_sqlite3_db)).str(APPLOCALE));
			return retval;
		}
		// 设置PERSIST方式
		//Note that the journal_mode for an in-memory database is either MEMORY or OFF and can not be changed to a different value
		if (sqlite3_exec(_DUP_sqlite3_db, "PRAGMA journal_mode=OFF", NULL, NULL, NULL) != SQLITE_OK) {
			GPP->write_log(__FILE__, __LINE__, (_("ERROR: PRAGMA journal_mode=PERSIST failed: {1}") % sqlite3_errmsg(_DUP_sqlite3_db)).str(APPLOCALE));
			return retval;
		}
	}
	else {
		if (sqlite3_open_v2(_DUP_db_name.c_str(), &_DUP_sqlite3_db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, NULL) != SQLITE_OK) {
			GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't open database: {1}") % sqlite3_errmsg(_DUP_sqlite3_db)).str(APPLOCALE));
			return retval;
		}
		// 设置PERSIST方式
		//Note that the journal_mode for an in-memory database is either MEMORY or OFF and can not be changed to a different value
		if (sqlite3_exec(_DUP_sqlite3_db, "PRAGMA journal_mode=PERSIST", NULL, NULL, NULL) != SQLITE_OK) {
			GPP->write_log(__FILE__, __LINE__, (_("ERROR: PRAGMA journal_mode=PERSIST failed: {1}") % sqlite3_errmsg(_DUP_sqlite3_db)).str(APPLOCALE));
			return retval;
		}
	}

	// 关闭同步方式
	if (sqlite3_exec(_DUP_sqlite3_db, "PRAGMA synchronous=0", NULL, NULL, NULL) != SQLITE_OK) {
		GPP->write_log(__FILE__, __LINE__, (_("ERROR: PRAGMA synchronous=0 failed: {1}") % sqlite3_errmsg(_DUP_sqlite3_db)).str(APPLOCALE));
		return retval;
	}

	

	// 设置auto_vacuum=FULL
	if (sqlite3_exec(_DUP_sqlite3_db, "PRAGMA auto_vacuum=FULL", NULL, NULL, NULL) != SQLITE_OK) {
		GPP->write_log(__FILE__, __LINE__, (_("ERROR: PRAGMA auto_vacuum=FULL failed: {1}") % sqlite3_errmsg(_DUP_sqlite3_db)).str(APPLOCALE));
		return retval;
	}

	// 设置EXCLUSIVE方式
	//The "temp" database (in which TEMP tables and indices are stored) and in-memory databases always uses exclusive locking mode
	if (_DUP_exclusive) {
		if (sqlite3_exec(_DUP_sqlite3_db, "PRAGMA locking_mode=EXCLUSIVE", NULL, NULL, NULL) != SQLITE_OK) {
			GPP->write_log(__FILE__, __LINE__, (_("ERROR: PRAGMA locking_mode=EXCLUSIVE failed: {1}") % sqlite3_errmsg(_DUP_sqlite3_db)).str(APPLOCALE));
			return retval;
		}
	}

	// 设置缓存大小
	if (_DUP_cache_size) {
		string cache_str = (boost::format("PRAGMA cache_size=%1%") % _DUP_cache_size).str();
		if (sqlite3_exec(_DUP_sqlite3_db, cache_str.c_str(), NULL, NULL, NULL) != SQLITE_OK) {
			GPP->write_log(__FILE__, __LINE__, (_("ERROR: {1} failed: {2}") % cache_str % sqlite3_errmsg(_DUP_sqlite3_db)).str(APPLOCALE));
			return retval;
		}
	}

	retval = true;
	return retval;
}

void dup_ctx::close_sqlite()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	for (boost::unordered_map<string, dup_stmt_t>::iterator iter = _DUP_sqlite3_stmts.begin(); iter != _DUP_sqlite3_stmts.end(); ++iter) {
		dup_stmt_t& item = iter->second;
		sqlite3_finalize(item.insert_stmt);
		sqlite3_finalize(item.select_stmt);
	}
	_DUP_sqlite3_stmts.clear();

	if (_DUP_sqlite3_db != NULL) {
		sqlite3_close(_DUP_sqlite3_db);
		_DUP_sqlite3_db = NULL;
	}
}


dup_ctx::dup_ctx()
{
	_DUP_version = -1;
	_DUP_enable_cross = false;
	_DUP_partitions = -1;
	_DUP_mids = -1;
	_DUP_all_partitions = -1;
	_DUP_auto_create = true;
	_DUP_partition_id = -1;
	_DUP_cache_size = 0;
	_DUP_exclusive = false;
	_DUP_sqlite3_db = NULL;
	_DUP_memorydb = false;
}

dup_ctx::~dup_ctx()
{
	close_sqlite();
}

void dup_ctx::load_config(const string& config)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif
	bpt::iptree pt;
	gpenv& env_mgr = gpenv::instance();
	istringstream is(config);

	bpt::read_xml(is, pt);

	try {
		const bpt::iptree& v_pt = pt.get_child("resource");

		env_mgr.expand_var(_DUP_db_name, v_pt.get<string>("database"));

		string enable_cross = v_pt.get<string>("enable_cross", "N");
		if (strcasecmp(enable_cross.c_str(), "Y") == 0)
			_DUP_enable_cross = true;
		else if (strcasecmp(enable_cross.c_str(), "N") == 0)
			_DUP_enable_cross = false;
		else
			throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: resource.enable_cross value invalid.")).str(APPLOCALE));

		_DUP_partitions = v_pt.get<int>("partitions");

		// 查找当前的lmid
		sgt_ctx *SGT = sgt_ctx::instance();
		sgc_ctx *SGC = SGT->SGC();
		const char *clmid = SGC->mid2lmid(SGC->_SGC_proc.mid);
		if (clmid == NULL)
			throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: Can't find hostid for node id {1}") % SGC->_SGC_proc.mid).str(APPLOCALE));

		_DUP_mids = 0;

		bool found = false;
		string lmids;
		env_mgr.expand_var(lmids, v_pt.get<string>("hostid"));
		boost::char_separator<char> sep(" \t\b");
		tokenizer lmid_tokens(lmids, sep);
		for (tokenizer::iterator iter = lmid_tokens.begin(); iter != lmid_tokens.end(); ++iter) {
			string lmid = *iter;

			if (lmid == clmid) {
				_DUP_partition_id = _DUP_mids * _DUP_partitions;
				found = true;
			}

			_DUP_mids++;
		}

		_DUP_all_partitions = _DUP_mids * _DUP_partitions;

		if (!found)
			throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: Current node {1} not in the dup list.") % clmid).str(APPLOCALE));

		string auto_create = v_pt.get<string>("auto_create", "Y");
		if (strcasecmp(auto_create.c_str(), "Y") == 0)
			_DUP_auto_create = true;
		else if (strcasecmp(auto_create.c_str(), "N") == 0)
			_DUP_auto_create = false;
		else
			throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: resource.auto_create value invalid.")).str(APPLOCALE));

		
		string memorydb = v_pt.get<string>("memorydb", "N");
		if (strcasecmp(memorydb.c_str(), "Y") == 0)
			_DUP_memorydb= true;
		else if (strcasecmp(memorydb.c_str(), "N") == 0)
			_DUP_memorydb= false;
		else
			throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: resource.memorydb value invalid.")).str(APPLOCALE));
	} catch (bpt::ptree_error& ex) {
		throw sg_exception(__FILE__, __LINE__, SGESYSTEM, 0, (_("ERROR: Parse section resource failure, {1}") % ex.what()).str(APPLOCALE));
	}
}

void dup_ctx::init_once()
{
	_instance = new dup_ctx();
}

}
}

