#include "sa_internal.h"

namespace ai
{
namespace app
{

using namespace ai::sg;
using namespace ai::scci;

master_dblog_t::master_dblog_t()
{
	clear();
}

void master_dblog_t::clear()
{
	insert_flag = true;
	record_count = 0;
	record_normal = 0;
	record_invalid = 0;
	record_error = 0;
	break_point = 0;
	break_normal = 0;
	break_invalid = 0;
	break_error = 0;
	error_type = 0;
	completed = 0;
}

slave_dblog_t::slave_dblog_t()
{
	insert_flag = true;
	file_serial = 0;
	record_count = 0;
	break_point = 0;
}

void dblog_item_t::clear()
{
	master.clear();
	slaves.clear();
}

file_log_t::file_log_t()
{
	file_count = 0;
	file_normal = 0;
	file_fatal = 0;
	file_dup = 0;
	record_count = 0;
	record_normal = 0;
	record_invalid = 0;
	record_error = 0;
}

dblog_manager& dblog_manager::instance(sat_ctx *SAT)
{
	dblog_manager *& _instance = reinterpret_cast<dblog_manager *&>(SAT->_SAT_mgr_array[SA_DBLOG_MANAGER]);
	if (_instance == NULL)
		_instance = new dblog_manager();
	return *_instance;
}

dblog_manager::dblog_manager()
{
	if (SAT->_SAT_db == NULL) {
		// 连接数据库
		try {
			map<string, string> conn_info;

			database_factory& factory_mgr = database_factory::instance();
			factory_mgr.parse(SAP->_SAP_resource.openinfo, conn_info);

			// 设置环境变量
			gpenv& env_mgr = gpenv::instance();
			for (map<string, string>::const_iterator iter = conn_info.begin(); iter != conn_info.end(); ++iter) {
				string env_str = "OPENINFO_";
				env_str += boost::to_upper_copy(iter->first);
				env_str += "=";
				env_str += iter->second;

				env_mgr.putenv(env_str.c_str());
			}

			SAT->_SAT_db = factory_mgr.create(SAP->_SAP_resource.dbname);
			SAT->_SAT_db->connect(conn_info);
		} catch (exception& ex) {
			delete SAT->_SAT_db;
			SAT->_SAT_db = NULL;
			throw bad_sql(__FILE__, __LINE__, SGEOS, 0, ex.what());
		}
	}

	string sql_str;
	Generic_Database *db = SAT->_SAT_db;

	master_select = db->create_data();
	master_insert = db->create_data();
	master_update = db->create_data();
	slave_select = db->create_data();
	slave_insert = db->create_data();
	slave_update = db->create_data();
	seq_select = db->create_data();
	completed_update = db->create_data();

	master_select_stmt = NULL;
	master_insert_stmt = NULL;
	master_update_stmt = NULL;
	slave_select_stmt = NULL;
	slave_insert_stmt = NULL;
	slave_update_stmt = NULL;
	seq_select_stmt = NULL;
	completed_stmt = NULL;

	sql_str = (boost::format("select sa_id{int}, file_sn{int}, record_count{int}, record_normal{int}, "
		"record_invalid{int}, record_error{int}, break_point{long}, break_normal{long}, break_invalid{long}, "
		"break_error{long}, login_time{datetime}, finish_time{datetime}, file_time{datetime}, "
		"error_type{int}, completed{int}, pid{int} from log_integrator "
		"where integrator_name = :integrator_name{char[%1%]} and raw_file_name = :raw_file_name{char[%2%]}")
		% INTEGRATE_NAME_LEN % RAW_FILE_NAME_LEN).str();
	master_select->setSQL(sql_str);
	master_select_stmt = db->create_statement();
	master_select_stmt->bind(master_select);

	sql_str = (boost::format("insert into log_integrator(integrator_name, sa_id, raw_file_name, file_sn, "
		"record_count, record_normal, record_invalid, record_error, break_point, break_normal, "
		"break_invalid, break_error, login_time, finish_time, file_time, error_type, completed, hostname, pid) "
		"values(:integrator_name{char[%1%]}, :sa_id{int}, :raw_file_name{char[%2%]}, :file_sn{int}, "
		":record_count{int}, :record_normal{int}, :record_invalid{int}, :record_error{int}, :break_point{long}, "
		":break_normal{long}, :break_invalid{long}, :break_error{long}, :login_time{datetime}, "
		":finish_time{datetime}, :file_time{datetime}, :error_type{int}, :completed{int}, :hostname{char[%3%]}, "
		":pid{int})")
		% INTEGRATE_NAME_LEN % RAW_FILE_NAME_LEN % MAX_IDENT).str();
	master_insert->setSQL(sql_str);
	master_insert_stmt = db->create_statement();
	master_insert_stmt->bind(master_insert);

	sql_str = (boost::format("update log_integrator set record_count = :record_count{int}, "
		"record_normal = :record_normal{int}, record_invalid = :record_invalid{int}, "
		"record_error = :record_error{int}, break_point = :break_point{long}, "
		"break_normal = :break_normal{long}, break_invalid = :break_invalid{long}, "
		"break_error = :break_error{long}, finish_time = :finish_time{datetime}, "
		"error_type = :error_type{int}, completed = :completed{int}, hostname = :hostname{char[%1%]} "
		"where integrator_name = :integrator_name{char[%2%]} and sa_id = :sa_id{int} "
		"and raw_file_name = :raw_file_name{char[%3%]}")
		% MAX_IDENT % INTEGRATE_NAME_LEN % RAW_FILE_NAME_LEN).str();
	master_update->setSQL(sql_str);
	master_update_stmt = db->create_statement();
	master_update_stmt->bind(master_update);

	sql_str = (boost::format("select sa_id{int}, file_serial{int}, file_name{char[%1%]}, "
		"record_count{int}, break_point{long} from log_integrator_list "
		"where integrator_name = :integrator_name{char[%2%]} and file_sn = :file_sn{int} "
		"order by file_serial")
		% RAW_FILE_NAME_LEN % MAX_IDENT).str();
	slave_select->setSQL(sql_str);
	slave_select_stmt = db->create_statement();
	slave_select_stmt->bind(slave_select);

	sql_str = (boost::format("insert into log_integrator_list(integrator_name, file_sn, sa_id, "
		"file_serial, file_name, record_count, break_point) values(:integrator_name{char[%1%]}, "
		":file_sn{int}, :sa_id{int}, :file_serial{int}, :file_name{char[%2%]}, :record_count{int}, "
		":break_point{long})")
		% INTEGRATE_NAME_LEN % RAW_FILE_NAME_LEN).str();
	slave_insert->setSQL(sql_str);
	slave_insert_stmt = db->create_statement();
	slave_insert_stmt->bind(slave_insert);

	sql_str = (boost::format("update log_integrator_list set record_count = :record_count{int}, "
		"break_point = :break_point{long} where integrator_name = :integrator_name{char[%1%]} "
		"and file_sn = :file_sn{int} and sa_id = :sa_id{int} and file_serial = :file_serial{int} "
		"and file_name = :file_name{char[%2%]}")
		% INTEGRATE_NAME_LEN % RAW_FILE_NAME_LEN).str();
	slave_update->setSQL(sql_str);
	slave_update_stmt = db->create_statement();
	slave_update_stmt->bind(slave_update);

	sql_str = "select ";
	sql_str += db->get_seq_item("file_sequence", SEQ_ACTION_NEXTVAL);
	sql_str += " seq{int} from dual";

	seq_select->setSQL(sql_str);
	seq_select_stmt = db->create_statement();
	seq_select_stmt->bind(seq_select);

	sql_str = (boost::format("update log_integrator set completed = 1 "
		"where integrator_name = :integrator_name{char[%1%]} and sa_id = :sa_id{int} "
		"and raw_file_name = :raw_file_name{char[%2%]}")
		% INTEGRATE_NAME_LEN % RAW_FILE_NAME_LEN).str();
	completed_update->setSQL(sql_str);
	completed_stmt = db->create_statement();
	completed_stmt->bind(completed_update);
}

dblog_manager::~dblog_manager()
{
	Generic_Database *db = SAT->_SAT_db;
	if (db != NULL) {
		db->terminate_statement(completed_stmt);
		db->terminate_statement(seq_select_stmt);
		db->terminate_statement(slave_update_stmt);
		db->terminate_statement(slave_insert_stmt);
		db->terminate_statement(slave_select_stmt);
		db->terminate_statement(master_update_stmt);
		db->terminate_statement(master_insert_stmt);
		db->terminate_statement(master_select_stmt);
	}

	delete completed_update;
	delete seq_select;
	delete slave_update;
	delete slave_insert;
	delete slave_select;
	delete master_update;
	delete master_insert;
	delete master_select;
}

void dblog_manager::select()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	SAT->_SAT_file_sn = -1;

	// Select from master table.
	master_select_stmt->setString(1, SAP->_SAP_resource.integrator_name);
	master_select_stmt->setString(2, SAT->_SAT_raw_file_name);

	Generic_ResultSet *rset = master_select_stmt->executeQuery();
	BOOST_SCOPE_EXIT((&master_select_stmt)(&rset)) {
		master_select_stmt->closeResultSet(rset);
	} BOOST_SCOPE_EXIT_END

	while (rset->next()) {
		int sa_id = rset->getInt(1);
		dblog_item_t& item = SAT->_SAT_dblog[sa_id];
		master_dblog_t& master = item.master;

		master.insert_flag = false;
		master.record_count = rset->getInt(3);
		master.record_normal = rset->getInt(4);
		master.record_invalid = rset->getInt(5);
		master.record_error = rset->getInt(6);
		master.break_point = static_cast<size_t>(rset->getLong(7));
		master.break_normal = static_cast<size_t>(rset->getLong(8));
		master.break_invalid = static_cast<size_t>(rset->getLong(9));
		master.break_error = static_cast<size_t>(rset->getLong(10));
		master.error_type = rset->getInt(14);
		master.completed = rset->getInt(15);
		SAT->_SAT_last_pid = rset->getInt(16);

		if (SAT->_SAT_file_sn == -1) {
			SAT->_SAT_file_sn = rset->getInt(2);
			SAT->_SAT_login_time = rset->getDatetime(11).time();
			SAT->_SAT_finish_time = rset->getDatetime(12).time();
			SAT->_SAT_file_time = rset->getDatetime(13).time();
		}

		// 如果从断点继续，需要减掉已处理的记录数
		file_log_t& file_log = SAT->_SAT_file_log;
		if (sa_id == 0)
			file_log.record_count -= master.record_count;
		else if (sa_id == SAT->_SAT_dblog.size() - 1)
			file_log.record_normal -= master.record_normal;
		file_log.record_invalid -= master.record_invalid;
		file_log.record_error -= master.record_error;
	}

	if (SAT->_SAT_file_sn == -1) {
		insert();
		return;
	}

	// Select from slave table.
	slave_select_stmt->setString(1, SAP->_SAP_resource.integrator_name);
	slave_select_stmt->setInt(2, SAT->_SAT_file_sn);

	Generic_ResultSet *slave_rset = slave_select_stmt->executeQuery();
	BOOST_SCOPE_EXIT((&slave_select_stmt)(&slave_rset)) {
		slave_select_stmt->closeResultSet(slave_rset);
	} BOOST_SCOPE_EXIT_END

	while (slave_rset->next()) {
		int sa_id = slave_rset->getInt(1);
		vector<slave_dblog_t>& slaves = SAT->_SAT_dblog[sa_id].slaves;

		slave_dblog_t value;
		value.insert_flag = false;
		value.file_serial = slave_rset->getInt(2);
		value.file_name = slave_rset->getString(3);
		value.record_count = slave_rset->getInt(4);
		value.break_point = static_cast<size_t>(slave_rset->getLong(5));
		slaves.push_back(value);
	}
}

void dblog_manager::update()
{
	int master_insert_count = 0;
	int master_update_count = 0;
	int slave_insert_count = 0;
	int slave_update_count = 0;
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	try {
		SAT->_SAT_finish_time = time(0);

		for (int sa_id = 0; sa_id < SAT->_SAT_dblog.size(); sa_id++) {
			dblog_item_t& item = SAT->_SAT_dblog[sa_id];
			master_dblog_t& master = item.master;

			if (master.insert_flag) {
				if (master_insert_count > 0)
					master_insert_stmt->addIteration();

				master_insert_stmt->setString(1, SAP->_SAP_resource.integrator_name);
				master_insert_stmt->setInt(2, sa_id);
				master_insert_stmt->setString(3, SAT->_SAT_raw_file_name);
				master_insert_stmt->setInt(4, SAT->_SAT_file_sn);
				master_insert_stmt->setInt(5, master.record_count);
				master_insert_stmt->setInt(6, master.record_normal);
				master_insert_stmt->setInt(7, master.record_invalid);
				master_insert_stmt->setInt(8, master.record_error);
				master_insert_stmt->setLong(9, static_cast<long>(master.break_point));
				master_insert_stmt->setLong(10, static_cast<long>(master.break_normal));
				master_insert_stmt->setLong(11, static_cast<long>(master.break_invalid));
				master_insert_stmt->setLong(12, static_cast<long>(master.break_error));
				master_insert_stmt->setDatetime(13, datetime(SAT->_SAT_login_time));
				master_insert_stmt->setDatetime(14, datetime(SAT->_SAT_finish_time));
				master_insert_stmt->setDatetime(15, datetime(SAT->_SAT_file_time));
				master_insert_stmt->setInt(16, master.error_type);
				master_insert_stmt->setInt(17, master.completed);
				master_insert_stmt->setString(18, SAT->_SAT_hostname);

				if (++master_insert_count == master_insert->size()) {
					master_insert_stmt->executeUpdate();
					master_insert_count = 0;
				}

				master.insert_flag = false;
			} else {
				if (master_update_count > 0)
					master_update_stmt->addIteration();

				master_update_stmt->setInt(1, master.record_count);
				master_update_stmt->setInt(2, master.record_normal);
				master_update_stmt->setInt(3, master.record_invalid);
				master_update_stmt->setInt(4, master.record_error);
				master_update_stmt->setLong(5, static_cast<long>(master.break_point));
				master_update_stmt->setLong(6, static_cast<long>(master.break_normal));
				master_update_stmt->setLong(7, static_cast<long>(master.break_invalid));
				master_update_stmt->setLong(8, static_cast<long>(master.break_error));
				master_update_stmt->setDatetime(9, datetime(SAT->_SAT_finish_time));
				master_update_stmt->setInt(10, master.error_type);
				master_update_stmt->setInt(11, master.completed);
				master_update_stmt->setString(12, SAT->_SAT_hostname);
				master_update_stmt->setString(13, SAP->_SAP_resource.integrator_name);
				master_update_stmt->setInt(14, sa_id);
				master_update_stmt->setString(15, SAT->_SAT_raw_file_name);

				if (++master_update_count == master_update->size()) {
					master_update_stmt->executeUpdate();
					master_update_count = 0;
				}
			}

			BOOST_FOREACH(slave_dblog_t& slave, item.slaves) {
				if (slave.insert_flag) {
					if (slave_insert_count > 0)
						slave_insert_stmt->addIteration();

					slave_insert_stmt->setString(1, SAP->_SAP_resource.integrator_name);
					slave_insert_stmt->setInt(2, SAT->_SAT_file_sn);
					slave_insert_stmt->setInt(3, sa_id);
					slave_insert_stmt->setInt(4, slave.file_serial);
					slave_insert_stmt->setString(5, slave.file_name);
					slave_insert_stmt->setInt(6, slave.record_count);
					slave_insert_stmt->setLong(7, static_cast<long>(slave.break_point));

					if (++slave_insert_count == slave_insert->size()) {
						slave_insert_stmt->executeUpdate();
						slave_insert_count = 0;
					}

					slave.insert_flag = false;
				} else {
					if (slave_update_count > 0)
						slave_update_stmt->addIteration();

					slave_update_stmt->setInt(1, slave.record_count);
					slave_update_stmt->setLong(2, static_cast<long>(slave.break_point));
					slave_update_stmt->setString(3, SAP->_SAP_resource.integrator_name);
					slave_update_stmt->setInt(4, SAT->_SAT_file_sn);
					slave_update_stmt->setInt(5, sa_id);
					slave_update_stmt->setInt(6, slave.file_serial);
					slave_update_stmt->setString(7, slave.file_name);

					if (++slave_update_count == slave_update->size()) {
						slave_update_stmt->executeUpdate();
						slave_update_count = 0;
					}
				}
			}
		}

		if (master_insert_count > 0)
			master_insert_stmt->executeUpdate();

		if (master_update_count > 0)
			master_update_stmt->executeUpdate();

		if (slave_insert_count > 0)
			slave_insert_stmt->executeUpdate();

		if (slave_update_count > 0)
			slave_update_stmt->executeUpdate();
	} catch (exception& ex) {
		master_insert_stmt->resetIteration();
		master_update_stmt->resetIteration();
		slave_insert_stmt->resetIteration();
		slave_update_stmt->resetIteration();
		throw;
	}
}

void dblog_manager::insert()
{
	int insert_count = 0;
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	get_sn();

	time_t current = time(0);
	if (SAT->_SAT_raw_file_name.empty()) {
		SAT->_SAT_raw_file_name = boost::lexical_cast<string>(SAT->_SAT_file_sn);
		SAT->_SAT_file_time = current;
	}

	try {
		SAT->_SAT_login_time = SAT->_SAT_finish_time = current;
		SAT->_SAT_last_pid = SAP->_SAP_pid;

		for (int sa_id = 0; sa_id < SAT->_SAT_dblog.size(); sa_id++) {
			if (insert_count > 0)
				master_insert_stmt->addIteration();

			master_insert_stmt->setString(1, SAP->_SAP_resource.integrator_name);
			master_insert_stmt->setInt(2, sa_id);
			master_insert_stmt->setString(3, SAT->_SAT_raw_file_name);
			master_insert_stmt->setInt(4, SAT->_SAT_file_sn);
			master_insert_stmt->setInt(5, 0);
			master_insert_stmt->setInt(6, 0);
			master_insert_stmt->setInt(7, 0);
			master_insert_stmt->setInt(8, 0);
			master_insert_stmt->setLong(9, 0);
			master_insert_stmt->setLong(10, 0);
			master_insert_stmt->setLong(11, 0);
			master_insert_stmt->setLong(12, 0);
			master_insert_stmt->setDatetime(13, datetime(SAT->_SAT_login_time));
			master_insert_stmt->setDatetime(14, datetime(SAT->_SAT_finish_time));
			master_insert_stmt->setDatetime(15, datetime(SAT->_SAT_file_time));
			master_insert_stmt->setInt(16, 0);
			master_insert_stmt->setInt(17, 0);
			master_insert_stmt->setString(18, SAT->_SAT_hostname);
			master_insert_stmt->setInt(19, SAT->_SAT_last_pid);

			dblog_item_t& item = SAT->_SAT_dblog[sa_id];
			item.master.insert_flag = false;

			if (++insert_count == master_insert->size()) {
				master_insert_stmt->executeUpdate();
				insert_count = 0;
			}
		}

		if (insert_count > 0)
			master_insert_stmt->executeUpdate();
	} catch (exception& ex) {
		master_insert_stmt->resetIteration();
		throw;
	}
}

void dblog_manager::complete()
{
	int update_count = 0;
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	try {
		for (int sa_id = 0; sa_id < SAT->_SAT_dblog.size(); sa_id++) {
			if (update_count > 0)
				completed_stmt->addIteration();

			completed_stmt->setString(1, SAP->_SAP_resource.integrator_name);
			completed_stmt->setInt(2, sa_id);
			completed_stmt->setString(3, SAT->_SAT_raw_file_name);

			if (++update_count == completed_update->size()) {
				completed_stmt->executeUpdate();
				update_count = 0;
			}
		}

		if (update_count > 0)
			completed_stmt->executeUpdate();
	} catch (exception& ex) {
		completed_stmt->resetIteration();
		throw;
	}
}

void dblog_manager::get_sn()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	Generic_ResultSet *rset = seq_select_stmt->executeQuery();
	BOOST_SCOPE_EXIT((&seq_select_stmt)(&rset)) {
		seq_select_stmt->closeResultSet(rset);
	} BOOST_SCOPE_EXIT_END

	if (rset->next() != Generic_ResultSet::DATA_AVAILABLE)
		throw bad_sql(__FILE__, __LINE__, 0, 0, (_("ERROR: no data found in result set")).str(APPLOCALE));

	SAT->_SAT_file_sn = seq_select_stmt->getInt(1);
}

void dblog_manager::dump(ostream& os)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	os << "create table log_integrator\n"
		<< "(\n"
		<< "\tintegrator_name varchar2(15) not null,\n"
		<< "\tsa_id number(2) not null,\n"
		<< "\traw_file_name varchar2(63) not null,\n"
		<< "\tfile_sn number(10) not null,\n"
		<< "\trecord_count number(10) not null,\n"
		<< "\trecord_normal number(10) not null,\n"
		<< "\trecord_invalid number(10) not null,\n"
		<< "\trecord_error number(10) not null,\n"
		<< "\tbreak_point number(20) not null,\n"
		<< "\tbreak_normal number(20) not null,\n"
		<< "\tbreak_invalid number(20) not null,\n"
		<< "\tbreak_error number(20) not null,\n"
		<< "\tlogin_time date not null,\n"
		<< "\tfinish_time date not null,\n"
		<< "\tfile_time date not null,\n"
		<< "\terror_type number(1),\n"
		<< "\tcompleted number(1),\n"
		<< "\thostname varchar2(31),\n"
		<< "\tpid number(10)\n"
		<< ");\n\n";

	os << "alter table log_integrator\n"
		<< "\tadd constraint pk_log_integrator primary key (integrator_name, sa_id, raw_file_name);\n\n";

	os << "create table log_integrator_list\n"
		<< "(\n"
		<< "\tintegrator_name varchar2(15) not null,\n"
		<< "\tfile_sn number(10) not null,\n"
		<< "\tsa_id number(2) not null,\n"
		<< "\tfile_serial number(4) not null,\n"
		<< "\tfile_name varchar2(63) not null,\n"
		<< "\trecord_count number(10) not null,\n"
		<< "\tbreak_point number(20) not null,\n"
		<< ");\n\n";

	os << "alter table log_integrator_list\n"
		<< "\tadd constraint pk_log_integrator_list primary key (integrator_name, file_sn, sa_id, file_serial, file_name);\n\n";

	os << "create table log_integrator_history\n"
		<< "(\n"
		<< "\tundo_id number(10) not null,\n"
		<< "\tintegrator_name varchar2(15) not null,\n"
		<< "\tsa_id number(2) not null,\n"
		<< "\traw_file_name varchar2(63) not null,\n"
		<< "\tfile_sn number(10) not null,\n"
		<< "\trecord_count number(10) not null,\n"
		<< "\trecord_normal number(10) not null,\n"
		<< "\trecord_invalid number(10) not null,\n"
		<< "\trecord_error number(10) not null,\n"
		<< "\tbreak_point number(20) not null,\n"
		<< "\tbreak_normal number(20) not null,\n"
		<< "\tbreak_invalid number(20) not null,\n"
		<< "\tbreak_error number(20) not null,\n"
		<< "\tlogin_time date not null,\n"
		<< "\tfinish_time date not null,\n"
		<< "\tfile_time date not null,\n"
		<< "\terror_type number(1),\n"
		<< "\tcompleted number(1),\n"
		<< "\thostname varchar2(31),\n"
		<< "\tpid number(10)\n"
		<< ");\n\n";

	os << "create table log_integrator_list_history\n"
		<< "(\n"
		<< "\tundo_id number(10) not null,\n"
		<< "\tintegrator_name varchar2(15) not null,\n"
		<< "\tfile_sn number(10) not null,\n"
		<< "\tsa_id number(2) not null,\n"
		<< "\tfile_serial number(4) not null,\n"
		<< "\tfile_name varchar2(63) not null,\n"
		<< "\trecord_count number(10) not null,\n"
		<< "\tbreak_point number(20) not null,\n"
		<< ");\n\n";
}

}
}

