#include "sa_internal.h"
#include "schd_rpc.h"


namespace ai
{
namespace app
{

namespace bf = boost::filesystem;
namespace bi = boost::interprocess;
namespace bp = boost::posix_time;
using namespace ai::sg;
using namespace ai::scci;

sa_idb::sa_idb(sa_base& _sa, int _flags)
	: sa_input(_sa, _flags)
{
	const map<string, string>& args = adaptor.source.args;
	map<string, string>::const_iterator iter;
	gpenv& env_mgr = gpenv::instance();

	if (adaptor.input_records.size() != 1)
		throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: Only one inputs.input is allowed.")).str(APPLOCALE));

	options = 0;
	iter = args.find("options");
	if (iter != args.end()) {
		const string& options_str = iter->second;
		if (strcasecmp(options_str.c_str(), "PER_ROWS") == 0)
			options |= OPTIONS_PER_ROWS;
		else if (strcasecmp(options_str.c_str(), "PER_LOOP") == 0)
			options |= OPTIONS_PER_LOOP;
		else
			throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: source.args.arg.options value invalid.")).str(APPLOCALE));
	}

	if (options == 0)
		options = OPTIONS_PER_LOOP;

	iter = args.find("align_time");
	if (iter == args.end() || iter->second.empty()) {
		align_time = -1;
	} else {
		try {
			ptime t(iter->second);
			align_time = t.duration();
		} catch (exception& ex) {
			throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: source.args.arg.align_time value invalid, {1}") % ex.what()).str(APPLOCALE));
		}
	}

	iter = args.find("max_rows");
	if (iter == args.end()) {
		max_rows = std::numeric_limits<int>::max();
	} else {
		max_rows = atoi(iter->second.c_str());
		if (max_rows <= 0)
			max_rows = std::numeric_limits<int>::max();
	}

	if (!(options & OPTIONS_PER_ROWS)) {
		per_rows = std::numeric_limits<int>::max();
	} else {
		iter = args.find("per_rows");
		if (iter == args.end()) {
			per_rows = std::numeric_limits<int>::max();
		} else {
			per_rows = atoi(iter->second.c_str());
			if (per_rows <= 0)
				per_rows = std::numeric_limits<int>::max();
		}
	}

	iter = args.find("pre_sql");
	if (iter != args.end() && !iter->second.empty()) {
		if (!set_sqls(pre_sql, iter->second))
			throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: source.args.arg.pre_sql invalid")).str(APPLOCALE));

		BOOST_FOREACH(sa_sql_struct_t& item, pre_sql) {
			if (!item.binds.empty())
				throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: source.args.arg.pre_sql has bindings")).str(APPLOCALE));
		}
	}

	iter = args.find("loop_sql");
	if (iter == args.end() || iter->second.empty())
		loop_sql.sql_str = "select '0'{char[1]} from dual";
	else
		env_mgr.expand_var(loop_sql.sql_str, iter->second);

	string tmp_sql;
	iter = args.find("exp_sql");
	if (iter == args.end() || iter->second.empty()) {
		throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: source.args.arg.exp_sql value missing")).str(APPLOCALE));
	} else {
		env_mgr.expand_var(tmp_sql,iter->second);
		if (!set_sqls(exp_sql, tmp_sql))
			throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: source.args.arg.exp_sql invalid")).str(APPLOCALE));
	}

	iter = args.find("post_sql");
	if (iter != args.end() && !iter->second.empty()) {
		env_mgr.expand_var(tmp_sql,iter->second);
		if (!set_sqls(post_sql, tmp_sql))
			throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: source.args.arg.post_sql invalid")).str(APPLOCALE));
	}

	if ((options & OPTIONS_PER_ROWS) && !post_sql.empty())
		GPP->write_log((_("WARN: Transaction consistency is not guaranteed with options=PER_ROWS and post_sql not empty")).str(APPLOCALE));

	iter = args.find("sleep_time");
	if (iter == args.end()) {
		sleep_time = DFLT_SLEEP_TIME;
	} else {
		sleep_time = atoi(iter->second.c_str());
		if (sleep_time <= 0)
			sleep_time = DFLT_SLEEP_TIME;
	}

	loop_rset = NULL;
	exp_rset = NULL;
	stmt = NULL;
	exp_idx = -1;
	rows = 0;
	real_eof = true;
}

sa_idb::~sa_idb()
{
	Generic_Database *db = SAT->_SAT_db;
	if (db == NULL)
		return;

	BOOST_FOREACH(sa_sql_struct_t& item, post_sql) {
		db->terminate_data(item.data);
		db->terminate_statement(item.stmt);
	}

	BOOST_FOREACH(sa_sql_struct_t& item, exp_sql) {
		db->terminate_data(item.data);
		db->terminate_statement(item.stmt);
	}

	db->terminate_data(loop_sql.data);
	db->terminate_statement(loop_sql.stmt);

	BOOST_FOREACH(sa_sql_struct_t& item, pre_sql) {
		db->terminate_data(item.data);
		db->terminate_statement(item.stmt);
	}
}

void sa_idb::init()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(600, __PRETTY_FUNCTION__, "", NULL);
#endif
	if (SAT->_SAT_db == NULL) {
		try {
			map<string, string> conn_info;
			database_factory& factory_mgr = database_factory::instance();
			factory_mgr.parse(SAP->_SAP_resource.openinfo, conn_info);
			SAT->_SAT_db = factory_mgr.create(SAP->_SAP_resource.dbname);
			SAT->_SAT_db->connect(conn_info);
		} catch (exception& ex) {
			delete SAT->_SAT_db;
			SAT->_SAT_db = NULL;
			throw bad_sql(__FILE__, __LINE__, SGEOS, 0, ex.what());
		}
	}
	Generic_Database *db = SAT->_SAT_db;

	BOOST_FOREACH(sa_sql_struct_t& item, pre_sql) {
		item.stmt = db->create_statement();
		item.data = db->create_data(false, 1);
		item.data->setSQL(item.sql_str);
		item.stmt->bind(item.data);
	}

	loop_sql.stmt = db->create_statement();
	loop_sql.data = db->create_data(false, adaptor.source.per_records);
	loop_sql.data->setSQL(loop_sql.sql_str);
	loop_sql.stmt->bind(loop_sql.data);

	BOOST_FOREACH(sa_sql_struct_t& item, exp_sql) {
		item.stmt = db->create_statement();
		item.data = db->create_data(false, adaptor.source.per_records);
		item.data->setSQL(item.sql_str);
		item.stmt->bind(item.data);
	}

	BOOST_FOREACH(sa_sql_struct_t& item, post_sql) {
		item.stmt = db->create_statement();
		item.data = db->create_data(false, 1);
		item.data->setSQL(item.sql_str);
		item.stmt->bind(item.data);
	}
}

void sa_idb::init2()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(600, __PRETTY_FUNCTION__, "", NULL);
#endif

	time_t current = time(0);
	if (align_time != -1) {
		int align = (current - align_time) % sleep_time;
		if (align != 0)
			current += sleep_time - align;
	}
	sysdate = current;
}

bool sa_idb::open()
{
	bool retval = false;
#if defined(DEBUG)
	scoped_debug<bool> debug(600, __PRETTY_FUNCTION__, "", &retval);
#endif

	if (real_eof) {
		time_t current = time(0);
		if (sysdate.duration() > current) {
			sleep(sysdate.duration() - current);
			if (SGP->_SGP_shutdown)
				return retval;
		}

		BOOST_FOREACH(sa_sql_struct_t& item, pre_sql) {
			item.stmt->executeUpdate();
		}

		loop_rset = loop_sql.stmt->executeQuery();
		exp_idx = -1;
		rows = 0;
		real_eof = false;
	}

	SAT->_SAT_raw_file_name = "";
	SAT->_SAT_record_type = RECORD_TYPE_BODY;
	SAT->clear();

	// insert log to database.
	try {
		dblog_manager& dblog_mgr = dblog_manager::instance(SAT);
		dblog_mgr.insert();
	} catch (bad_sql& ex) {
		real_eof = true;
		clean();
		throw;
	}

	SAT->_SAT_global[GLOBAL_SVC_KEY_SERIAL][0] = '\0';
	memcpy(SAT->_SAT_global[GLOBAL_FILE_NAME_SERIAL], SAT->_SAT_raw_file_name.c_str(), SAT->_SAT_raw_file_name.length() + 1);
	sprintf(SAT->_SAT_global[GLOBAL_FILE_SN_SERIAL], "%d", SAT->_SAT_file_sn);
	strcpy(SAT->_SAT_global[GLOBAL_REDO_MARK_SERIAL], "0000");

	string dts;
	sysdate.iso_string(dts);
	sysdate += sleep_time;
	memcpy(SAT->_SAT_global[GLOBAL_SYSDATE_SERIAL], dts.c_str(), dts.length() + 1);
	strcpy(SAT->_SAT_global[GLOBAL_FIRST_SERIAL], "0");
	strcpy(SAT->_SAT_global[GLOBAL_SECOND_SERIAL], "0");

	retval = true;
	return retval;
}

void sa_idb::flush(bool completed)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, (_("completed={1}") % completed).str(APPLOCALE), NULL);
#endif

	master.error_type = (master.record_error == 0 ? ERROR_TYPE_SUCCESS : ERROR_TYPE_PARTIAL);
	master.completed = (completed ? 2 : 0);
}

void sa_idb::close()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	if (!real_eof)
		return;

	master.error_type = ERROR_TYPE_FATAL;
	master.completed = 1;
	loop_sql.stmt->closeResultSet(loop_rset);

	if (exp_rset != NULL)
		stmt->closeResultSet(exp_rset);
}

void sa_idb::clean()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	if (!real_eof)
		return;

	master.completed = 1;
	loop_sql.stmt->closeResultSet(loop_rset);

	if (exp_rset != NULL)
		stmt->closeResultSet(exp_rset);
}

void sa_idb::recover()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(600, __PRETTY_FUNCTION__, "", NULL);
#endif

	if (!real_eof)
		return;

	loop_sql.stmt->closeResultSet(loop_rset);

	if (exp_rset != NULL)
		stmt->closeResultSet(exp_rset);
}

int sa_idb::read()
{
	int retval = SA_SUCCESS;
#if defined(DEBUG)
	scoped_debug<int> debug(600, __PRETTY_FUNCTION__, "", &retval);
#endif
	int param_idx;
	sa_sql_struct_t *exp_item;

	if (rows == per_rows) {
		// 切换文件
		retval = SA_EOF;
		return retval;
	} else if (rows == max_rows) {
		// 一个批次处理完成
		real_eof = true;
		retval = SA_EOF;
		return retval;
	}

	while (1) {
		if (exp_idx < 0) {
			if (loop_rset->next() == Generic_ResultSet::END_OF_FETCH) {
				// 一个批次处理完成
				real_eof = true;
				retval = SA_EOF;
				// 报告正常退出
				schd_rpc& rpc_mgr = schd_rpc::instance();
				rpc_mgr.report_exit();
				SGP->_SGP_shutdown++;
				return retval;
			}

			exp_item = &exp_sql[0];
			stmt = exp_item->stmt;
			param_idx = 1;
			BOOST_FOREACH(const int& idx, exp_item->binds) {
				stmt->setString(param_idx++, loop_rset->getString(idx));
			}

			exp_rset = stmt->executeQuery();

			BOOST_SCOPE_EXIT((&exp_idx)) {
				exp_idx = 0;
			} BOOST_SCOPE_EXIT_END

			if ((options & OPTIONS_PER_LOOP) && exp_idx == -2) {
				// 切换文件
				retval = SA_EOF;
				return retval;
			}
		}

		if (exp_rset->next() == Generic_ResultSet::END_OF_FETCH) {
			stmt->closeResultSet(exp_rset);

			if (++exp_idx == exp_sql.size()) {
				// 继续下一个循环
				exp_idx = -2;

				BOOST_FOREACH(sa_sql_struct_t& item, post_sql) {
					param_idx = 1;
					BOOST_FOREACH(const int& idx, item.binds) {
						item.stmt->setString(param_idx++, loop_rset->getString(idx));
					}

					item.stmt->executeUpdate();
				}

				continue;
			}

			exp_item = &exp_sql[exp_idx];
			stmt = exp_item->stmt;
			param_idx = 1;
			BOOST_FOREACH(const int& idx, exp_item->binds) {
				stmt->setString(param_idx++, loop_rset->getString(idx));
			}

			exp_rset = stmt->executeQuery();
		}

		++rows;
		if (!to_fields(adaptor.input_records[0], exp_rset)) {
			retval = SA_ERROR;
			return retval;
		}

		return retval;
	}
}

bool sa_idb::to_fields(const input_record_t& input_record, Generic_ResultSet *rset)
{
	const vector<input_field_t>& field_vector = input_record.field_vector;
	char **input = sa.get_input();
	int i;

	SAT->_SAT_error_pos = -1;
	for (i = FIXED_INPUT_SIZE; i < field_vector.size(); i++) {
		const input_field_t& field = field_vector[i];
		string value = rset->getString(i - FIXED_INPUT_SIZE + 1);

		if (value.length() <= field.factor2) { // In range of max keep length
			memcpy(input[i], value.c_str(), value.length() + 1);
		} else if (value.length() <= field.factor1) { // In range of max allow length
			memcpy(input[i], value.c_str(), field.factor2);
			input[i][field.factor2] = '\0';
		} else {
			memcpy(input[i], value.c_str(), field.factor2);
			input[i][field.factor2] = '\0';
			if (SAT->_SAT_error_pos == -1)
				SAT->_SAT_error_pos = i - FIXED_INPUT_SIZE;
		}
	}

	dout << "In " << __PRETTY_FUNCTION__ << ", field size = " << field_vector.size() << "\n";
	for (i = 0; i < field_vector.size(); i++) {
		const input_field_t& field = field_vector[i];
		dout << field.field_name << " = [" << input[i] << "] length = " << strlen(input[i]) << "\n";
	}
	dout << std::flush;

	return (SAT->_SAT_error_pos == -1);
}

bool sa_idb::set_sqls(vector<sa_sql_struct_t>& sqls, const string& value)
{
	bool retval = false;
#if defined(DEBUG)
	scoped_debug<bool> debug(600, __PRETTY_FUNCTION__, (_("value={1}") % value).str(APPLOCALE), &retval);
#endif
	sa_sql_struct_t item;
	string& sql_str = item.sql_str;

	for (int i = 0; i < value.size();) {
		char ch = value[i];

		if (ch == '\'') {
			sql_str += ch;
			for (i++; i < value.size(); i++) {
				ch = value[i];
				sql_str += ch;
				if (ch == '\'')
					break;
			}
		} else if (ch == '\"') {
			sql_str += ch;
			for (i++; i < value.size(); i++) {
				ch = value[i];
				sql_str += ch;
				if (ch == '\"')
					break;
			}
		} else if (ch == ';') {
			sqls.push_back(item);
			if (!set_binds(*sqls.rbegin()))
				return retval;
			sql_str.clear();
		} else if (!isspace(ch) || !sql_str.empty()) {
			sql_str += ch;
		}

		i++;
	}

	if (!sql_str.empty()) {
		sqls.push_back(item);
		if (!set_binds(*sqls.rbegin()))
			return retval;
	}

	retval = true;
	return retval;
}

bool sa_idb::set_binds(sa_sql_struct_t& sql)
{
	bool retval = false;
#if defined(DEBUG)
	scoped_debug<bool> debug(600, __PRETTY_FUNCTION__, "", &retval);
#endif
	string& sql_str = sql.sql_str;
	vector<int>& binds = sql.binds;

	for (int i = 0; i < sql_str.size();) {
		char ch = sql_str[i];

		if (ch == '\'') {
			for (i++; i < sql_str.size(); i++) {
				ch = sql_str[i];
				if (ch == '\'')
					break;
			}
		} else if (ch == '\"') {
			for (i++; i < sql_str.size(); i++) {
				ch = sql_str[i];
				if (ch == '\"')
					break;
			}
		} else if (ch == ':') {
			i++;
			if (i >= sql_str.size())
				return retval;

			ch = sql_str[i];
			if (ch != 'v' && ch != 'V')
				return retval;

			string key;
			for (i++; i < sql_str.size(); i++) {
				ch = sql_str[i];
				if (!isalnum(ch) && ch != '_')
					break;
				key += ch;
			}

			try {
				binds.push_back(boost::lexical_cast<int>(key));
			} catch (exception& ex) {
				return retval;
			}
		}

		i++;
	}

	retval = true;
	return retval;
}

DECLARE_SA_INPUT(0, sa_idb)

}
}

