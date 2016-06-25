#include "sa_internal.h"

namespace ai
{
namespace app
{

using namespace ai::sg;
using namespace ai::scci;
typedef boost::tokenizer<boost::char_separator<char> > tokenizer;

sa_odbsource::sa_odbsource(sa_base& _sa, int _flags)
	: sa_output(_sa, _flags)
{
	const map<string, string> *args;

	switch (flags) {
	case OTYPE_INVALID:
		args = &adaptor.invalid.args;
		table_prefix = "INV_";
		break;
	case OTYPE_ERROR:
		args = &adaptor.error.args;
		table_prefix = "ERR_";
		break;
	case OTYPE_TARGET:
		args = &adaptor.target.args;
		table_prefix = "TGT_";
		break;
	default:
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: unsupported flags {1}") % flags).str(APPLOCALE));
	}

	map<string, string>::const_iterator iter = args->find("tables");
	if (iter != args->end()) {
		boost::char_separator<char> sep(" \t\b");
		tokenizer tokens(iter->second, sep);
		for (tokenizer::iterator iter = tokens.begin(); iter != tokens.end(); ++iter)
			tables.push_back(*iter);
	}

	iter = args->find("per_records");
	if (iter != args->end())
		per_records = boost::lexical_cast<int>(iter->second);
	else
		per_records = DFLT_PER_RECORDS;

	iter = args->find("rollback");
	if (iter != args->end()) {
		if (strcasecmp(iter->second.c_str(), "NONE") == 0)
			rollback_type = ROLLBACK_BY_NONE;
		else if (strcasecmp(iter->second.c_str(), "TRUNCATE") == 0)
			rollback_type = ROLLBACK_BY_TRUNCATE;
		else if (strcasecmp(iter->second.c_str(), "DELETE") == 0)
			rollback_type = ROLLBACK_BY_DELETE;
		else if (strcasecmp(iter->second.c_str(), "SCRIPT") == 0)
			rollback_type = ROLLBACK_BY_SCRIPT;
		else
			throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: rollback invalid")).str(APPLOCALE));
	} else {
		rollback_type = ROLLBACK_BY_DELETE;
	}

	if (rollback_type == ROLLBACK_BY_SCRIPT) {
		iter = args->find("rollback_script");
		if (iter == args->end())
			throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: rollback_script missing")).str(APPLOCALE));

		rollback_script = iter->second;
	}

	table_prefix += adaptor.parms.com_key;
	table_prefix += "_";
	table_prefix += adaptor.parms.svc_key;

 	items.resize(adaptor.input_records.size());
	stmt = NULL;
	record_serial = -1;
	update_count = 0;
	rollback_called = false;
}

sa_odbsource::~sa_odbsource()
{
	Generic_Database *db = SAT->_SAT_db;
	if (db == NULL)
		return;

	BOOST_FOREACH(db_item_t& item, items) {
		db->terminate_data(item.del_data);
		db->terminate_statement(item.del_stmt);
		db->terminate_data(item.data);
		db->terminate_statement(item.stmt);
	}
}

int sa_odbsource::write(int input_idx)
{
	int retval = 0;
#if defined(DEBUG)
	scoped_debug<int> debug(100, __PRETTY_FUNCTION__, (_("input_idx={1}") % input_idx).str(APPLOCALE), &retval);
#endif
	const int& status = sa.get_status();
	const char **global = const_cast<const char **>(sa.get_global());
	const char **input = const_cast<const char **>(sa.get_input(input_idx));

	bool format_error = false;
	const vector<input_record_t>& input_records = adaptor.input_records;
	int& input_serial = sa.get_input_serial(input_idx);
	if (input_serial < 0 || input_serial >= input_records.size()) {
		input_serial = 0;
		format_error = true;
	}

	const vector<input_field_t>& field_vector = input_records[input_serial].field_vector;
	int size = field_vector.size();

	try {
		if (record_serial != input_serial) { // Switch to another table.
			if (update_count) { // It may not update if this is the first time of dealing a file.
				stmt->executeUpdate();
				update_count = 0;
			}

			record_serial = input_serial;
			if (items[record_serial].stmt == NULL)
				create_stmt();

			stmt = items[record_serial].stmt;
		} else if (update_count) {	// Insert a record into the same table.
			stmt->addIteration();
		}

		stmt->setString(1, SAT->_SAT_raw_file_name);
		stmt->setInt(2, SAT->_SAT_file_sn);
		stmt->setInt(3, status / 1000);
		stmt->setInt(4, status % 1000);
		if (format_error) {
			for (int i = 0; i < size; i++)
				stmt->setString(i + 5, "");
		} else {
			for (int i = 0; i < size; i++)
				stmt->setString(i + 5, input[i]);
		}
		stmt->setString(size + 5, global[GLOBAL_REDO_MARK_SERIAL]);
		if (sa.get_id() == 0)
			stmt->setString(size + 6, SAT->_SAT_rstrs[input_idx]);
		else
			stmt->setString(size + 6, "");
		stmt->setInt(size + 7, record_serial);

		if (++update_count == per_records) {
			stmt->executeUpdate();
			update_count = 0;
		}
	} catch (exception& ex) {
		api_mgr.write_log(__FILE__, __LINE__, (_("ERROR: failed to insert record into table {1}") % table_name).str(APPLOCALE));
		stmt->resetIteration();
		update_count = 0;
		throw;
	}

	retval++;
	return retval;
}

void sa_odbsource::flush(bool completed)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(100, __PRETTY_FUNCTION__, (_("completed={1}") % completed).str(APPLOCALE), NULL);
#endif

	if (update_count) {
		try {
			stmt->executeUpdate();
			update_count = 0;
		} catch (exception& ex) {
			api_mgr.write_log(__FILE__, __LINE__, (_("ERROR: failed to insert/update record into table {1}") % table_name).str(APPLOCALE));
			stmt->resetIteration();
			update_count = 0;
			throw;
		}
	}
}

void sa_odbsource::close()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(100, __PRETTY_FUNCTION__, "", NULL);
#endif

	if (update_count) {
		try {
			stmt->executeUpdate();
			update_count = 0;
		} catch (exception& ex) {
			api_mgr.write_log(__FILE__, __LINE__, (_("ERROR: failed to insert/update record into table {1}") % table_name).str(APPLOCALE));
			stmt->resetIteration();
			update_count = 0;
			throw;
		}
	}
}

void sa_odbsource::clean()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(100, __PRETTY_FUNCTION__, "", NULL);
#endif

	if (update_count) {
		try {
			stmt->executeUpdate();
			update_count = 0;
		} catch (exception& ex) {
			api_mgr.write_log(__FILE__, __LINE__, (_("ERROR: failed to insert/update record into table {1}") % table_name).str(APPLOCALE));
			stmt->resetIteration();
			update_count = 0;
			throw;
		}
	}

	Generic_Statement *del_stmt = NULL;
	try {
		BOOST_FOREACH(db_item_t& item, items) {
			del_stmt = item.del_stmt;

			if (del_stmt == NULL)
				continue;

			del_stmt->setInt(1, SAT->_SAT_file_sn);
			del_stmt->executeUpdate();
		}
	} catch (exception& ex) {
		api_mgr.write_log(__FILE__, __LINE__, (_("ERROR: failed to delete record from table {1}") % table_name).str(APPLOCALE));
		del_stmt->resetIteration();
		throw;
	}
}

void sa_odbsource::recover()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(100, __PRETTY_FUNCTION__, "", NULL);
#endif

	if (update_count) {
		stmt->resetIteration();
		update_count = 0;
	}
}

void sa_odbsource::dump(ostream& os)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(100, __PRETTY_FUNCTION__, "", NULL);
#endif
	const vector<input_record_t>& input_records = adaptor.input_records;

	for (int i = 0; i < input_records.size(); i++) {
		const vector<input_field_t>& field_vector = input_records[i].field_vector;

		if (i < tables.size()) {
			table_name = tables[i];
		} else {
			table_name = table_prefix;
			// If there are more than one source record type, we need more than one error table,
			// so error table name is attached with record_serial from 0 on.
			if (input_records.size() > 1) {
				table_name += "_";
				table_name += boost::lexical_cast<string>(i);
			}
		}

		os << "create table "
			<< table_name;

		os << "\n(\n"
			<< "\tfile_name varchar2(64) not null,\n"
			<< "\tfile_sn number(10) not null,\n"
			<< "\terror_code number(6) not null,\n"
			<< "\terror_pos number(3) not null,\n";

		BOOST_FOREACH(const input_field_t& field, field_vector) {
			os << '\t'
				<< field.column_name
				<< " varchar2(";
			if (field.encode_type == ENCODE_ASCII)
				os << field.factor2;
			else
				os << (field.factor2 * 2);
			os << "),\n";
		}

		os << "\tredo_mark varchar2(4) not null,\n"
			<< "\tsrc_record varchar2(2000),\n"
			<< "\trecord_serial number(1) not null,\n"
			<< "\tredo_flag number(1) not null\n"
			<< ");\n\n";

		os << "create index idx_" << table_name << " on " << table_name << "(file_sn);\n\n";
	}
}

void sa_odbsource::create_stmt()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(100, __PRETTY_FUNCTION__, "", NULL);
#endif
	const vector<input_record_t>& input_records = adaptor.input_records;
	const vector<input_field_t>& field_vector = input_records[record_serial].field_vector;

	if (record_serial < tables.size()) {
		table_name = tables[record_serial];
	} else {
		table_name = table_prefix;
		// If there are more than one source record type, we need more than one error table,
		// so error table name is attached with record_serial from 0 on.
		if (input_records.size() > 1) {
			table_name += "_";
			table_name += boost::lexical_cast<string>(record_serial);
		}
	}

	string sql_str = "insert into ";
	sql_str += table_name;
	sql_str += "(file_name, file_sn, error_code, error_pos, ";

	BOOST_FOREACH(const input_field_t& field, field_vector) {
		sql_str += field.column_name;
		sql_str += ", ";
	}

	sql_str += "redo_mark, src_record, record_serial, redo_flag) values(:file_name{char[";
	sql_str += boost::lexical_cast<string>(RAW_FILE_NAME_LEN);
	sql_str += "]}, :file_sn{int}, :error_code{int}, :error_pos{int}, ";

	BOOST_FOREACH(const input_field_t& field, field_vector) {
		sql_str += ":";
		sql_str += field.column_name;
		sql_str += "{char[";
		if (field.encode_type == ENCODE_ASCII)
			sql_str += boost::lexical_cast<string>(field.factor2);
		else
			sql_str += boost::lexical_cast<string>(field.factor2 * 2);
		sql_str += "]}, ";
	}

	sql_str += ":redo_mark{int}, :src_record{char[";
	sql_str += boost::lexical_cast<string>(MAX_RECORD_LEN);
	sql_str += "]}, :record_serial{int}, 0)";

	string del_str = "delete from ";
	del_str += table_name;
	del_str += " where file_sn = :file_sn{int}";

	dout << __PRETTY_FUNCTION__ << ": sql_str = [" << sql_str << "]\n";
	dout << __PRETTY_FUNCTION__ << ": del_str = [" << del_str << "]\n";

	Generic_Database *db = SAT->_SAT_db;
	db_item_t& item = items[record_serial];

	try {
		item.stmt = db->create_statement();
		item.data = db->create_data(false, per_records);
		item.del_stmt = db->create_statement();
		item.del_data = db->create_data(false, 1);

		item.data->setSQL(sql_str);
		item.stmt->bind(item.data);
		item.del_data->setSQL(del_str);
		item.del_stmt->bind(item.del_data);
	} catch (exception& ex) {
		db->terminate_data(item.del_data);
		db->terminate_statement(item.del_stmt);
		db->terminate_data(item.data);
		db->terminate_statement(item.stmt);

		api_mgr.write_log(__FILE__, __LINE__, (_("ERROR: failed to parse statement {1}") % sql_str).str(APPLOCALE));
		throw;
	}
}

void sa_odbsource::rollback(const string& raw_file_name, int file_sn)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(100, __PRETTY_FUNCTION__, (_("raw_file_name={1}, file_sn={2}") % raw_file_name % file_sn).str(APPLOCALE), NULL);
#endif

	if (rollback_called && rollback_type != ROLLBACK_BY_DELETE)
		return;

	rollback_called = true;

	if (rollback_type == ROLLBACK_BY_NONE)
		return;

	if (rollback_type == ROLLBACK_BY_SCRIPT) {
		sys_func& func_mgr = sys_func::instance();
		int retval = func_mgr.new_task(rollback_script.c_str(), NULL, 0);
		if (retval)
			throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: failed to execute rollback script {1}") % rollback_script).str(APPLOCALE));

		return;
	}

	const vector<input_record_t>& input_records = adaptor.input_records;
	for (int i = 0; i < input_records.size(); i++) {
		if (i < tables.size()) {
			table_name = tables[i];
		} else {
			table_name = table_prefix;
			// If there are more than one source record type, we need more than one error table,
			// so error table name is attached with record_serial from 0 on.
			if (input_records.size() > 1) {
				table_name += "_";
				table_name += boost::lexical_cast<string>(i);
			}
		}

		string sql_str;
		Generic_Database *db = SAT->_SAT_db;

		if (rollback_type == ROLLBACK_BY_DELETE) {
			sql_str = "delete from ";
			sql_str += table_name;
			sql_str += " where file_sn = :file_sn{int}";
		} else {
			sql_str = "truncate table ";
			sql_str += table_name;
		}

		dout << __PRETTY_FUNCTION__ << ": sql_str = [" << sql_str << "]\n";

		try {
			auto_ptr<Generic_Statement> auto_stmt(db->create_statement());
			Generic_Statement *stmt = auto_stmt.get();

			auto_ptr<struct_dynamic> auto_data(db->create_data(false, 1));
			struct_dynamic *data = auto_data.get();

			data->setSQL(sql_str);
			stmt->bind(data);

			if (rollback_type == ROLLBACK_BY_DELETE)
				stmt->setInt(1, file_sn);

			stmt->executeUpdate();
		} catch (exception& ex) {
			api_mgr.write_log(__FILE__, __LINE__, (_("ERROR: failed to create/execute statement {1}") % sql_str).str(APPLOCALE));
			throw;
		}
	}
}

DECLARE_SA_OUTPUT(OTYPE_INVALID | OTYPE_ERROR | OTYPE_TARGET, sa_odbsource)

}
}

