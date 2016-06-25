#include "sa_internal.h"

namespace ai
{
namespace app
{

using namespace ai::sg;
using namespace ai::scci;
typedef boost::tokenizer<boost::char_separator<char> > tokenizer;

sa_odbtarget::sa_odbtarget(sa_base& _sa, int _flags)
	: sa_output(_sa, _flags)
{
	const map<string, string> *args;

	switch (flags) {
	case OTYPE_TARGET:
		args = &adaptor.target.args;
		table_name = "TGT_";
		break;
	default:
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: unsupported flags {1}") % flags).str(APPLOCALE));
	}

	map<string, string>::const_iterator iter = args->find("table");
	if (iter != args->end()) {
		table_name = iter->second;
	} else {
		table_name += adaptor.parms.com_key;
		table_name += "_";
		table_name += adaptor.parms.svc_key;
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

	stmt = NULL;
	data = NULL;
	del_stmt = NULL;
	del_data = NULL;
	update_count = 0;
	rollback_called = false;

	create_stmt();
}

sa_odbtarget::~sa_odbtarget()
{
	Generic_Database *db = SAT->_SAT_db;
	if (db == NULL)
		return;

	db->terminate_data(del_data);
	db->terminate_statement(del_stmt);
	db->terminate_data(data);
	db->terminate_statement(stmt);
}

int sa_odbtarget::write(int input_idx)
{
	int retval = 0;
#if defined(DEBUG)
	scoped_debug<int> debug(100, __PRETTY_FUNCTION__, (_("input_idx={1}") % input_idx).str(APPLOCALE), &retval);
#endif
	const char **output = const_cast<const char **>(sa.get_output());

	const vector<output_field_t>& output_fields = adaptor.output_fields;
	int size = output_fields.size();

	try {
		if (update_count)
			stmt->addIteration();

		stmt->setInt(1, SAT->_SAT_file_sn);
		for (int i = 0; i < size; i++)
			stmt->setString(i + 2, output[i]);

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

void sa_odbtarget::flush(bool completed)
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

void sa_odbtarget::close()
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

void sa_odbtarget::clean()
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

	if (del_stmt == NULL)
		return;

	try {
		del_stmt->setInt(1, SAT->_SAT_file_sn);
		del_stmt->executeUpdate();
	}  catch (exception& ex) {
		api_mgr.write_log(__FILE__, __LINE__, (_("ERROR: failed to delete record from table {1}") % table_name).str(APPLOCALE));
		del_stmt->resetIteration();
		throw;
	}
}

void sa_odbtarget::recover()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(100, __PRETTY_FUNCTION__, "", NULL);
#endif

	if (update_count) {
		stmt->resetIteration();
		update_count = 0;
	}
}

void sa_odbtarget::dump(ostream& os)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(100, __PRETTY_FUNCTION__, "", NULL);
#endif
	const vector<output_field_t>& output_fields = adaptor.output_fields;

	os << "create table "
		<< table_name
		<< "\n(\n"
		<< "\tfile_sn number(10)";

	BOOST_FOREACH(const output_field_t& field, output_fields) {
		os << ",\n\t"
			<< field.column_name
			<< " varchar2("
			<< field.field_size
			<< ")";
	}

	os << "\n);\n\n";

	os << "create index idx_" << table_name << " on " << table_name << "(file_sn);\n\n";
}

void sa_odbtarget::create_stmt()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(100, __PRETTY_FUNCTION__, "", NULL);
#endif
	const vector<output_field_t>& output_fields = adaptor.output_fields;

	string sql_str = "insert into ";
	sql_str += table_name;
	sql_str += "(file_sn";

	BOOST_FOREACH(const output_field_t& field, output_fields) {
		sql_str += ", ";
		sql_str += field.column_name;
	}

	sql_str += ") values(:file_sn{int}";

	BOOST_FOREACH(const output_field_t& field, output_fields) {
		sql_str += ", ";
		sql_str += ":";
		sql_str += field.column_name;
		sql_str += "{char[";
		sql_str += boost::lexical_cast<string>(field.field_size);
		sql_str += "]}";
	}

	sql_str += ")";

	string del_str = "delete from ";
	del_str += table_name;
	del_str += " where file_sn = :file_sn{int}";

	dout << __PRETTY_FUNCTION__ << ": sql_str = [" << sql_str << "]\n";
	dout << __PRETTY_FUNCTION__ << ": del_str = [" << del_str << "]\n";

	try {
		Generic_Database *db = SAT->_SAT_db;

		stmt = db->create_statement();
		data = db->create_data(false, per_records);
		del_stmt = db->create_statement();
		del_data = db->create_data(false, 1);

		data->setSQL(sql_str);
		stmt->bind(data);
		del_data->setSQL(del_str);
		del_stmt->bind(del_data);
	} catch (exception& ex) {
		api_mgr.write_log(__FILE__, __LINE__, (_("ERROR: failed to parse statement {1}") % sql_str).str(APPLOCALE));
		throw;
	}
}

void sa_odbtarget::rollback(const string& raw_file_name, int file_sn)
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
			throw bad_msg(__FILE__, __LINE__, 0,(_("ERROR: failed to execute rollback script {1}") % rollback_script).str(APPLOCALE));

		return;
	}

	string sql_str;

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
		Generic_Database *db = SAT->_SAT_db;

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
		api_mgr.write_log(__FILE__, __LINE__, (_("ERROR: failed to parse statement {1}") % sql_str).str(APPLOCALE));
		throw;
	}
}

DECLARE_SA_OUTPUT(OTYPE_TARGET, sa_odbtarget)

}
}

