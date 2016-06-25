#include "sa_internal.h"

namespace ai
{
namespace app
{

using namespace std;
using namespace ai::sg;
using namespace ai::scci;

sa_osummary::sa_osummary(sa_base& _sa, int _flags)
	: sa_output(_sa, _flags)
{
	switch (flags) {
	case OTYPE_SUMMARY:
		break;
	default:
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: unsupported flags {1}") % flags).str(APPLOCALE));
	}

	rollback_called = false;
}

sa_osummary::~sa_osummary()
{
	Generic_Database *db = SAT->_SAT_db;
	if (db == NULL)
		return;

	BOOST_FOREACH(osummary_item_t& item, items) {
		db->terminate_data(item.delete_data);
		db->terminate_statement(item.delete_stmt);
		db->terminate_data(item.update_data);
		db->terminate_statement(item.update_stmt);
		db->terminate_data(item.insert_data);
		db->terminate_statement(item.insert_stmt);
	}
}

void sa_osummary::flush(bool completed)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, (_("completed={1}") % completed).str(APPLOCALE), NULL);
#endif

	execute_update();
}

void sa_osummary::close()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	execute_update();
}

void sa_osummary::clean()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	for (int i = 0; i < items.size(); i++) {
		osummary_item_t& item = items[i];
		Generic_Statement *& delete_stmt = item.delete_stmt;
		if (delete_stmt == NULL)
			continue;

		const sa_summary_record_t& record = adaptor.summary.records[i];
		const char **global = const_cast<const char **>(sa.get_global());
		if (record.del_flag & DEL_BY_FILE_SN)
			delete_stmt->setString(1, global[GLOBAL_FILE_SN_SERIAL]);
		else
			delete_stmt->setString(1, global[GLOBAL_FILE_NAME_SERIAL]);

		delete_stmt->executeUpdate();
	}
}

void sa_osummary::recover()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	for (int idx = 0; idx < items.size(); idx++) {
		osummary_item_t& item = items[idx];

		Generic_Statement *& insert_stmt = item.insert_stmt;
		insert_stmt->resetIteration();

		Generic_Statement *& update_stmt = item.update_stmt;
		update_stmt->resetIteration();

		Generic_Statement *& delete_stmt = item.delete_stmt;
		delete_stmt->resetIteration();
	}
}

void sa_osummary::dump(ostream& os)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(100, __PRETTY_FUNCTION__, "", NULL);
#endif
	const vector<sa_summary_record_t>& records = adaptor.summary.records;

	for (int i = 0; i < records.size(); i++) {
		const sa_summary_record_t& record = records[i];
		string keys;

		if (record.del_flag & DEL_BY_FILE_SN) {
			if (!keys.empty())
				keys += ", ";
			keys = "file_sn";
		} else if (record.del_flag & DEL_BY_FILE_NAME) {
			if (!keys.empty())
				keys += ", ";
			keys = "file_name";
		}

		os << "create table " << record.table_name
			<< "\n(\n";

		bool first = true;
		BOOST_FOREACH(const sa_summary_field_t& field, record.keys) {
			if (first)
				first = false;
			else
				os << ",\n";

			os << '\t'
				<< field.column_name
				<< " ";

			switch (field.field_type) {
			case FIELD_TYPE_INT:
			case FIELD_TYPE_LONG:
				os << "number(" << field.field_size
					<< ")";
				break;
			case FIELD_TYPE_FLOAT:
			case FIELD_TYPE_DOUBLE:
				os << "number";
				break;
			case FIELD_TYPE_STRING:
				os << "varchar2("
					<< field.field_size
					<< ")";
			default:
				break;
			}

			if (!((field.column_name == "file_sn" && (record.del_flag & DEL_BY_FILE_SN))
				|| (field.column_name == "file_name" && (record.del_flag & DEL_BY_FILE_NAME)))) {
				if (!keys.empty())
					keys += ", ";
				keys = field.column_name;
			}
		}

		BOOST_FOREACH(const sa_summary_field_t& field, record.values) {
			if (first)
				first = false;
			else
				os << ",\n";

			os << '\t'
				<< field.column_name
				<< " ";

			switch (field.field_type) {
			case FIELD_TYPE_INT:
			case FIELD_TYPE_LONG:
				os << "number(" << field.field_size
					<< ")";
				break;
			case FIELD_TYPE_FLOAT:
			case FIELD_TYPE_DOUBLE:
				os << "number";
				break;
			case FIELD_TYPE_STRING:
				os << "varchar2("
					<< field.field_size
					<< ")";
			default:
				break;
			}
		}

		os << "\n);\n\n";

		os << "alter table " << record.table_name << "\n"
			<< "\tadd constraint pk_" << record.table_name
			<< " primary key (" << keys << ");\n\n";
	}
}

void sa_osummary::rollback(const string& raw_file_name, int file_sn)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(100, __PRETTY_FUNCTION__, (_("raw_file_name={1}, file_sn={2}") % raw_file_name % file_sn).str(APPLOCALE), NULL);
#endif

	const vector<sa_summary_record_t>& records = adaptor.summary.records;
	string sql_str;

	for (int i = 0; i < records.size(); i++) {
		const sa_summary_record_t& record = records[i];

		if (rollback_called && record.rollback_type != ROLLBACK_BY_DELETE)
			return;

		rollback_called = true;

		switch (record.rollback_type) {
		case ROLLBACK_BY_NONE:
			break;
		case ROLLBACK_BY_SCRIPT:
			{
				sys_func& func_mgr = sys_func::instance();
				int retval = func_mgr.new_task(record.rollback_script.c_str(), NULL, 0);
				if (retval)
					throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: failed to execute rollback script {1}") % record.rollback_script).str(APPLOCALE));
			}
			break;
		default:
			try {
				if (record.rollback_type == ROLLBACK_BY_DELETE) {
					if (!(record.del_flag & (DEL_BY_FILE_SN | DEL_BY_FILE_NAME)))
						continue;

					sql_str = "delete from ";
					sql_str += record.table_name;
					sql_str += " where ";
					if (record.del_flag & DEL_BY_FILE_SN) {
						sql_str += "file_sn = :file_sn{int}";
					} else {
						sql_str += "file_name = :file_name{char[";
						sql_str += boost::lexical_cast<string>(RAW_FILE_NAME_LEN);
						sql_str += "]}";
					}
				} else {
					sql_str = "truncate table ";
					sql_str += record.table_name;
				}

				dout << __PRETTY_FUNCTION__ << ": sql_str = [" << sql_str << "]\n";

				Generic_Database *db = SAT->_SAT_db;
				auto_ptr<Generic_Statement> auto_stmt(db->create_statement());
				Generic_Statement *stmt = auto_stmt.get();

				auto_ptr<struct_dynamic> auto_data(db->create_data(false, 1));
				struct_dynamic *data = auto_data.get();

				data->setSQL(sql_str);
				stmt->bind(data);

				if (record.rollback_type == ROLLBACK_BY_DELETE) {
					if (record.del_flag & DEL_BY_FILE_SN)
						stmt->setInt(1, file_sn);
					else
						stmt->setString(1, raw_file_name);
				}

				stmt->executeUpdate();
			} catch (exception& ex) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: failed to parse statement {1}") % sql_str).str(APPLOCALE));
				throw;
			}
		}
	}
}

void sa_osummary::create_stmt(int idx)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, (_("idx={1}") % idx).str(APPLOCALE), NULL);
#endif
	Generic_Database *db = SAT->_SAT_db;
	const sa_summary_record_t& record = adaptor.summary.records[idx];
	osummary_item_t& item = items[idx];

	try {
		Generic_Statement *& insert_stmt = item.insert_stmt;
		struct_dynamic *& insert_data = item.insert_data;
		Generic_Statement *& update_stmt = item.update_stmt;
		struct_dynamic *& update_data = item.update_data;
		Generic_Statement *& delete_stmt = item.delete_stmt;
		struct_dynamic *& delete_data = item.delete_data;

		if (insert_stmt == NULL) {
			insert_stmt = db->create_statement();
			insert_data = db->create_data(false, 1);
		}

		if (update_stmt == NULL) {
			update_stmt = db->create_statement();
			update_data = db->create_data(false, 1);
		}

		if (delete_stmt == NULL) {
			delete_stmt = db->create_statement();
			delete_data = db->create_data(false, 1);
		}

		string select_clause;
		string insert_clause;
		bool first = true;

		string update_str = "update ";
		update_str += record.table_name;
		update_str += " set ";

		BOOST_FOREACH(const sa_summary_field_t& field, record.values) {
			string bind_str = get_binder(field);

			if (!select_clause.empty())
				select_clause += ", ";
			if (!insert_clause.empty())
				insert_clause += ", ";

			select_clause += field.column_name;
			insert_clause += bind_str;

			if (first)
				first = false;
			else
				update_str += ", ";

			update_str += field.column_name;
			switch (field.action) {
			case SUMMARY_ACTION_ASSIGN:
				update_str += " = ";
				update_str += bind_str;
				break;
			case SUMMARY_ACTION_MIN:
				update_str += " = min(";
				update_str += field.column_name;
				update_str += ", ";
				update_str += bind_str;
				update_str += ")";
				break;
			case SUMMARY_ACTION_MAX:
				update_str += " = max(";
				update_str += field.column_name;
				update_str += ", ";
				update_str += bind_str;
				update_str += ")";
				break;
			case SUMMARY_ACTION_ADD:
				update_str += " = ";
				update_str += field.column_name;
				update_str += " + ";
				update_str += bind_str;
				break;
			default:
				break;
			}
		}

		update_str += " where ";
		first = true;

		BOOST_FOREACH(const sa_summary_field_t& field, record.keys) {
			string bind_str = get_binder(field);

			if (!select_clause.empty())
				select_clause += ", ";
			if (!insert_clause.empty())
				insert_clause += ", ";

			select_clause += field.column_name;
			insert_clause += bind_str;

			if (first)
				first = false;
			else
				update_str += "and ";

			update_str += field.column_name;
			update_str += " = ";
			update_str += bind_str;
		}

		string insert_str = "insert into ";
		insert_str += record.table_name;
		insert_str += "(";
		insert_str += select_clause;
		insert_str += ") values(";
		insert_str += insert_clause;
		insert_str += ")";

		insert_data->setSQL(insert_str);
		insert_stmt->bind(insert_data);
		update_data->setSQL(update_str);
		update_stmt->bind(update_data);

		dout << __PRETTY_FUNCTION__ << ": sql_insert = [" << insert_str << "]\n";
		dout << __PRETTY_FUNCTION__ << ": sql_update = [" << update_str << "]\n";

		if ((record.del_flag & (DEL_BY_FILE_SN | DEL_BY_FILE_NAME))) {
			string delete_str = "delete from ";
			delete_str += record.table_name;
			delete_str += " where ";
			if (record.del_flag & DEL_BY_FILE_SN) {
				delete_str += "file_sn = :file_sn{int}";
			} else {
				delete_str += "file_name = :file_name{char[";
				delete_str += boost::lexical_cast<string>(RAW_FILE_NAME_LEN);
				delete_str += "]}";
			}

			delete_data->setSQL(delete_str);
			delete_stmt->bind(delete_data);

			dout << __PRETTY_FUNCTION__ << ": sql_delete = [" << delete_str << "]\n";
		}
	} catch (bad_sql& ex) {
		throw bad_msg(__FILE__, __LINE__, 0, ex.what());
	}
}

void sa_osummary::set_data(int idx)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, (_("idx={1}") % idx).str(APPLOCALE), NULL);
#endif
	const sa_summary_record_t& record = adaptor.summary.records[idx];
	osummary_item_t& item = items[idx];
	const char **input = const_cast<const char **>(sa.get_input());
	const char **global = const_cast<const char **>(sa.get_global());
	const char **output = const_cast<const char **>(sa.get_output());

	Generic_Statement *& insert_stmt = item.insert_stmt;
	int param_idx = 1;

	BOOST_FOREACH(const sa_summary_field_t& field, record.values) {
		switch (field.field_orign) {
		case FIELD_ORIGN_INPUT:
			insert_stmt->setString(param_idx, input[field.field_serial]);
			break;
		case FIELD_ORIGN_GLOBAL:
			insert_stmt->setString(param_idx, global[field.field_serial]);
			break;
		case FIELD_ORIGN_OUTPUT:
			insert_stmt->setString(param_idx, output[field.field_serial]);
			break;
		default:
			throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: unknown field orign {1} for field_serial {2}") % field.field_orign % field.field_serial).str(APPLOCALE));
		}
		param_idx++;
	}

	BOOST_FOREACH(const sa_summary_field_t& field, record.keys) {
		switch (field.field_orign) {
		case FIELD_ORIGN_INPUT:
			insert_stmt->setString(param_idx, input[field.field_serial]);
			break;
		case FIELD_ORIGN_GLOBAL:
			insert_stmt->setString(param_idx, global[field.field_serial]);
			break;
		case FIELD_ORIGN_OUTPUT:
			insert_stmt->setString(param_idx, output[field.field_serial]);
			break;
		default:
			throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: unknown field orign {1} for field_serial {2}") % field.field_orign % field.field_serial).str(APPLOCALE));
		}
		param_idx++;
	}

	try {
		insert_stmt->executeUpdate();
	} catch (bad_sql& ex) {
		// 通常是主键冲突
		Generic_Statement *& update_stmt = item.update_stmt;
		param_idx = 1;

		BOOST_FOREACH(const sa_summary_field_t& field, record.values) {
			switch (field.field_orign) {
			case FIELD_ORIGN_INPUT:
				update_stmt->setString(param_idx, input[field.field_serial]);
				break;
			case FIELD_ORIGN_GLOBAL:
				update_stmt->setString(param_idx, global[field.field_serial]);
				break;
			case FIELD_ORIGN_OUTPUT:
				update_stmt->setString(param_idx, output[field.field_serial]);
				break;
			default:
				throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: unknown field orign {1} for field_serial {2}") % field.field_orign % field.field_serial).str(APPLOCALE));
			}
			param_idx++;
		}

		BOOST_FOREACH(const sa_summary_field_t& field, record.keys) {
			switch (field.field_orign) {
			case FIELD_ORIGN_INPUT:
				update_stmt->setString(param_idx, input[field.field_serial]);
				break;
			case FIELD_ORIGN_GLOBAL:
				update_stmt->setString(param_idx, global[field.field_serial]);
				break;
			case FIELD_ORIGN_OUTPUT:
				update_stmt->setString(param_idx, output[field.field_serial]);
				break;
			default:
				throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: unknown field orign {1} for field_serial {2}") % field.field_orign % field.field_serial).str(APPLOCALE));
			}
			param_idx++;
		}

		update_stmt->executeUpdate();
	}
}

void sa_osummary::execute_update()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	if (items.empty()) {
		items.resize(adaptor.summary.records.size());
		for (int i = 0; i < items.size(); i++)
			create_stmt(i);
	}

	for (int i = 0; i < items.size(); i++)
		set_data(i);
}

string sa_osummary::get_binder(const sa_summary_field_t& field)
{
	string bind_str = ":";
	bind_str += field.column_name;
	bind_str += "{";

	switch (field.field_type) {
	case FIELD_TYPE_INT:
		bind_str += "int";
		break;
	case FIELD_TYPE_LONG:
		bind_str += "long";
		break;
	case FIELD_TYPE_FLOAT:
		bind_str += "float";
		break;
	case FIELD_TYPE_DOUBLE:
		bind_str += "double";
		break;
	case FIELD_TYPE_STRING:
		bind_str += "char[";
		bind_str += boost::lexical_cast<string>(field.field_size);
		bind_str += "]";
		break;
	default:
		break;
	}

	bind_str += "}";
	return bind_str;
}

DECLARE_SA_OUTPUT(OTYPE_SUMMARY, sa_osummary)

}
}

