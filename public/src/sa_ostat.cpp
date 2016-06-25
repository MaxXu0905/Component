#include "sa_internal.h"

namespace ai
{
namespace app
{

using namespace std;
using namespace ai::sg;
using namespace ai::scci;

sa_ostat::sa_ostat(sa_base& _sa, int _flags)
	: sa_output(_sa, _flags)
{
	switch (flags) {
	case OTYPE_STAT:
		break;
	default:
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: unsupported flags {1}") % flags).str(APPLOCALE));
	}

	items.resize(adaptor.stat.records.size());
	for (int i = 0; i < adaptor.stat.records.size(); i++)
		rollback_called.push_back(false);
}

sa_ostat::~sa_ostat()
{
	destroy();
}

void sa_ostat::init()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(600, __PRETTY_FUNCTION__, "", NULL);
#endif
	compiler& cmpl = SAP->_SAP_cmpl;
	const field_map_t& global_map = adaptor.global_map;
	const field_map_t& output_map = adaptor.output_map;

	field_map_t input_map;
	for (int i = 0; i < adaptor.input_maps.size(); i++) {
		const field_map_t& item = adaptor.input_maps[i];
		for (field_map_t::const_iterator iter = item.begin(); iter != item.end(); ++iter)
			input_map[iter->first] = iter->second;
	}

	BOOST_FOREACH(const sa_stat_record_t& item, adaptor.stat.records) {
		try {
			cond_idx.push_back(cmpl.create_function(item.rule_condition, global_map, input_map, output_map));
		} catch (exception& ex) {
			throw bad_msg(__FILE__, __LINE__, 357, ex.what());
		}

		try {
			table_idx.push_back(cmpl.create_function(item.rule_table_name, global_map, output_map, field_map_t()));
		} catch (exception& ex) {
			throw bad_msg(__FILE__, __LINE__, 358, ex.what());
		}
	}
}

void sa_ostat::init2()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(600, __PRETTY_FUNCTION__, "", NULL);
#endif
	compiler& cmpl = SAP->_SAP_cmpl;

	BOOST_FOREACH(const int& idx, cond_idx) {
		cond_funs.push_back(cmpl.get_function(idx));
	}

	BOOST_FOREACH(const int& idx, table_idx) {
		table_funs.push_back(cmpl.get_function(idx));
	}
}

void sa_ostat::open()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(600, __PRETTY_FUNCTION__, "", NULL);
#endif
}

int sa_ostat::write(int input_idx)
{
	int retval = 0;
#if defined(DEBUG)
	scoped_debug<int> debug(500, __PRETTY_FUNCTION__, (_("input_idx={1}") % input_idx).str(APPLOCALE), &retval);
#endif

	char gen_name[MAX_LEN + 1];
	char *out[1] = { gen_name };
	const char **input = const_cast<const char **>(sa.get_input(input_idx));
	const char **global = const_cast<const char **>(sa.get_global());
	char **output = sa.get_output();

	for (int i = 0; i < cond_idx.size(); i++) {
		ostat_item_t& item = items[i];

		if ((*cond_funs[i])(NULL, const_cast<char **>(global), input, output) != 0)
			continue;

		if ((*table_funs[i])(NULL, const_cast<char **>(global), const_cast<const char **>(output), out) != 0)
			throw bad_param(__FILE__, __LINE__, 135, (_("ERROR: rule_table_name error")).str(APPLOCALE));

		// 如果表不一致，则创建表结构
		if (item.table_name != gen_name) {
			if (!item.table_name.empty())
				execute_update(i);
			item.table_name = gen_name;
			create_stmt(i);
		}

		// 设置记录
		set_data(input_idx, i);
		retval++;
	}

	return retval;
}

void sa_ostat::flush(bool completed)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, (_("completed={1}") % completed).str(APPLOCALE), NULL);
#endif

	execute_update();
}

void sa_ostat::close()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	execute_update();
}

void sa_ostat::clean()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	execute_update();
}

void sa_ostat::recover()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	destroy();
}

void sa_ostat::dump(ostream& os)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(100, __PRETTY_FUNCTION__, "", NULL);
#endif
	const vector<sa_stat_record_t>& records = adaptor.stat.records;

	for (int i = 0; i < records.size(); i++) {
		const sa_stat_record_t& record = records[i];

		os << "create table XXX_" << i
			<< "\n(\n";

		bool first = true;
		BOOST_FOREACH(const sa_stat_field_t& field, record.keys) {
			if (first)
				first = false;
			else
				os << "),\n";

			os << '\t'
				<< field.element_name
				<< " varchar2("
				<< field.field_size;
		}

		BOOST_FOREACH(const sa_stat_field_t& field, record.values) {
			if (first)
				first = false;
			else
				os << "),\n";

			os << '\t'
				<< field.element_name
				<< " varchar2("
				<< field.field_size;
		}

		os << ")\n"
			<< ");\n\n";
	}
}

void sa_ostat::rollback(const string& raw_file_name, int file_sn)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(100, __PRETTY_FUNCTION__, (_("raw_file_name={1}, file_sn={2}") % raw_file_name % file_sn).str(APPLOCALE), NULL);
#endif

	const vector<sa_stat_record_t>& records = adaptor.stat.records;
	string sql_str;

	for (int i = 0; i < records.size(); i++) {
		const sa_stat_record_t& record = records[i];

		if (rollback_called[i] && record.rollback_type != ROLLBACK_BY_DELETE)
			return;

		rollback_called[i] = true;

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

void sa_ostat::create_stmt(int idx)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, (_("idx={1}") % idx).str(APPLOCALE), NULL);
#endif
	Generic_Database *db = SAT->_SAT_db;
	const sa_stat_record_t& record = adaptor.stat.records[idx];
	ostat_item_t& item = items[idx];
	vector<int>& keys = item.keys;
	int *& sort_array = item.sort_array;

	keys.clear();

	if (record.sort && sort_array == NULL)
		sort_array = new int[adaptor.source.per_records];

	try {
		if (record.stat_action & STAT_ACTION_INSERT) {
			Generic_Statement *& insert_stmt = item.insert_stmt;
			struct_dynamic *& insert_data = item.insert_data;

			if (insert_stmt == NULL) {
				insert_stmt = db->create_statement();
				insert_data = db->create_data(false, adaptor.source.per_records);
			}

			string select_clause;
			string insert_clause;
			int key_idx = 0;

			BOOST_FOREACH(const sa_stat_field_t& field, record.values) {
				if (!select_clause.empty())
					select_clause += ", ";
				if (!insert_clause.empty())
					insert_clause += ", ";

				select_clause += field.element_name;
				insert_clause += field.insert_desc;
				key_idx += field.insert_times;
			}

			BOOST_FOREACH(const sa_stat_field_t& field, record.keys) {
				if (!select_clause.empty())
					select_clause += ", ";
				if (!insert_clause.empty())
					insert_clause += ", ";

				select_clause += field.element_name;
				insert_clause += field.insert_desc;

				if (!(record.stat_action & STAT_ACTION_UPDATE)) {
					keys.push_back(key_idx);
					key_idx += field.insert_times;
				}
			}

			string sql_str = "insert into ";
			sql_str += item.table_name;
			sql_str += "(";
			sql_str += select_clause;
			sql_str += ") values(";
			sql_str += insert_clause;
			sql_str += ")";

			insert_data->setSQL(sql_str);
			insert_stmt->bind(insert_data);

			if (!record.sort && (record.stat_action & STAT_ACTION_UPDATE))
				insert_stmt->setExecuteMode(EM_BATCH_ERRORS);

			dout << __PRETTY_FUNCTION__ << ": sql_insert = [" << sql_str << "]\n";
		}

		if (record.stat_action & STAT_ACTION_UPDATE) {
			Generic_Statement *& update_stmt = item.update_stmt;
			struct_dynamic *& update_data = item.update_data;

			if (update_stmt == NULL) {
				update_stmt = db->create_statement();
				update_data = db->create_data(false, adaptor.source.per_records);
			}

			string sql_str = "update ";
			sql_str += item.table_name;
			sql_str += " set ";

			bool first = true;
			int key_idx = 0;

			BOOST_FOREACH(const sa_stat_field_t& field, record.values) {
				if (first)
					first = false;
				else
					sql_str += ", ";

				sql_str += field.element_name;
				sql_str += " = ";
				sql_str += field.update_desc;
				key_idx += field.update_times;
			}

			sql_str += " where ";

			first = true;
			BOOST_FOREACH(const sa_stat_field_t& field, record.keys) {
				if (first)
					first = false;
				else
					sql_str += " and ";

				sql_str += field.element_name;
				sql_str += " ";
				sql_str += field.operation;
				sql_str += " ";
				sql_str += field.update_desc;

				keys.push_back(key_idx);
				key_idx += field.update_times;
			}

			update_data->setSQL(sql_str);
			update_stmt->bind(update_data);

			dout << __PRETTY_FUNCTION__ << ": sql_update = [" << sql_str << "]\n";
		}
	} catch (bad_sql& ex) {
		throw bad_msg(__FILE__, __LINE__, 0, ex.what());
	}
}

void sa_ostat::set_data(int input_idx, int idx)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, (_("input_idx={1}, idx={2}") % input_idx % idx).str(APPLOCALE), NULL);
#endif
	const sa_stat_record_t& record = adaptor.stat.records[idx];
	ostat_item_t& item = items[idx];
	const char **input = const_cast<const char **>(sa.get_input(input_idx));
	const char **global = const_cast<const char **>(sa.get_global());
	const char **output = const_cast<const char **>(sa.get_output());
	bool set_insert;
	bool set_update;

	switch (record.stat_action) {
	case STAT_ACTION_INSERT:
		set_insert = true;
		set_update = false;
		break;
	case STAT_ACTION_UPDATE:
		set_insert = false;
		set_update = true;
		break;
	case STAT_ACTION_INSERT | STAT_ACTION_UPDATE:
		if (!record.same_struct) {
			set_insert = true;
			set_update = true;
		} else if (record.sort) {
			set_insert = false;
			set_update = true;
		} else {
			set_insert = true;
			set_update = false;
		}
		break;
	default:
		return;
	}

	if (set_insert) {
		Generic_Statement *& insert_stmt = item.insert_stmt;
		int param_idx = 1;

		if (item.do_update & SA_INSERT_MASK)
			insert_stmt->addIteration();
		else
			item.do_update |= SA_INSERT_MASK;

		BOOST_FOREACH(const sa_stat_field_t& field, record.values) {
			for (int i = 0; i < field.insert_times; i++) {
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
		}

		BOOST_FOREACH(const sa_stat_field_t& field, record.keys) {
			for (int i = 0; i < field.insert_times; i++) {
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
					throw bad_msg(__FILE__, __LINE__, 0,(_("ERROR: unknown field orign {1} for field_serial {2}") % field.field_orign % field.field_serial).str(APPLOCALE));
				}
				param_idx++;
			}
		}
	}

	if (set_update) {
		Generic_Statement *& update_stmt = item.update_stmt;
		int param_idx = 1;

		if (item.do_update & SA_UPDATE_MASK)
			update_stmt->addIteration();
		else
			item.do_update |= SA_UPDATE_MASK;

		BOOST_FOREACH(const sa_stat_field_t& field, record.values) {
			for (int i = 0; i < field.update_times; i++) {
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
		}

		BOOST_FOREACH(const sa_stat_field_t& field, record.keys) {
			for (int i = 0; i < field.update_times; i++) {
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
		}
	}
}

void sa_ostat::execute_update()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	for (int i = 0; i < items.size(); i++)
		execute_update(i);
}

void sa_ostat::execute_update(int idx)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, (_("idx={1}") % idx).str(APPLOCALE), NULL);
#endif

	ostat_item_t& item = items[idx];
	if (!item.do_update)
		return;

	item.do_update = 0;

	const sa_stat_record_t& record = adaptor.stat.records[idx];
	switch (record.stat_action) {
	case STAT_ACTION_INSERT:
		{
			Generic_Statement *& insert_stmt = item.insert_stmt;
			if (record.sort)
				insert_stmt->sort(item.keys, item.sort_array);
			insert_stmt->executeUpdate();
		}
		break;
	case STAT_ACTION_UPDATE:
		{
			Generic_Statement *& update_stmt = item.update_stmt;
			if (record.sort)
				update_stmt->sort(item.keys, item.sort_array);
			update_stmt->executeUpdate();
		}
		break;
	case (STAT_ACTION_INSERT | STAT_ACTION_UPDATE):
		{
			Generic_Statement *& insert_stmt = item.insert_stmt;
			Generic_Statement *& update_stmt = item.update_stmt;

			if (record.sort) {
				// 需要排序，则允许多进程，需要单条执行
				int rows = update_stmt->getCurrentIteration() + 1;
				update_stmt->sort(item.keys, item.sort_array);
				update_stmt->resetIteration();

				if (!record.same_struct)
					insert_stmt->resetIteration();

				for (int i = 0; i < rows; i++) {
					update_stmt->copy_bind(0, *update_stmt, i);
					update_stmt->executeArrayUpdate(1);

					int count = update_stmt->getUpdateCount();
					if (count > 1) {
						GPP->write_log(__FILE__, __LINE__, (_("ERROR: Failed to update table, please check primary key")).str(APPLOCALE));
						exit(1);
					} else if (count == 0) {
						if (record.same_struct)
							insert_stmt->copy_bind(0, *update_stmt, i);
						else
							insert_stmt->copy_bind(0, *insert_stmt, item.sort_array[i]);

						try {
							insert_stmt->executeArrayUpdate(1);
						} catch (bad_sql& ex) {
							update_stmt->executeArrayUpdate(1);
							if (update_stmt->getUpdateCount() != 1) {
								GPP->write_log(__FILE__, __LINE__, (_("ERROR: Failed to update table, please check primary key, insert exception: {1}") % ex.what()).str(APPLOCALE));
								SAT->db_rollback();
								exit(1);
							}
						}
					}
				}
			} else {
				// 不需排序，单进程模式，可批量执行
				if (!record.same_struct)
					update_stmt->resetIteration();

				insert_stmt->executeUpdate();

				int error_count = insert_stmt->getRowsErrorCount();
				if (error_count > 0) {
					ub4 *error_array = insert_stmt->getRowsError();

					for (int i = 0; i < error_count; i++)
						update_stmt->copy_bind(i, record.same_struct ? *insert_stmt : *update_stmt, error_array[i]);

					update_stmt->executeArrayUpdate(error_count);
				}
			}
		}
		break;
	default:
		break;
	}
}

void sa_ostat::destroy()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif
	Generic_Database *db = SAT->_SAT_db;

	BOOST_FOREACH(ostat_item_t& item, items) {
		delete []item.sort_array;

		if (db != NULL) {
			db->terminate_data(item.update_data);
			db->terminate_statement(item.update_stmt);
			db->terminate_data(item.insert_data);
			db->terminate_statement(item.insert_stmt);
		}

		item.table_name.clear();
		item.keys.clear();
		item.do_update = 0;
	}
}

DECLARE_SA_OUTPUT(OTYPE_STAT, sa_ostat)

}
}

