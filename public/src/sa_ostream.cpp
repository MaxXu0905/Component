#include "sa_internal.h"

namespace ai
{
namespace app
{

using namespace std;
using namespace ai::sg;
using namespace ai::scci;

stat_file::stat_file(const vector<dst_field_t>& fields_)
	: fields(fields_),
	  first(true)
{
	// Allocate at lease size.
	values.reserve(fields.size());

	value_type record;
	record.int_value = 0;
	record.long_value = 0;
	record.float_value = 0.0;
	record.double_value = 0.0;
	record.string_value = "";

	for (int i = 0; i < fields.size(); i++)
		values.push_back(record);
}

stat_file::~stat_file()
{
}

void stat_file::statistics(const char **input, const char **global, const char **output)
{
	if (first) {
		init(input, global, output);
		first = false;
	} else {
		set(input, global, output);
	}
}

void stat_file::init(const char **input, const char **global, const char **output)
{
	for (int i = 0; i < fields.size(); i++) {
		const dst_field_t& field = fields[i];
		const char *field_value;

		switch (field.field_orign) {
		case FIELD_ORIGN_INPUT:
			field_value = input[field.field_serial];
			break;
		case FIELD_ORIGN_GLOBAL:
			field_value = global[field.field_serial];
			break;
		case FIELD_ORIGN_OUTPUT:
			field_value = output[field.field_serial];
			break;
		default:
			throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: unknown field orign {1} for field_serial {2}") % field.field_orign % field.field_serial).str(APPLOCALE));
		}

		switch (field.action) 	{
		case ADD_INT:
		case MIN_INT:
		case MAX_INT:
			values[i].int_value = ::atoi(field_value);
			break;
		case INC_INT:
			values[i].int_value = 1;
			break;
		case ADD_LONG:
		case MIN_LONG:
		case MAX_LONG:
			values[i].long_value = ::atol(field_value);
			break;
		case ADD_FLOAT:
		case MIN_FLOAT:
		case MAX_FLOAT:
			values[i].float_value = ::atof(field_value);
			break;
		case ADD_DOUBLE:
		case MIN_DOUBLE:
		case MAX_DOUBLE:
			values[i].double_value = ::atof(field_value);
			break;
		case MIN_STR:
		case MAX_STR:
			values[i].string_value = field_value;
			break;
		default:
			break;
		}
	}
}

void stat_file::set(const char **input, const char **global, const char **output)
{
	for (int i = 0; i < fields.size(); i++) {
		const dst_field_t& field = fields[i];
		const char *field_value;

		switch (field.field_orign) {
		case FIELD_ORIGN_INPUT:
			field_value = input[field.field_serial];
			break;
		case FIELD_ORIGN_GLOBAL:
			field_value = global[field.field_serial];
			break;
		case FIELD_ORIGN_OUTPUT:
			field_value = output[field.field_serial];
			break;
		default:
			throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: unknown field orign {1} for field_serial {2}") % field.field_orign % field.field_serial).str(APPLOCALE));
		}

		switch (field.action) {
		case ADD_INT:
			values[i].int_value += ::atoi(field_value);
			break;
		case INC_INT:
			++values[i].int_value;
			break;
		case MIN_INT:
			values[i].int_value = std::min(::atoi(field_value), values[i].int_value);
			break;
		case MAX_INT:
			values[i].int_value = std::max(::atoi(field_value), values[i].int_value);
			break;
		case ADD_LONG:
			values[i].long_value += ::atol(field_value);
			break;
		case MIN_LONG:
			values[i].long_value = std::min(::atol(field_value), values[i].long_value);
			break;
		case MAX_LONG:
			values[i].long_value = std::max(::atol(field_value), values[i].long_value);
			break;
		case ADD_FLOAT:
			values[i].float_value += ::atof(field_value);
			break;
		case MIN_FLOAT:
			values[i].float_value = std::min(static_cast<float>(::atof(field_value)), values[i].float_value);
			break;
		case MAX_FLOAT:
			values[i].float_value = std::max(static_cast<float>(::atof(field_value)), values[i].float_value);
			break;
		case ADD_DOUBLE:
			values[i].double_value += ::atof(field_value);
			break;
		case MIN_DOUBLE:
			values[i].double_value = std::min(::atof(field_value), values[i].double_value);
			break;
		case MAX_DOUBLE:
			values[i].double_value = std::max(::atof(field_value), values[i].double_value);
			break;
		case MIN_STR:
			values[i].string_value = std::min(string(field_value), values[i].string_value);
			break;
		case MAX_STR:
			values[i].string_value = std::max(string(field_value), values[i].string_value);
			break;
		default:
			break;
		}
	}
}

distr_file::distr_file(const dst_file_t& dst_file_, vector<slave_dblog_t>& slaves_, int idx_)
	: dst_file(dst_file_),
	  slaves(slaves_),
	  idx(idx_)
{
	const slave_dblog_t& slave = slave_log();
	const vector<dst_field_t>& head_fields = dst_file.head.fields;
	const vector<dst_field_t>& tail_fields = dst_file.tail.fields;
	string full_name = dst_file.dst_dir + slave.file_name + ".tmp";

	if (slave.break_point > 0)
		ofs.open(full_name.c_str(), ios_base::in | ios_base::out);
	else
		ofs.open(full_name.c_str(), ios_base::out);
	if (!ofs)
		throw bad_file(__FILE__, __LINE__, 132, bad_file::bad_open, full_name);

	if (slave.break_point > 0) {
		ofs.seekp(slave.break_point);
		if (!ofs)
			throw bad_file(__FILE__, __LINE__, 132, bad_file::bad_seek, full_name);
	}

	if (head_fields.size() > 0) {
		if (slave.break_point != 0) // If need to statistics, we must be do a file once a time.
			throw bad_param(__FILE__, __LINE__, 133, (_("ERROR: Having head, do not support breakpoint.")).str(APPLOCALE));

		hfile = new stat_file(head_fields);
	} else {
		hfile = 0;
	}

	if (tail_fields.size() > 0) {
		if (slave.break_point != 0) // If need to statistics, we must do a file once a time.
			throw bad_param(__FILE__, __LINE__, 134, (_("ERROR: Having tail, do not support breakpoint.")).str(APPLOCALE));

		tfile = new stat_file(tail_fields);
	} else {
		tfile = 0;
	}
}

distr_file::~distr_file()
{
	delete hfile;
	delete tfile;
}

slave_dblog_t& distr_file::slave_log()
{
	return slaves[idx];
}

sa_ostream::sa_ostream(sa_base& _sa, int _flags)
	: sa_output(_sa, _flags)
{
	switch (flags) {
	case OTYPE_DISTR:
		break;
	default:
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: unsupported flags {1}") % flags).str(APPLOCALE));
	}

	dst_files = &adaptor.distr.dst_files;

	if (SAP->_SAP_auto_mkdir) {
		BOOST_FOREACH(const dst_file_t& dst_file, *dst_files) {
			if (!common::mkdir(NULL, dst_file.dst_dir))
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: Failed to create directory {1}") % dst_file.dst_dir).str(APPLOCALE));

			if (!dst_file.sftp_prefix.empty()) {
				file_rpc& rpc_mgr = file_rpc::instance(SGT);

				if (!rpc_mgr.mkdir(dst_file.sftp_prefix, dst_file.rdst_dir))
					GPP->write_log(__FILE__, __LINE__, (_("ERROR: Failed to create directory {1} on {2} - {3}") % dst_file.rdst_dir % dst_file.sftp_prefix % SGT->strerror()).str(APPLOCALE));
			}
		}
	}
}

sa_ostream::~sa_ostream()
{
}

void sa_ostream::init()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(600, __PRETTY_FUNCTION__, "", NULL);
#endif
	compiler& cmpl = SAP->_SAP_cmpl;
	const field_map_t& global_map = adaptor.global_map;
	const field_map_t& output_map = ((adaptor.output_map.size() == FIXED_OUTPUT_SIZE) ? adaptor.input_maps[0] : adaptor.output_map);

	BOOST_FOREACH(const dst_file_t& item, *dst_files) {
		try {
			cond_idx.push_back(cmpl.create_function(item.rule_condition, global_map, output_map, field_map_t()));
		} catch (exception& ex) {
			throw bad_msg(__FILE__, __LINE__, 357, ex.what());
		}

		try {
			file_idx.push_back(cmpl.create_function(item.rule_file_name, global_map, output_map, field_map_t()));
		} catch (exception& ex) {
			throw bad_msg(__FILE__, __LINE__, 358, ex.what());
		}
	}
}

void sa_ostream::init2()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(600, __PRETTY_FUNCTION__, "", NULL);
#endif
	compiler& cmpl = SAP->_SAP_cmpl;

	BOOST_FOREACH(const int& idx, cond_idx) {
		cond_funs.push_back(cmpl.get_function(idx));
	}

	BOOST_FOREACH(const int& idx, file_idx) {
		file_funs.push_back(cmpl.get_function(idx));
	}
}

void sa_ostream::open()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(600, __PRETTY_FUNCTION__, "", NULL);
#endif
	dblog_item_t& item = SAT->_SAT_dblog[sa.get_id()];
	vector<slave_dblog_t>& slaves = item.slaves;

	file_map.clear();

	for (int i = 0; i < slaves.size(); i++) {
		slave_dblog_t& slave = slaves[i];
		const dst_file_t& dst_file = (*dst_files)[slave.file_serial];
		string full_name = dst_file.dst_dir + slave.file_name;

		file_map[full_name] = boost::shared_ptr<distr_file>(new distr_file(dst_file, slaves, i));
	}

	is_open = true;
}

int sa_ostream::write(int input_idx)
{
	int retval = 0;
#if defined(DEBUG)
	scoped_debug<int> debug(500, __PRETTY_FUNCTION__, (_("input_idx={1}") % input_idx).str(APPLOCALE), &retval);
#endif

	if (!is_open)
		open();

	char gen_name[MAX_LEN + 1];
	char *out[1] = { gen_name };
	const char **input = const_cast<const char **>(sa.get_input(input_idx));
	const char **global = const_cast<const char **>(sa.get_global());
	const char **output = const_cast<const char **>(sa.get_output());
	const char **inoutput = ((adaptor.output_map.size() == FIXED_OUTPUT_SIZE) ? input : output);

	for (int i = 0; i < cond_idx.size(); i++) {
		const dst_file_t& dst_file = (*dst_files)[i];
		const vector<dst_field_t>& body_fields = dst_file.body.fields;

		// Save fields which will be changed
		map<int, string> bak_fields;
		map<int, vector<string> > split_fields;
		int cycles = 1;
		if (dst_file.options & SA_OPTION_SPLIT) {
			BOOST_FOREACH(const dst_field_t& field, body_fields) {
				if (field.action == SPLIT_FIELD) {
					const int& field_serial = field.field_serial;
					string field_str = output[field_serial];

					vector<string> tmp_fields;
					common::string_to_array(field_str, ',', tmp_fields);

					if (tmp_fields.size() > 1) {
						bak_fields[field_serial] = field_str;
						cycles = std::max(cycles, static_cast<int>(tmp_fields.size()));
						split_fields[field_serial] = tmp_fields;

						dout << "split field " << std::setbase(10) << field_serial << ":" << std::endl;
						for (int loop = 0; loop < tmp_fields.size(); loop++)
							dout << "\tdata[" << loop << "] = [" << tmp_fields[loop] << "]" << std::endl;
					}
				}
			}
		}

		for (int j = 0; j < cycles; j++) {
			for (map<int, vector<string> >::const_iterator iter = split_fields.begin(); iter != split_fields.end(); ++iter) 	{
				if (j < iter->second.size()) {
					const string& second = iter->second[j];
					::memcpy(const_cast<char *>(output[iter->first]), second.c_str(), second.length() + 1);
				} else {
					const_cast<char *>(output[iter->first])[0] = '\0';
				}
			}

			if ((*cond_funs[i])(NULL, const_cast<char **>(global), inoutput, 0) != 0)
				continue;

			gen_name[0] = '\0';
			if ((*file_funs[i])(NULL, const_cast<char **>(global), inoutput, out) != 0
				|| gen_name[0] == '\0')
				throw bad_param(__FILE__, __LINE__, 135, (_("ERROR: rule_file_name error")).str(APPLOCALE));

			string full_name = dst_file.dst_dir + gen_name;
			file_map_t::iterator map_iter = file_map.find(full_name);
			if (map_iter == file_map.end()) { // This means it's a new file.
				if (file_map.size() > adaptor.distr.max_open_files)
					throw bad_param(__FILE__, __LINE__, 137, (_("ERROR: max_open_files too small")).str(APPLOCALE));

				dblog_item_t& item = SAT->_SAT_dblog[sa.get_id()];
				vector<slave_dblog_t>& slaves = item.slaves;

				slave_dblog_t slave;
				slave.file_serial = i;
				slave.file_name = gen_name;
				slaves.push_back(slave);
				map_iter = file_map.insert(std::make_pair(full_name,
					boost::shared_ptr<distr_file>(new distr_file(dst_file, slaves, slaves.size() - 1))))
					.first;
				write_head(global, output, *map_iter->second);
			} else {
				distr_file& file = *map_iter->second;
				slave_dblog_t& slave_log = file.slave_log();
				if (slave_log.file_serial != i
					&& !(dst_file.options & SA_OPTION_SAME_FILE)) {
					// Check this output case of the same directory and file name.
					throw bad_param(__FILE__, __LINE__, 138, (_("ERROR: dst_file has defined 2 files with the same directory and file name.")).str(APPLOCALE));
				}
			}

			distr_file& file = *map_iter->second;
			slave_dblog_t& slave_log = file.slave_log();
			if (dst_file.options & SA_OPTION_INTERNAL) // Internal format.
				write(input, global, output, file, body_fields);
			else
				write(input, global, output, file, dst_file.body);

			retval++;
			slave_log.record_count++;
			if (file.hfile)
				file.hfile->statistics(input, global, output);
			if (file.tfile)
				file.tfile->statistics(input, global, output);
		}

		for (map<int, string>::const_iterator iter = bak_fields.begin(); iter != bak_fields.end(); ++iter) {
			const string& second = iter->second;
			::memcpy(const_cast<char *>(output[iter->first]), second.c_str(), second.length() + 1);
 		}
	}

	return retval;
}

void sa_ostream::flush(bool completed)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, (_("completed={1}") % completed).str(APPLOCALE), NULL);
#endif

	if (!is_open)
		return;

	if (!completed) {
		for (file_map_t::iterator iter = file_map.begin(); iter != file_map.end(); ++iter) {
			distr_file& file = *iter->second;
			slave_dblog_t& slave_log = file.slave_log();

			file.ofs << std::flush;
			if (!file.ofs) {
				string full_name = file.dst_file.dst_dir + slave_log.file_name;
				throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: flush file {1} failed - {2}") % full_name % strerror(errno)).str(APPLOCALE));
			}

			slave_log.break_point = file.ofs.tellp();
		}
	} else {
		const char **global = const_cast<const char **>(sa.get_global());
		const char **output = const_cast<const char **>(sa.get_output());

		for (file_map_t::iterator iter = file_map.begin(); iter != file_map.end(); ++iter) {
			distr_file& file = *iter->second;
			slave_dblog_t& slave_log = file.slave_log();
			const dst_file_t& dst_file = (*dst_files)[slave_log.file_serial];
			const vector<dst_field_t>& head_fields = dst_file.head.fields;
			const vector<dst_field_t>& tail_fields = dst_file.tail.fields;

			// Now the file position is at the end, so do write tail first.
			if (tail_fields.size() > 0) {
				write_head_tail(global, output, file, *file.tfile, dst_file.tail);
				slave_log.record_count++;
			}

			slave_log.break_point = file.ofs.tellp();

	 		if (head_fields.size() > 0) {
				file.ofs.seekp(0);

				write_head_tail(global, output, file, *file.hfile, dst_file.head);
			}

			file.ofs << std::flush;
			if (!file.ofs) {
				string full_name = file.dst_file.dst_dir + slave_log.file_name;
				throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: flush file {1} failed - {2}") % full_name % strerror(errno)).str(APPLOCALE));
			}
		}
	}
}

void sa_ostream::close()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	// 在断点恢复时，需要先打开文件，以保证临时文件正常改名
	if (!is_open)
		open();

	is_open = false;
	BOOST_SCOPE_EXIT((&file_map)) {
		file_map.clear();
	} BOOST_SCOPE_EXIT_END

	for (file_map_t::iterator iter = file_map.begin(); iter != file_map.end(); ++iter) {
		distr_file& file = *iter->second;
		slave_dblog_t& slave_log = file.slave_log();
		const dst_file_t& dst_file = (*dst_files)[slave_log.file_serial];

		file.ofs.close();

		string full_name = iter->first + ".tmp";
		if (dst_file.sftp_prefix.empty()) {
			if (!common::rename(full_name.c_str(), iter->first.c_str()))
				throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Can't rename file {1} to {2} - {3}") % full_name % iter->first % strerror(errno)).str(APPLOCALE));
		} else {
			string rfull_name = dst_file.rdst_dir + slave_log.file_name;
			string rtmp_name = rfull_name + ".tmp";
			file_rpc& rpc_mgr = file_rpc::instance(SGT);

			if (!rpc_mgr.put(dst_file.sftp_prefix, full_name, rtmp_name))
				throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Can't put {1} to {2} on {3} - {4}") % full_name % rtmp_name % dst_file.sftp_prefix % SGT->strerror()).str(APPLOCALE));

			if (!rpc_mgr.rename(dst_file.sftp_prefix, rtmp_name, rfull_name))
				throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Can't rename {1} to {2} on {3} - {4}") % rtmp_name % rfull_name % dst_file.sftp_prefix % SGT->strerror()).str(APPLOCALE));

			if (::remove(full_name.c_str()) == -1)
				throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Can't remove file {1} - {2}") % full_name % strerror(errno)).str(APPLOCALE));
		}
	}
}

void sa_ostream::complete()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	// 在断点恢复时，需要先打开文件，以保证临时文件正常改名
	if (!is_open)
		open();

	is_open = false;
	BOOST_SCOPE_EXIT((&file_map)) {
		file_map.clear();
	} BOOST_SCOPE_EXIT_END

	for (file_map_t::iterator iter = file_map.begin(); iter != file_map.end(); ++iter) {
		distr_file& file = *iter->second;
		slave_dblog_t& slave_log = file.slave_log();
		const dst_file_t& dst_file = (*dst_files)[slave_log.file_serial];

		string full_name = iter->first + ".tmp";
		if (dst_file.sftp_prefix.empty()) {
			if (!common::rename(full_name.c_str(), iter->first.c_str()))
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't rename file {1} to {2} - {3}") % full_name % iter->first % strerror(errno)).str(APPLOCALE));
		} else {
			string rfull_name = dst_file.rdst_dir + slave_log.file_name;
			string rtmp_name = rfull_name + ".tmp";
			file_rpc& rpc_mgr = file_rpc::instance(SGT);

			if (!rpc_mgr.put(dst_file.sftp_prefix, full_name, rtmp_name)) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't put {1} to {2} on {3} - {4}") % full_name % rtmp_name % dst_file.sftp_prefix % SGT->strerror()).str(APPLOCALE));
				continue;
			}

			if (!rpc_mgr.rename(dst_file.sftp_prefix, rtmp_name, rfull_name)) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't rename {1} to {2} on {3} - {4}") % rtmp_name % rfull_name % dst_file.sftp_prefix % SGT->strerror()).str(APPLOCALE));
				continue;
			}

			if (::remove(full_name.c_str()) == -1) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't remove file {1} - {2}") % full_name % strerror(errno)).str(APPLOCALE));
				continue;
			}
		}
	}
}

void sa_ostream::clean()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	// 在断点恢复时，需要先打开文件，以保证临时文件正常删除
	if (!is_open)
		open();

	is_open = false;
	BOOST_SCOPE_EXIT((&file_map)) {
		file_map.clear();
	} BOOST_SCOPE_EXIT_END

	for (file_map_t::iterator iter = file_map.begin(); iter != file_map.end(); ++iter) {
		string tmp_name = iter->first + ".tmp";

		if (::remove(tmp_name.c_str()) == -1)
			GPP->write_log(__FILE__, __LINE__,  (_("ERROR: Can't remove file {1} - {2}") % tmp_name % strerror(errno)).str(APPLOCALE));
	}
}

void sa_ostream::recover()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	// 在断点恢复时，需要先打开文件，以保证临时文件正常改名
	if (!is_open)
		open();

	is_open = false;
	BOOST_SCOPE_EXIT((&file_map)) {
		file_map.clear();
	} BOOST_SCOPE_EXIT_END

	for (file_map_t::iterator iter = file_map.begin(); iter != file_map.end(); ++iter) {
		distr_file& file = *iter->second;

		file.ofs.close();
	}
}

void sa_ostream::dump(ostream& os)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(100, __PRETTY_FUNCTION__, "", NULL);
#endif

	BOOST_FOREACH(const dst_file_t& dst_file, *dst_files) {
		os << "mkdir -p " << dst_file.dst_dir << std::endl;
	}

	os << std::endl;
}

void sa_ostream::rollback(const string& raw_file_name, int file_sn)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, (_("raw_file_name={1}, file_sn={2}") % raw_file_name % file_sn).str(APPLOCALE), NULL);
#endif

	int insert_count = 0;
	Generic_Database *db = SAT->_SAT_db;
	auto_ptr<Generic_Statement> auto_stmt(db->create_statement());
	Generic_Statement *stmt = auto_stmt.get();

	auto_ptr<struct_dynamic> auto_data(db->create_data());
	struct_dynamic *data = auto_data.get();
	if (data == NULL)
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Memory allocation failure")).str(APPLOCALE));

	auto_ptr<Generic_Statement> auto_insert_stmt(db->create_statement());
	Generic_Statement *insert_stmt = auto_insert_stmt.get();

	auto_ptr<struct_dynamic> auto_insert_data(db->create_data());
	struct_dynamic *insert_data = auto_insert_data.get();
	if (insert_data == NULL)
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Memory allocation failure")).str(APPLOCALE));

	string sql_str = (boost::format("select file_serial{int}, file_name{char[%1%]}, record_count{int}, "
		"break_point{long} from log_integrator_list "
		"where integrator_name = :integrator_name{char[%2%]} and file_sn = :file_sn{int} "
		"and sa_id = :sa_id{int} ")
		% RAW_FILE_NAME_LEN % MAX_IDENT).str();
	data->setSQL(sql_str);
	stmt->bind(data);

	sql_str = (boost::format("insert into log_integrator_list_history(undo_id, integrator_name, file_sn, sa_id, "
		"file_serial, file_name, record_count, break_point) values(:undo_id{int}, :integrator_name{char[%1%]}, "
		":file_sn{int}, :sa_id{int}, :file_serial{int}, :file_name{char[%2%]}, :record_count{int}, :break_normal{long})")
		% INTEGRATE_NAME_LEN % RAW_FILE_NAME_LEN).str();
	insert_data->setSQL(sql_str);
	insert_stmt->bind(insert_data);

	stmt->setString(1, SAP->_SAP_resource.integrator_name);
	stmt->setInt(2, file_sn);
	stmt->setInt(3, sa.get_id());

	auto_ptr<Generic_ResultSet> auto_rset(stmt->executeQuery());
	Generic_ResultSet *rset = auto_rset.get();

	while (rset->next()) {
		int file_serial = rset->getInt(1);
		string file_name = rset->getString(2);
		int record_count = rset->getInt(3);
		long break_point = rset->getLong(4);

		if (file_serial < 0 || file_serial >= dst_files->size())
			throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: file serial out of range")).str(APPLOCALE));

		const dst_file_t& dst_file = (*dst_files)[file_serial];
		string full_name;

		full_name = dst_file.dst_dir + file_name;
		(void)::unlink(full_name.c_str());

		full_name += ".tmp";
		(void)::unlink(full_name.c_str());

		if (!dst_file.sftp_prefix.empty()) {
			file_rpc& rpc_mgr = file_rpc::instance(SGT);

			full_name = dst_file.rdst_dir + file_name;
			(void)rpc_mgr.unlink(dst_file.sftp_prefix, full_name);

			full_name += ".tmp";
			(void)rpc_mgr.unlink(dst_file.sftp_prefix, full_name);
		}

		if (insert_count)
			insert_stmt->addIteration();

		insert_stmt->setInt(1, SAP->_SAP_undo_id);
		insert_stmt->setString(2, SAP->_SAP_resource.integrator_name);
		insert_stmt->setInt(3, file_sn);
		insert_stmt->setInt(4, sa.get_id());
		insert_stmt->setInt(5, file_serial);
		insert_stmt->setString(6, file_name);
		insert_stmt->setInt(7, record_count);
		insert_stmt->setInt(8, break_point);

		if (++insert_count == insert_data->size()) {
			insert_stmt->executeUpdate();
			insert_count = 0;
		}
	}

	if (insert_count > 0)
		insert_stmt->executeUpdate();

	sql_str = (boost::format("delete from log_integrator_list "
		"where integrator_name = :integrator_name{char[%1%]} and  file_sn = :file_sn{int} and sa_id = :sa_id{int}")
		% INTEGRATE_NAME_LEN).str();

	data->setSQL(sql_str);
	stmt->bind(data);

	stmt->setString(1, SAP->_SAP_resource.integrator_name);
	stmt->setInt(2, file_sn);
	stmt->setInt(3, sa.get_id());
	stmt->executeUpdate();
}

void sa_ostream::global_rollback()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	BOOST_FOREACH(const dst_file_t& dst_file, *dst_files) {
		vector<string> files;
		scan_file<> scan(dst_file.dst_dir, dst_file.pattern);
		scan.get_files(files);
		BOOST_FOREACH(const string& filename, files) {
			string full_name = dst_file.dst_dir + filename;
			(void)::unlink(full_name.c_str());
		}

		if (!dst_file.sftp_prefix.empty()) {
			file_rpc& rpc_mgr = file_rpc::instance(SGT);

			files.clear();
			if (!rpc_mgr.dir(files, dst_file.sftp_prefix, dst_file.rdst_dir, dst_file.pattern))
				return;

			BOOST_FOREACH(const string& filename, files) {
				string full_name = dst_file.rdst_dir + filename;
				(void)rpc_mgr.unlink(dst_file.sftp_prefix, full_name);
			}
		}
	}
}

void sa_ostream::write(const char **input, const char **global, const char **output, distr_file& file, const dst_record_t& dst_record)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif
	const vector<dst_field_t>& fields = dst_record.fields;
	const char *value;

	for (int i = 0; i < fields.size(); i++) {
		const dst_field_t& field = fields[i];

		switch (field.field_orign) {
		case FIELD_ORIGN_INPUT:
			value = input[field.field_serial];
			break;
		case FIELD_ORIGN_GLOBAL:
			value = global[field.field_serial];
			break;
		case FIELD_ORIGN_OUTPUT:
			value = output[field.field_serial];
			break;
		default:
			throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: unknown field orign {1} for field_serial {2}") % field.field_orign % field.field_serial).str(APPLOCALE));
		}

		if (i > 0 && dst_record.delimiter != '\0')
			file.ofs << dst_record.delimiter;

		switch (field.field_type) {
		case FIELD_TYPE_STRING:
			if (dst_record.is_fixed && ::strlen(value) >= field.field_size) {
				file.ofs.write(value, field.field_size);
			} else {
				if (dst_record.is_fixed)
					set_format(file.ofs, field);

				file.ofs << value;
			}
			break;
		default:
			{
				ostringstream fmt;
				if (dst_record.is_fixed)
					set_format(fmt, field);

				switch (field.field_type) {
				case FIELD_TYPE_INT:
					fmt << ::atoi(value);
					break;
				case FIELD_TYPE_LONG:
					fmt << ::atol(value);
					break;
				case FIELD_TYPE_FLOAT:
				case FIELD_TYPE_DOUBLE:
					fmt << std::fixed << std::setprecision(field.precision);
					fmt << ::atof(value);
					break;
				default:
					break;
				}

				string field_str = fmt.str();
				if (dst_record.is_fixed)
					file.ofs.write(field_str.c_str(), field.field_size);
				else
					file.ofs << field_str;
			}
			break;
		}
	}

	if (dst_record.has_return) // Add return.
		file.ofs << '\r';

	file.ofs << '\n';
	if (!file.ofs) {
		slave_dblog_t& slave_log = file.slave_log();
		string full_name = file.dst_file.dst_dir + slave_log.file_name;
		throw bad_file(__FILE__, __LINE__, 124, bad_file::bad_write, full_name);
	}
}

void sa_ostream::write(const char **input, const char **global, const char **output, distr_file& file, const vector<dst_field_t>& fields)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif
	int j;
	const char *value;

	for (int i = 0; i < fields.size(); i++) {
		const dst_field_t& field = fields[i];

		switch (field.field_orign) {
		case FIELD_ORIGN_INPUT:
			value = input[field.field_serial];
			break;
		case FIELD_ORIGN_GLOBAL:
			value = global[field.field_serial];
			break;
		case FIELD_ORIGN_OUTPUT:
			value = output[field.field_serial];
			break;
		default:
			throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: unknown field orign {1} for field_serial {2}") % field.field_orign % field.field_serial).str(APPLOCALE));
		}

		if (i > 0)
			file.ofs << ',';

		for (j = 0; j < field.field_size; j++) {
			if (*value == '\0')
				break;

			file.ofs << *value;
			++value;
		}
		file.ofs << '\0';
		for (; j < field.field_size; j++)
			file.ofs << ' ';
	}

	file.ofs << '\n';
	if (!file.ofs) {
		slave_dblog_t& slave_log = file.slave_log();
		string full_name = file.dst_file.dst_dir + slave_log.file_name;
		throw bad_file(__FILE__, __LINE__, 124, bad_file::bad_write, full_name);
	}
}

void sa_ostream::write_head_tail(const char **global, const char **output, distr_file& file,
	stat_file& sfile, const dst_record_t& dst_record)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(400, __PRETTY_FUNCTION__, "", NULL);
#endif
	const vector<dst_field_t>& fields = dst_record.fields;
	const char *value;
	ostringstream fmt;

	for (int i = 0; i < fields.size(); i++) {
		const dst_field_t& field = fields[i];

		if (i > 0 && dst_record.delimiter != '\0')
			file.ofs << dst_record.delimiter;

		fmt.str("");

		if (dst_record.is_fixed)
			set_format(fmt, field);

		switch (field.action) {
		case ADD_INT:
		case MIN_INT:
		case MAX_INT:
		case INC_INT:
			fmt << sfile.values[i].int_value;
			break;
		case ADD_LONG:
		case MIN_LONG:
		case MAX_LONG:
			fmt << sfile.values[i].long_value;
			break;
		case ADD_FLOAT:
		case MIN_FLOAT:
		case MAX_FLOAT:
			fmt << std::fixed << std::setprecision(field.precision);
			fmt << sfile.values[i].float_value;
			break;
		case ADD_DOUBLE:
		case MIN_DOUBLE:
		case MAX_DOUBLE:
			fmt << std::fixed << std::setprecision(field.precision);
			fmt << sfile.values[i].double_value;
			break;
		case MIN_STR:
		case MAX_STR:
			fmt << sfile.values[i].string_value;
			break;
		case ACTION_NONE:
			switch (field.field_orign) {
			case FIELD_ORIGN_GLOBAL:
				value = global[field.field_serial];
				break;
			case FIELD_ORIGN_OUTPUT:
				value = output[field.field_serial];
				break;
			default:
				throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: unknown field orign {1} for field_serial {2}") % field.field_orign % field.field_serial).str(APPLOCALE));
			}

			switch (field.field_type) {
			case FIELD_TYPE_INT:
				fmt << ::atoi(value);
				break;
			case FIELD_TYPE_LONG:
				fmt << ::atol(value);
				break;
			case FIELD_TYPE_FLOAT:
			case FIELD_TYPE_DOUBLE:
				fmt << std::fixed << std::setprecision(field.precision);
				fmt << ::atof(value);
				break;
			case FIELD_TYPE_STRING:
				fmt << value;
				break;
			}
			break;
		default:
			throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Unsupported type given")).str(APPLOCALE));
		}

		string field_str = fmt.str();
		if (dst_record.is_fixed)
			file.ofs.write(field_str.c_str(), field.field_size);
		else
			file.ofs << field_str;
	}

	if (dst_record.has_return) // Add return.
		file.ofs << '\r';

	file.ofs << '\n';
	if (!file.ofs) {
		slave_dblog_t& slave_log = file.slave_log();
		string full_name = file.dst_file.dst_dir + slave_log.file_name;
		throw bad_file(__FILE__, __LINE__, 124, bad_file::bad_write, full_name);
	}
}

// Write head string if needed. This is used to write to a new file. So tail is no need.
void sa_ostream::write_head(const char **global, const char **output, distr_file& file)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(400, __PRETTY_FUNCTION__, "", NULL);
#endif
	slave_dblog_t& slave_log = file.slave_log();
	const dst_file_t& dst_file = (*dst_files)[slave_log.file_serial];
	const vector<dst_field_t>& head_fields = dst_file.head.fields;

	if (head_fields.size() > 0) {
		file.ofs.seekp(0);

		write_head_tail(global, output, file, *file.hfile, dst_file.head);
		slave_log.record_count++;
	}
	// Now the file position is after the first record, that is the right position to write normal records.
}

void sa_ostream::set_format(ostream& os, const dst_field_t& field)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(400, __PRETTY_FUNCTION__, "", NULL);
#endif

	// Set width.
	os << std::setw(field.field_size);

	// Set fill.
	os << std::setfill(field.fill);

	// Set align.
	os.unsetf(ios_base::left | ios_base::internal | ios_base::right);
	switch (field.align_mode) {
	case ALIGN_MODE_LEFT:
		os.setf(ios_base::left);
		break;
	case ALIGN_MODE_INTERNAL:
		os.setf(ios_base::internal);
		break;
	case ALIGN_MODE_RIGHT:
		os.setf(ios_base::right);
		break;
	default:
		break;
	}
}

DECLARE_SA_OUTPUT(OTYPE_DISTR, sa_ostream)

}
}

