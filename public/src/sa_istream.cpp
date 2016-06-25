#include "sa_internal.h"
#include "schd_rpc.h"
#include "sftp.h"
#include "sa_global.h"

namespace ai
{
namespace app
{

typedef boost::tokenizer<boost::char_separator<char> > tokenizer;

namespace bf = boost::filesystem;
namespace bi = boost::interprocess;
namespace bp = boost::posix_time;
using namespace ai::sg;
using namespace ai::scci;

sa_istream::sa_istream(sa_base& _sa, int _flags)
	: sa_input(_sa, _flags)
{
	const map<string, string>& args = adaptor.source.args;
	map<string, string>::const_iterator iter;
	gpenv& env_mgr = gpenv::instance();

	iter = args.find("max_records");
	if (iter == args.end())
		max_records = -1;
	else
		max_records = boost::lexical_cast<int>(iter->second.c_str());

	iter = args.find("rule_hcheck");
	if (iter != args.end())
		rule_hcheck = iter->second;

	iter = args.find("src_dir");
	if (iter == args.end())
		throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: source.args.arg.src_dir value invalid.")).str(APPLOCALE));

	env_mgr.expand_var(src_dir, iter->second);
	if (*src_dir.rbegin() != '/')
		src_dir += '/';

	iter = args.find("dup_dir");
	if (iter == args.end())
		throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: source.args.arg.dup_dir value invalid.")).str(APPLOCALE));

	env_mgr.expand_var(dup_dir, iter->second);
	if (*dup_dir.rbegin() != '/')
		dup_dir += '/';

	iter = args.find("bak_dir");
	if (iter == args.end())
		throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: source.args.arg.bak_dir value invalid.")).str(APPLOCALE));

	env_mgr.expand_var(bak_dir, iter->second);
	if (*bak_dir.rbegin() != '/')
		bak_dir += '/';

	iter = args.find("tmp_dir");
	if (iter == args.end())
		throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: source.args.arg.tmp_dir value invalid.")).str(APPLOCALE));

	env_mgr.expand_var(tmp_dir, iter->second);
	if (*tmp_dir.rbegin() != '/')
		tmp_dir += '/';

	iter = args.find("ftl_dir");
	if (iter == args.end())
		throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: source.args.arg.ftl_dir value invalid.")).str(APPLOCALE));

	env_mgr.expand_var(ftl_dir, iter->second);
	if (*ftl_dir.rbegin() != '/')
		ftl_dir += '/';

	iter = args.find("protocol");
	if (iter != args.end())
		env_mgr.expand_var(sftp_prefix, iter->second);

	if (!sftp_prefix.empty()) {
		if (memcmp(sftp_prefix.c_str(), "sftp://", 7) != 0)
			throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: source.args.arg.protocol value should begin with sftp://")).str(APPLOCALE));

		string sub_protocol = sftp_prefix.substr(7, sftp_prefix.length() - 7);

		if (sub_protocol.find('@') != string::npos) {
			sftp_prefix = sub_protocol;
		} else {
			const char *ptr = getenv("LOGNAME");
			if (ptr == NULL)
				throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: environment variable LOGNAME not set")).str(APPLOCALE));

			sftp_prefix = ptr;
			sftp_prefix += "@";
			sftp_prefix += sub_protocol;
		}

		iter = args.find("local_dir");
		if (iter == args.end()) {
			local_dir = tmp_dir;
		} else {
			env_mgr.expand_var(local_dir, iter->second);
			if (*local_dir.rbegin() != '/')
				local_dir += '/';
		}

		// 对于本地协议，直接忽略
		if (common::is_local(sftp_prefix)) {
			sftp_prefix.clear();
			local_dir = tmp_dir;
		}
	} else {
		local_dir = tmp_dir;
	}

	iter = args.find("pattern");
	if (iter == args.end())
		pattern = "^.*$";
	else
		env_mgr.expand_var(pattern, iter->second);

	iter = args.find("bak_flag");
	if (iter == args.end()) {
		bak_flag = BAK_FLAG_SUFFIX;
	} else {
		const string& bak_flag_str = iter->second;
		if (strcasecmp(bak_flag_str.c_str(), "REMOVE") == 0)
			bak_flag = BAK_FLAG_REMOVE;
		else if (strcasecmp(bak_flag_str.c_str(), "SUFFIX") == 0)
			bak_flag = BAK_FLAG_SUFFIX;
		else if (strcasecmp(bak_flag_str.c_str(), "RENAME") == 0)
			bak_flag = BAK_FLAG_RENAME;
		else
			throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: source.args.arg.bak_flag value invalid.")).str(APPLOCALE));
	}

	iter = args.find("zip_cmd");
	if (iter == args.end())
		zip_cmd = "";
	else
		zip_cmd = iter->second;

	iter = args.find("sleep_time");
	if (iter == args.end()) {
		sleep_time = DFLT_SLEEP_TIME;
	} else {
		sleep_time = atoi(iter->second.c_str());
		if (sleep_time <= 0)
			sleep_time = DFLT_SLEEP_TIME;
	}

	iter = args.find("exit_nofile");
	if (iter == args.end())
		exit_nofile = false;
	else if (iter->second == "Y")
		exit_nofile = true;
	else
		exit_nofile = false;

	iter = args.find("deal_flag");
	if (iter == args.end()) {
		deal_flag = DEAL_FLAG_PARTIAL;
	} else {
		const string& deal_flag_str = iter->second;
		if (strcasecmp(deal_flag_str.c_str(), "PARTIAL") == 0)
			deal_flag = DEAL_FLAG_PARTIAL;
		else if (strcasecmp(deal_flag_str.c_str(), "WHOLE") == 0)
			deal_flag = DEAL_FLAG_WHOLE;
		else if (strcasecmp(deal_flag_str.c_str(), "CHECK") == 0)
			deal_flag = DEAL_FLAG_CHECK;
		else
			throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: source.args.arg.deal_flag value invalid.")).str(APPLOCALE));
	}

	iter = args.find("rule_check");
	if (iter != args.end())
		rule_check = iter->second;

	if (!rule_check.empty() && deal_flag != DEAL_FLAG_CHECK)
		throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: rule_check is defined but source.args.arg.deal_flag is not CHECK.")).str(APPLOCALE));

	fscan = NULL;

	if (SAP->_SAP_auto_mkdir) {
		if (sftp_prefix.empty()) {
			if (!common::mkdir(NULL, src_dir))
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: Failed to create directory {1} - {2}") % src_dir % strerror(errno)).str(APPLOCALE));
			if (!common::mkdir(NULL, dup_dir))
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: Failed to create directory {1} - {2}") % dup_dir % strerror(errno)).str(APPLOCALE));
			if (!common::mkdir(NULL, bak_dir))
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: Failed to create directory {1} - {2}") % bak_dir % strerror(errno)).str(APPLOCALE));
			if (!common::mkdir(NULL, tmp_dir))
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: Failed to create directory {1} - {2}") % tmp_dir % strerror(errno)).str(APPLOCALE));
			if (!common::mkdir(NULL, ftl_dir))
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: Failed to create directory {1} - {2}") % ftl_dir % strerror(errno)).str(APPLOCALE));
		} else {
			file_rpc& rpc_mgr = file_rpc::instance(SGT);

			if (!rpc_mgr.mkdir(sftp_prefix, src_dir))
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: Failed to create directory {1} on {2} - {3}") % src_dir % sftp_prefix % SGT->strerror()).str(APPLOCALE));
			if (!rpc_mgr.mkdir(sftp_prefix, dup_dir))
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: Failed to create directory {1} on {2} - {3}") % dup_dir % sftp_prefix % SGT->strerror()).str(APPLOCALE));
			if (!rpc_mgr.mkdir(sftp_prefix, bak_dir))
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: Failed to create directory {1} on {2} - {3}") % bak_dir % sftp_prefix % SGT->strerror()).str(APPLOCALE));
			if (!rpc_mgr.mkdir(sftp_prefix, tmp_dir))
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: Failed to create directory {1} on {2} - {3}") % tmp_dir % sftp_prefix % SGT->strerror()).str(APPLOCALE));
			if (!rpc_mgr.mkdir(sftp_prefix, ftl_dir))
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: Failed to create directory {1} on {2} - {3}") % ftl_dir % sftp_prefix % SGT->strerror()).str(APPLOCALE));

			if (!common::mkdir(NULL, local_dir))
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: Failed to create directory {1} - {2}") % local_dir % strerror(errno)).str(APPLOCALE));
		}
	}

	file_iter = files.begin();
}

sa_istream::~sa_istream()
{
	delete fscan;
}

void sa_istream::init()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(600, __PRETTY_FUNCTION__, "", NULL);
#endif
	compiler& cmpl = SAP->_SAP_cmpl;
	const field_map_t& global_map = adaptor.global_map;
	const field_map_t& output_map = adaptor.output_map;

	try {
		hchk_idx = cmpl.create_function(rule_hcheck, field_map_t(), field_map_t(), field_map_t());
	} catch (exception& ex) {
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Can't create function for rule_hcheck, {1}") % ex.what()).str(APPLOCALE));
	}

	const vector<input_record_t>& input_records = adaptor.input_records;
	for (int i = 0; i < input_records.size(); i++) {
		const input_record_t& input_record = input_records[i];
		try {
			record_serial_idx.push_back(cmpl.create_function(input_record.rule_condition, global_map, field_map_t(), field_map_t()));
		} catch (exception& ex) {
			throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Can't create function for rule_condition, {1}") % ex.what()).str(APPLOCALE));
		}
	}

	if (deal_flag == DEAL_FLAG_CHECK) {
		try {
			chk_idx = cmpl.create_function(rule_check, global_map, field_map_t(), output_map);
		} catch (exception& ex) {
			throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Can't create function for rule_check, {1}") % ex.what()).str(APPLOCALE));
		}
	}
}

void sa_istream::init2()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(600, __PRETTY_FUNCTION__, "", NULL);
#endif
	compiler& cmpl = SAP->_SAP_cmpl;

	hchk_func = cmpl.get_function(hchk_idx);

	BOOST_FOREACH(const int& idx, record_serial_idx) {
		record_serial_func.push_back(cmpl.get_function(idx));
	}

	if (deal_flag == DEAL_FLAG_CHECK)
		chk_func = cmpl.get_function(chk_idx);

	if (!sftp_prefix.empty()) {
		sgt_ctx *SGT = sgt_ctx::instance();
		sgc_ctx *SGC = SGT->SGC();
		const char *lmid_str = SGC->mid2lmid(SGC->_SGC_proc.mid);
		if (lmid_str == NULL)
			throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: Can't find hostid for node id {1}") % SGC->_SGC_proc.mid).str(APPLOCALE));

		lmid = lmid_str;

		file_prefix = (boost::format("%1%.%2%.") % lmid % SAP->_SAP_pid).str();
	} else {
		fscan = new scan_file<>(src_dir, pattern);
		file_prefix = (boost::format("%1%.") % SAP->_SAP_pid).str();
	}

	get_redo_files();
}

bool sa_istream::open()
{
	bool retval = false;
#if defined(DEBUG)
	scoped_debug<bool> debug(600, __PRETTY_FUNCTION__, "", &retval);
#endif

	// 获取文件
	while (!get_file()) {
		if (SGP->_SGP_shutdown)
			return retval;

		// 如果没有文件，则需要查看是否有进程异常未处理的文件
		get_redo_files();

		if (SGP->_SGP_shutdown)
			return retval;

		if (!redo_files.empty())
			continue;

		if (exit_nofile) {
			try {
				BOOST_FOREACH(sa_base *sa, SAT->_SAT_sas) {
					sa->post_run();
				}

				// 报告正常退出
				schd_rpc& rpc_mgr = schd_rpc::instance();
				rpc_mgr.report_exit();
			} catch (exception& ex) {
				GPP->write_log(__FILE__, __LINE__, ex.what());
			}

			SGP->_SGP_shutdown++;
			return retval;
		}

		if (SGP->_SGP_shutdown)
			return retval;

		sleep(sleep_time);

		if (SGP->_SGP_shutdown)
			return retval;
	}

	bool is_fatal = true;
	sa_istream *_this = this;
	BOOST_SCOPE_EXIT((&is_fatal)(&retval)(&_this)) {
		if (retval)
			return;

		string full_name;
		_this->SAT->_SAT_file_log.file_count++;
		if (is_fatal) {
			full_name = _this->ftl_dir + _this->src_name;
			_this->SAT->_SAT_file_log.file_fatal++;
		} else {
			full_name = _this->dup_dir + _this->src_name;
			_this->SAT->_SAT_file_log.file_dup++;
		}

		if (_this->sftp_prefix.empty()) {
			if (!common::rename(_this->target_full_name.c_str(), full_name.c_str()))
				_this->api_mgr.write_log(__FILE__, __LINE__, (_("ERROR: Can't move file {1} to {2}, {3}") % _this->target_full_name % full_name % strerror(errno)).str(APPLOCALE));
		} else {
			file_rpc& rpc_mgr = file_rpc::instance(_this->SGT);
			if (!rpc_mgr.rename(_this->sftp_prefix, _this->target_full_name, full_name))
				_this->api_mgr.write_log(__FILE__, __LINE__, (_("ERROR: Can't rename {1} to {2} on {3} - {4}") % _this->target_full_name % full_name % _this->sftp_prefix % _this->SGT->strerror()).str(APPLOCALE));

			if (::remove(_this->local_full_name.c_str()) == -1)
				_this->api_mgr.write_log(__FILE__, __LINE__, (_("ERROR: Can't remove file {1} - {2}") % _this->local_full_name % strerror(errno)).str(APPLOCALE));
		}
	} BOOST_SCOPE_EXIT_END

	if (src_name.length() > RAW_FILE_NAME_LEN) {
		api_mgr.write_log(__FILE__, __LINE__, (_("ERROR: filename too long for target file {1}") % target_full_name).str(APPLOCALE));
		return retval;
	}

	struct stat st;
	if (::stat(local_full_name.c_str(), &st) < 0) {
		api_mgr.write_log(__FILE__, __LINE__, (_("ERROR: Can't stat target file {1}, {2}") % local_full_name % strerror(errno)).str(APPLOCALE));
		return retval;
	}

	end_pos = st.st_size;
	SAT->_SAT_file_time = st.st_mtime;

	file_correct = true;
	SAT->_SAT_raw_file_name = src_name;
	SAT->_SAT_record_type = RECORD_TYPE_BODY;
	SAT->clear();

	// Get log from database.
	try {
		dblog_manager& dblog_mgr = dblog_manager::instance(SAT);
		dblog_mgr.select();
	} catch (bad_sql& ex) {
		api_mgr.write_log(__FILE__, __LINE__, ex.what());
		// 后续尝试恢复
		retval = true;
		throw;
	}

	SAT->_SAT_global[GLOBAL_SVC_KEY_SERIAL][0] = '\0';
	memcpy(SAT->_SAT_global[GLOBAL_FILE_NAME_SERIAL], SAT->_SAT_raw_file_name.c_str(), SAT->_SAT_raw_file_name.length() + 1);
	sprintf(SAT->_SAT_global[GLOBAL_FILE_SN_SERIAL], "%d", SAT->_SAT_file_sn);
	strcpy(SAT->_SAT_global[GLOBAL_REDO_MARK_SERIAL], "0000");

	datetime dt(SAT->_SAT_login_time);
	string dts;
	dt.iso_string(dts);
	memcpy(SAT->_SAT_global[GLOBAL_SYSDATE_SERIAL], dts.c_str(), dts.length() + 1);
	strcpy(SAT->_SAT_global[GLOBAL_FIRST_SERIAL], "0");
	strcpy(SAT->_SAT_global[GLOBAL_SECOND_SERIAL], "0");

	if (master.completed == 2) {
		// 待完成时异常
		is_fatal = false;
		SAT->on_complete();
		return retval;
	} else if (master.completed == 1) {
		// 重复文件
		is_fatal = false;
		api_mgr.write_log(__FILE__, __LINE__, (_("ERROR: Dealing file {1} duplicate, {2}") % src_full_name % strerror(errno)).str(APPLOCALE));
		return retval;
	}

	is.reset(common::create_istream(STREAM_TYPE_IFSTREAM, "", local_full_name.c_str()));
	if (!*is) {
		api_mgr.write_log(__FILE__, __LINE__, (_("ERROR: Can't open source file {1}, {2}") % src_full_name % strerror(errno)).str(APPLOCALE));
		return retval;
	}

	// 打开全局变量对象
	sa_global& global_mgr = sa_global::instance(SAT);
	global_mgr.open();

	if (master.break_point > 0) {
		is->seekg(master.break_point, std::ios_base::beg);
		if (!*is) {
			api_mgr.write_log(__FILE__, __LINE__, (_("ERROR: Can't seek source file {1}, {2}") % src_full_name % strerror(errno)).str(APPLOCALE));
			return retval;
		}

		// 加载全局变量
		global_mgr.load();
	} else {
		if (max_records >= 0 || !rule_hcheck.empty()) {
			string head_str;
			std::getline(*is, head_str);
			if (head_str.empty()) {
				api_mgr.write_log(__FILE__, __LINE__, (_("ERROR: Can't pass rule_hcheck for {1}") % src_full_name).str(APPLOCALE));
				return retval;
			}

			char buf[4096];
			int records = 1;
			while (!*is) {
				is->read(buf, sizeof(buf));
				char *bufe = buf + is->gcount();
				for (char *bufp = buf; bufp < bufe; ++bufp) {
					if (*bufp == '\n')
						records++;
				}
			}

			if (max_records >= 0 && records > max_records) {
				api_mgr.write_log(__FILE__, __LINE__, (_("ERROR: Too many rows in file {1}") % src_full_name).str(APPLOCALE));
				return retval;
			}

			char records_buf[32];
			sprintf(records_buf, "%d", records);

			const char *in[2] = { head_str.c_str(), records_buf };
			if ((*hchk_func)(NULL, NULL, in, NULL) != 0) {
				api_mgr.write_log(__FILE__, __LINE__, (_("ERROR: Can't pass rule_hcheck for {1}") % src_full_name).str(APPLOCALE));
				return retval;
			}

			is->seekg(0, std::ios_base::beg);
			if (!*is) {
				api_mgr.write_log(__FILE__, __LINE__, (_("ERROR: Can't seek source file {1}, {2}") % src_full_name % strerror(errno)).str(APPLOCALE));
				return retval;
			}
		}

		const vector<global_field_t>& global_fields = SAP->_SAP_global.global_fields;
		for (int i = FIXED_GLOBAL_SIZE; i < global_fields.size(); i++) {
			const string& default_value = global_fields[i].default_value;

			memcpy(SAT->_SAT_global[i], default_value.c_str(), default_value.length() + 1);
		}
	}

	// 回调函数
	post_open();

	retval = true;
	return retval;
}

void sa_istream::flush(bool completed)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(600, __PRETTY_FUNCTION__, (_("completed={1}") % completed).str(APPLOCALE), NULL);
#endif

	is->clear();
	master.break_point = is->tellg();
	master.error_type = (master.record_error == 0 ? ERROR_TYPE_SUCCESS : ERROR_TYPE_PARTIAL);
	master.completed = (completed ? 2 : 0);
}

void sa_istream::close()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(600, __PRETTY_FUNCTION__, "", NULL);
#endif

	if (is == NULL)
		return;

	master.completed = 1;
	common::close(*is, STREAM_TYPE_IFSTREAM);
	rename();

	SAT->_SAT_file_log.file_count++;
	SAT->_SAT_file_log.file_normal++;
}

void sa_istream::complete()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(600, __PRETTY_FUNCTION__, "", NULL);
#endif

	master.completed = 1;
	rename();

	SAT->_SAT_file_log.file_count++;
	SAT->_SAT_file_log.file_normal++;
}

void sa_istream::clean()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(600, __PRETTY_FUNCTION__, "", NULL);
#endif

	if (is == NULL)
		return;

	is->clear();
	master.break_point = is->tellg();
	master.error_type = ERROR_TYPE_FATAL;
	master.completed = 1;

	common::close(*is, STREAM_TYPE_IFSTREAM);

	string ftl_full_name = ftl_dir + src_name;
	if (sftp_prefix.empty()) {
		(void)common::rename(target_full_name.c_str(), ftl_full_name.c_str());
	} else {
		(void)::remove(local_full_name.c_str());

		file_rpc& rpc_mgr = file_rpc::instance(SGT);
		(void)rpc_mgr.rename(sftp_prefix, target_full_name.c_str(), ftl_full_name);
	}

	SAT->_SAT_file_log.file_count++;
	SAT->_SAT_file_log.file_fatal++;
}

void sa_istream::recover()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(600, __PRETTY_FUNCTION__, "", NULL);
#endif

	if (is == NULL || *is == NULL)
		return;

	common::close(*is, STREAM_TYPE_IFSTREAM);
	// 把文件放入重做列表
	redo_files[src_name] = target_full_name.substr(tmp_dir.length());
}

bool sa_istream::validate()
{
	bool retval = false;
#if defined(DEBUG)
	scoped_debug<bool> debug(600, __PRETTY_FUNCTION__, "", &retval);
#endif

	if (!file_correct)
		return retval;

	if (deal_flag != DEAL_FLAG_CHECK) {
		retval = true;
		return retval;
	}

	char **global = sa.get_global();
	const char **output = const_cast<const char **>(sa.get_output());

	if ((*chk_func)(NULL, global, output, 0) == 0) {
		retval = true;
		return retval;
	}

	return retval;
}

void sa_istream::rollback(const string& raw_file_name, int file_sn)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(100, __PRETTY_FUNCTION__, (_("raw_file_name={1}, file_sn={2}") % raw_file_name % file_sn).str(APPLOCALE), NULL);
#endif

	src_full_name = src_dir + raw_file_name;
	string ftl_full_name = ftl_dir + raw_file_name;
	string dup_full_name = dup_dir + raw_file_name;
	string bak_full_name;

	if (!zip_cmd.empty())
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Can't rollback since zip_cmd is set")).str(APPLOCALE));

	switch (bak_flag) {
	case BAK_FLAG_REMOVE:
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Can't rollback since bak_flag is REMOVE")).str(APPLOCALE));
	case BAK_FLAG_SUFFIX:
		bak_full_name = src_full_name + ".bak";
		break;
	case BAK_FLAG_RENAME:
		bak_full_name = bak_dir + raw_file_name;
		break;
	default:
		break;
	}

	if (sftp_prefix.empty()) {
		(void)common::rename(ftl_full_name.c_str(), src_full_name.c_str());
		(void)common::rename(dup_full_name.c_str(), src_full_name.c_str());
		if (!bak_full_name.empty())
			(void)common::rename(bak_full_name.c_str(), src_full_name.c_str());
	} else {
		file_rpc& rpc_mgr = file_rpc::instance(SGT);

		(void)rpc_mgr.rename(sftp_prefix, ftl_full_name, src_full_name);
		(void)rpc_mgr.rename(sftp_prefix, dup_full_name, src_full_name);
		if (!bak_full_name.empty())
			(void)rpc_mgr.rename(sftp_prefix, bak_full_name, src_full_name);
	}
}

void sa_istream::iclean()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(100, __PRETTY_FUNCTION__, "", NULL);
#endif

	vector<string> files;
	file_rpc& rpc_mgr = file_rpc::instance(SGT);
	files.clear();

	if (sftp_prefix.empty()) {
		scan_file<> src_scan(src_dir, "^.*$");
		src_scan.get_files(files);

		BOOST_FOREACH(const string& filename, files) {
			string full_name = src_dir + filename;
			(void)unlink(full_name.c_str());
		}

		files.clear();

		scan_file<> tmp_scan(tmp_dir, "^.*$");
		tmp_scan.get_files(files);

		BOOST_FOREACH(const string& filename, files) {
			string full_name = tmp_dir + filename;
			(void)unlink(full_name.c_str());
		}
	} else {
		if (!rpc_mgr.dir(files, sftp_prefix, src_dir, "^.*$"))
			throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Can't read directory {1} on {2} - {3}") % src_dir % sftp_prefix % SGT->strerror()).str(APPLOCALE));

		BOOST_FOREACH(const string& filename, files) {
			string full_name = src_dir + filename;
			rpc_mgr.unlink(sftp_prefix, full_name);
		}

		files.clear();

		if (!rpc_mgr.dir(files, sftp_prefix, tmp_dir, "^.*$"))
			throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Can't read directory {1} on {2} - {3}") % tmp_dir % sftp_prefix % SGT->strerror()).str(APPLOCALE));

		BOOST_FOREACH(const string& filename, files) {
			string full_name = tmp_dir + filename;
			rpc_mgr.unlink(sftp_prefix, full_name);
		}

		files.clear();

		scan_file<> local_scan(local_dir, "^.*$");
		local_scan.get_files(files);

		BOOST_FOREACH(const string& filename, files) {
			string full_name = local_dir + filename;
			(void)unlink(full_name.c_str());
		}
	}
}

void sa_istream::global_rollback()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(100, __PRETTY_FUNCTION__, "", NULL);
#endif

	vector<string> files;
	file_rpc& rpc_mgr = file_rpc::instance(SGT);
	files.clear();

	if (sftp_prefix.empty()) {
		scan_file<> tmp_scan(tmp_dir, "^[0-9]+\\..*$");
		tmp_scan.get_files(files);

		BOOST_FOREACH(const string& tmp_name, files) {
			string full_tmp_name = tmp_dir + tmp_name;
			string::const_iterator siter;
			for (siter = tmp_name.begin(); siter != tmp_name.end(); ++siter) {
				char ch = *siter;

				if (ch < '0' || ch > '9')
					break;
			}

			string full_src_name = src_dir + string(siter + 1, tmp_name.end());
			(void)common::rename(full_tmp_name.c_str(), full_src_name.c_str());
		}
	} else {
		string tmp_pattern = (boost::format("^%1%\\.[0-9]+\\..*$") % lmid).str();
		if (!rpc_mgr.dir(files, sftp_prefix, tmp_dir, tmp_pattern))
			throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Can't read directory {1} on {2} - {3}") % tmp_dir % sftp_prefix % SGT->strerror()).str(APPLOCALE));

		BOOST_FOREACH(const string& tmp_name, files) {
			string full_tmp_name = tmp_dir + tmp_name;
			string filename = tmp_name.substr(lmid.length() + 1, tmp_name.length() - lmid.length() - 1);
			string::iterator siter;
			for (siter = filename.begin(); siter != filename.end(); ++siter) {
				char ch = *siter;

				if (ch < '0' || ch > '9')
					break;
			}

			string full_src_name = src_dir + string(siter + 1, filename.end());
			(void)rpc_mgr.rename(sftp_prefix, full_tmp_name.c_str(), full_src_name.c_str());
		}

		files.clear();

		scan_file<> local_scan(local_dir, "^.*$");
		local_scan.get_files(files);

		BOOST_FOREACH(const string& filename, files) {
			string full_name = local_dir + filename;
			(void)unlink(full_name.c_str());
		}
	}
}

bool sa_istream::get_file()
{
	bool retval = false;
#if defined(DEBUG)
	scoped_debug<bool> debug(600, __PRETTY_FUNCTION__, "", &retval);
#endif

	try {
		while (!SGP->_SGP_shutdown) {
			string orig_full_name;
			string local_tmp_name;

			if (!redo_files.empty()) {
				map<string, string>::iterator iter = redo_files.begin();

				src_name = iter->first;
				orig_full_name = tmp_dir + iter->second;
				local_tmp_name = local_dir + iter->second;
				redo_files.erase(iter);
			} else if (!sftp_prefix.empty()) {
				if (file_iter == files.end()) {
					file_rpc& rpc_mgr = file_rpc::instance(SGT);
					files.clear();

					if (!rpc_mgr.dir(files, sftp_prefix, src_dir, pattern)) {
						GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't read directory {1} on {2} - {3}") % src_dir % sftp_prefix % SGT->strerror()).str(APPLOCALE));
						file_iter = files.begin();
						return retval;
					}

					file_iter = files.begin();
					if (files.empty())
						return retval;
				}

				src_name = *file_iter++;
				orig_full_name = src_dir + src_name;
			} else {
				if (!fscan->get_file(src_name))
					return retval;

				orig_full_name = src_dir + src_name;
			}

			src_full_name = src_dir + src_name;
			target_full_name = tmp_dir + file_prefix + src_name;
			local_full_name = local_dir + file_prefix + src_name;

			if (sftp_prefix.empty()) {
				if (!common::rename(orig_full_name.c_str(), target_full_name.c_str())) {
					if (errno != ENOENT) {
						GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't rename {1} to {2} - {3}") % orig_full_name % target_full_name % strerror(errno)).str(APPLOCALE));
						return retval;
					}

					continue;
				}
			} else {
				file_rpc& rpc_mgr = file_rpc::instance(SGT);
				if (!rpc_mgr.rename(sftp_prefix, orig_full_name, target_full_name))
					continue;

				if (!local_tmp_name.empty())
					(void)::remove(local_tmp_name.c_str());

				if (!rpc_mgr.get(sftp_prefix, target_full_name, local_full_name)) {
					GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't get file {1} on {2} - {3}") % target_full_name % sftp_prefix % SGT->strerror()).str(APPLOCALE));
					continue;
				}
			}

			retval = true;
			return retval;
		}

		return retval;
	} catch (exception& ex) {
		GPP->write_log(__FILE__, __LINE__, ex.what());
		return retval;
	}
}

void sa_istream::get_redo_files()
{
	if (!sftp_prefix.empty()) {
		// 获取临时目录下的文件
		string tmp_pattern = (boost::format("^%1%\\.[0-9]+\\..*$") % lmid).str();
		file_rpc& rpc_mgr = file_rpc::instance(SGT);
		vector<string> files;

		if (!rpc_mgr.dir(files, sftp_prefix, tmp_dir, tmp_pattern)) {
			GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't read directory {1} on {2} - {3}") % tmp_dir % sftp_prefix % SGT->strerror()).str(APPLOCALE));
			return;
		}

		for (vector<string>::const_iterator iter = files.begin(); iter != files.end(); ++iter) {
			string tmp_name = *iter;
			string filename = tmp_name.substr(lmid.length() + 1, tmp_name.length() - lmid.length() - 1);
			pid_t pid = 0;
			string::iterator siter;
			for (siter = filename.begin(); siter != filename.end(); ++siter) {
				char ch = *siter;

				if (ch < '0' || ch > '9')
					break;

				pid = pid * 10 + (ch - '0');
			}

			// 处理该文件的进程正常
			if (pid == 0 || kill(pid, 0) != -1)
				continue;

			if (siter == filename.end() || *siter != '.')
				continue;

			filename = string(siter + 1, filename.end());
			redo_files[filename] = tmp_name;
		}
	} else {
		// 检查临时目录
		for (bf::directory_iterator iter = bf::directory_iterator(tmp_dir); iter != bf::directory_iterator(); ++iter) {
			const bf::path& path = iter->path();

			if (!bf::is_regular_file(path))
				continue;

			string tmp_name = path.filename().native();
			pid_t pid = 0;
			string::iterator siter;
			for (siter = tmp_name.begin(); siter != tmp_name.end(); ++siter) {
				char ch = *siter;

				if (ch < '0' || ch > '9')
					break;

				pid = pid * 10 + (ch - '0');
			}

			// 处理该文件的进程正常
			if (pid == 0 || kill(pid, 0) != -1)
				continue;

			if (siter == tmp_name.end() || *siter != '.')
				continue;

			string filename = string(siter + 1, tmp_name.end());
			redo_files[filename] = tmp_name;
		}
	}
}

void sa_istream::rename()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(600, __PRETTY_FUNCTION__, "", NULL);
#endif

	if (!sftp_prefix.empty()) {
		if (::remove(local_full_name.c_str()) == -1)
			api_mgr.write_log(__FILE__, __LINE__, (_("ERROR: Can't remove {1} - {2}") % target_full_name % strerror(errno)).str(APPLOCALE));
	}

	switch (bak_flag) {
	case BAK_FLAG_REMOVE:
		if (sftp_prefix.empty()) {
			if (::remove(target_full_name.c_str()) == -1) {
				api_mgr.write_log(__FILE__, __LINE__, (_("ERROR: Can't remove {1} - {2}") % target_full_name % strerror(errno)).str(APPLOCALE));
				return;
			}
		} else {
			file_rpc& rpc_mgr = file_rpc::instance(SGT);
			if (rpc_mgr.unlink(sftp_prefix, target_full_name) != 0) {
				api_mgr.write_log(__FILE__, __LINE__, (_("ERROR: Can't remove {1} on {2} - {3}") % target_full_name % sftp_prefix % SGT->strerror()).str(APPLOCALE));
				return;
			}
		}
		break;
	case BAK_FLAG_SUFFIX:
		if (sftp_prefix.empty()) {
			string bak_full_name = src_full_name + ".bak";
			if (!common::rename(target_full_name.c_str(), bak_full_name.c_str())) {
				api_mgr.write_log(__FILE__, __LINE__, (_("ERROR: Can't rename {1} to {2} - {3}") % target_full_name % bak_full_name % strerror(errno)).str(APPLOCALE));
				return;
			}

			if (!zip_cmd.empty()) {
				sys_func& func_mgr = sys_func::instance();
				string cmd_string = zip_cmd + " " + bak_full_name;
				if (func_mgr.gp_status(::system(cmd_string.c_str()))) {
					api_mgr.write_log(__FILE__, __LINE__, (_("ERROR: Can't execute {1} - {2}") % cmd_string % strerror(errno)).str(APPLOCALE));
					return;
				}
			}
		} else {
			file_rpc& rpc_mgr = file_rpc::instance(SGT);
			string bak_full_name = local_full_name + ".bak";
			if (!rpc_mgr.rename(sftp_prefix, target_full_name, bak_full_name)) {
				api_mgr.write_log(__FILE__, __LINE__, (_("ERROR: Can't rename {1} to {2} on {3} - {4}") % target_full_name % bak_full_name % sftp_prefix % SGT->strerror()).str(APPLOCALE));
				return;
			}
		}
		break;
	case BAK_FLAG_RENAME:
		if (sftp_prefix.empty()) {
			string bak_full_name = bak_dir + src_name;
			if (!common::rename(target_full_name.c_str(), bak_full_name.c_str())) {
				api_mgr.write_log(__FILE__, __LINE__, (_("ERROR: Can't rename {1} to {2} - {3}") % target_full_name % bak_full_name % strerror(errno)).str(APPLOCALE));
				return;
			}

			if (!zip_cmd.empty()) {
				sys_func& func_mgr = sys_func::instance();
				string cmd_string = zip_cmd + " " + bak_full_name;
				if (func_mgr.gp_status(::system(cmd_string.c_str()))) {
					api_mgr.write_log(__FILE__, __LINE__, (_("ERROR: Can't execute {1} - {2}") % cmd_string % strerror(errno)).str(APPLOCALE));
					return;
				}
			}
		} else {
			file_rpc& rpc_mgr = file_rpc::instance(SGT);
			string bak_full_name = bak_dir + src_name;
			if (!rpc_mgr.rename(sftp_prefix, target_full_name, bak_full_name)) {
				api_mgr.write_log(__FILE__, __LINE__, (_("ERROR: Can't rename {1} to {2} on {3} - {4}") % target_full_name % bak_full_name % sftp_prefix % SGT->strerror()).str(APPLOCALE));
				return;
			}
		}
		break;
	default:
		break;
	}
}

void sa_istream::post_open()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(600, __PRETTY_FUNCTION__, "", NULL);
#endif
}

}
}

