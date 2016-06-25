#include "sa_internal.h"

namespace ai
{
namespace app
{

namespace bp = boost::posix_time;
using namespace ai::sg;
using namespace ai::scci;

sa_base::sa_base(int sa_id_)
	: sa_id(sa_id_),
	  adaptor(SAP->_SAP_adaptors[sa_id]),
	  master(SAT->_SAT_dblog[sa_id].master)
{
	isrc_mgr = NULL;
	otgt_mgr = NULL;
	oinv_mgr = NULL;
	oerr_mgr = NULL;
	osummary_mgr = NULL;
	odistr_mgr = NULL;
	ostat_mgr = NULL;
	inputs = NULL;
	input_serials = NULL;
	output = NULL;
	input2_size = 0;
	inputs2 = NULL;
	handles = NULL;
	snd_msgs = NULL;
	rcv_msgs = NULL;

	rqst = NULL;
	rqst_data = NULL;
	rply = NULL;
	rply_data = NULL;

	inited = false;
}

sa_base::~sa_base()
{
	destroy();
}

void sa_base::init()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	if (inited)
		return;

	// 调用初始化函数前的自定义函数，例如:
	// 查重组件需要先调整输出配置
	pre_init();

	batch = adaptor.parms.batch;
	if (support_batch()) {
		batch = (batch < 1 ? 1 : batch);
	} else if (batch != 1) {
		GPP->write_log((_("WARN: Operation Adaptor {1} doesn't support batch mode, reset batch to 1") % get_com_key()).str(APPLOCALE));
		batch = 1;
	}

	concurrency = adaptor.parms.concurrency;
	if (support_concurrency()) {
		if (batch > 1 && concurrency != 1) {
			GPP->write_log((_("WARN: Operation Adaptor {1} batch mode on, reset concurrency to 1") % get_com_key()).str(APPLOCALE));
			concurrency = 1;
		}
	} else if (concurrency != 1) {
		GPP->write_log((_("WARN: Operation Adaptor {1} doesn't support concurrency mode, reset to 1") % get_com_key()).str(APPLOCALE));
		concurrency = 1;
	}

	per_call = std::max(batch, concurrency);
	svc_name = (boost::format("%1%_%2%") % adaptor.parms.com_key % adaptor.parms.svc_key).str();

	isrc_mgr = SAP->sa_input_factory(*this, 0, adaptor.source.class_name);
	otgt_mgr = SAP->sa_output_factory(*this, OTYPE_TARGET, adaptor.target.class_name);
	oinv_mgr = SAP->sa_output_factory(*this, OTYPE_INVALID, adaptor.invalid.class_name);
	oerr_mgr = SAP->sa_output_factory(*this, OTYPE_ERROR, adaptor.error.class_name);
	osummary_mgr = SAP->sa_output_factory(*this, OTYPE_SUMMARY, adaptor.summary.class_name);
	odistr_mgr = SAP->sa_output_factory(*this, OTYPE_DISTR, adaptor.distr.class_name);
	ostat_mgr = SAP->sa_output_factory(*this, OTYPE_STAT, adaptor.stat.class_name);

	execute_count = 0;
	sndmsg_count = 0;
	status = 0;

	int i;
	const vector<global_field_t>& global_fields = SAP->_SAP_global.global_fields;
	const vector<int>& input_globals = adaptor.input_globals;
	const vector<int>& output_globals = adaptor.output_globals;
	const vector<int>& input_sizes = adaptor.input_sizes;
	const vector<output_field_t>& output_fields = adaptor.output_fields;

	inputs = new char **[per_call];
	input_serials = new int[per_call];
	for (i = 0; i < per_call; i++) {
		inputs[i] = new char *[input_sizes.size()];
		input_serials[i] = 0;

		for (int j = 0; j < input_sizes.size(); j++) {
			const int& size = input_sizes[j];

			inputs[i][j] = new char[size + 1];
		}
	}

	if (!output_fields.empty()) {
		output = new char *[output_fields.size()];
		for (i = 0; i < output_fields.size(); i++) {
			const int& size = output_fields[i].field_size;

			output[i] = new char[size + 1];
		}
	} else {
		output = NULL;
	}

	input2_size = 0;
	inputs2 = NULL;

	{
		int global_size = input_globals.size();
		int global_dsize = 0;
		BOOST_FOREACH(const int& idx, input_globals) {
			const global_field_t& field = global_fields[idx];

			global_dsize += field.field_size + 1;
		}

		int input_size = input_sizes.size();
		int input_dsize = 0;
		BOOST_FOREACH(const int& size, input_sizes) {
			input_dsize += size + 1;
		}

		snd_len = sa_rqst_t::need(batch, global_size, global_dsize, input_size, input_dsize);
	}

	{
		int global_size = output_globals.size();
		int global_dsize = 0;
		BOOST_FOREACH(const int& idx, output_globals) {
			const global_field_t& field = global_fields[idx];

			global_dsize += field.field_size + 1;
		}

		int output_size = output_fields.size();
		int output_dsize = 0;
		BOOST_FOREACH(const output_field_t& field, output_fields) {
			output_dsize += field.field_size + 1;
		}

		rcv_len = sa_rply_t::need(batch, global_size, global_dsize, output_size, output_dsize);
	}

	if (concurrency > 1)
		handles = new int[concurrency];
	else
		handles = NULL;

	snd_msgs = new message_pointer[concurrency];
	rcv_msgs = new message_pointer[concurrency];
	for (int i = 0; i < concurrency; i++) {
		snd_msgs[i] = sg_message::create();
		snd_msgs[i]->reserve(snd_len);

		rcv_msgs[i] = sg_message::create();
		rcv_msgs[i]->reserve(rcv_len);
	}

	isrc_mgr->init();
	otgt_mgr->init();
	oinv_mgr->init();
	oerr_mgr->init();
	osummary_mgr->init();
	odistr_mgr->init();
	ostat_mgr->init();

	inited = true;

	// 调用初始化函数后的自定义函数
	post_init();
}

void sa_base::destroy()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif
	int i;

	if (!inited)
		return;

	// 调用销毁函数前的自定义函数
	pre_destroy();

	delete isrc_mgr;
	delete otgt_mgr;
	delete oinv_mgr;
	delete oerr_mgr;
	delete osummary_mgr;
	delete odistr_mgr;
	delete ostat_mgr;

	if (snd_msgs != NULL)
		delete []snd_msgs;
	if (rcv_msgs != NULL)
		delete []rcv_msgs;
	if (handles != NULL)
		delete []handles;

	if (inputs2 != NULL) {
		for (i = 0; i < per_call; i++) {
			for (int j = 0; j < input2_size; j++)
				delete []inputs2[i][j];
			delete []inputs2[i];
		}
		delete []inputs2;
		input2_size = 0;
	}

	const vector<int>& input_sizes = adaptor.input_sizes;
	const vector<output_field_t>& output_fields = adaptor.output_fields;

	if (output != NULL) {
		for (i = 0; i < output_fields.size(); i++)
			delete []output[i];
		delete []output;
	}

	if (input_serials != NULL)
		delete []input_serials;

	if (inputs != NULL) {
		for (i = 0; i < per_call; i++) {
			for (int j = 0; j < input_sizes.size(); j++)
				delete []inputs[i][j];
			delete []inputs[i];
		}
		delete []inputs;
	}

	inited = false;
	pass_validate = false;

	// 调用销毁函数后的自定义函数
	post_destroy();
}

void sa_base::run()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	// 打开文件失败的错误只允许记录一次
	bool disable_logging = false;
	sa_global& global_mgr = sa_global::instance(SAT);
	int batches = 0;

	while (!SGP->_SGP_shutdown) {
		try {
			if (!isrc_mgr->open())
				continue;
		} catch (exception& ex) {
			if (!disable_logging) {
				api_mgr.write_log(__FILE__, __LINE__, ex.what());
				disable_logging = true;
			}
			global_mgr.clean();
			sleep(1);
			continue;
		}

		// 文件打开之后的特定操作
		post_open();

		// 允许继续记录错误
		disable_logging = false;
		batches = 0;
		execute_count = 0;

		try {
			while (!SGP->_SGP_shutdown) {
				set_fixed_input();
				status = 0;

				// 如果是一个批次的开始，则清空
				if (execute_count == 0)
					SAT->_SAT_rstrs.clear();

				switch (isrc_mgr->read()) {
				case SA_SUCCESS:
					if (++execute_count == per_call) {
						if (!call())
							break;
					}
					goto NEXT_LABEL;
				case SA_INVALID:
					status = STATUS_PREFIX + SAT->_SAT_error_pos;
					oinv_mgr->write(execute_count);
					master.record_invalid++;
					goto NEXT_LABEL;
				case SA_ERROR:
					status = STATUS_PREFIX + SAT->_SAT_error_pos;
					oerr_mgr->write(execute_count);
					master.record_error++;
					goto NEXT_LABEL;
				case SA_SKIP:
					goto NEXT_LABEL;
				case SA_FATAL:
					SAT->on_fatal();
					break;
				case SA_EOF:
					SAT->on_eof();
					break;
				}

				break;

NEXT_LABEL:
				if (master.record_error > adaptor.source.max_error_records) {
					api_mgr.write_log(__FILE__, __LINE__, (_("ERROR: Too many error records")).str(APPLOCALE));
					SAT->on_fatal();
					break;
				}

				if ((++master.record_count % adaptor.source.per_records) == 0) {
					if (++batches == adaptor.source.per_report) {
						batches = 0;
						SAT->on_save(true);
					} else {
						SAT->on_save(false);
					}
				}

#if defined(DEBUG)
				if (master.record_count >= SAP->_SAP_record_no) {
					SGP->_SGP_shutdown = true;
					break;
				}
#endif
			}
		} catch (bad_sql& ex) {
			api_mgr.write_log(__FILE__, __LINE__, ex.what());
			SAT->db_rollback();
			exit(1);
		} catch (exception& ex) {
			api_mgr.write_log(__FILE__, __LINE__, ex.what());
			try {
				SAT->on_fatal();
			} catch (exception& ex) {
				api_mgr.write_log(__FILE__, __LINE__, ex.what());
				// 不要清理全局文件
				master.completed = 0;
			}
		}
	}

	// 如果已完成，则清除全局文件
	if (master.completed)
		global_mgr.clean();
}

void sa_base::post_run()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif
}

void sa_base::dump(ostream& os)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	otgt_mgr->dump(os);
	oinv_mgr->dump(os);
	oerr_mgr->dump(os);
	osummary_mgr->dump(os);
	odistr_mgr->dump(os);
	ostat_mgr->dump(os);
}

void sa_base::rollback()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	// Rollback SA specific job
	pre_rollback();

	gpenv& env_mgr = gpenv::instance();
	string appdir = env_mgr.getenv("APPROOT");
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

	string sql_str = (boost::format("select raw_file_name{char[%1%]}, file_sn{int}, record_count{int}, "
		"record_normal{int}, record_invalid{int}, record_error{int}, break_point{long}, break_normal{long}, "
		"break_invalid{long}, break_error{long}, login_time{datetime}, finish_time{datetime}, file_time{datetime}, "
		"error_type{int}, completed{int}, pid{int} from log_integrator "
		"where integrator_name = :integrator_name{char[%2%]} and sa_id = :sa_id{int} and hostname=:hostname{char[%3%]}")
		% RAW_FILE_NAME_LEN % INTEGRATE_NAME_LEN % MAX_IDENT).str();
	data->setSQL(sql_str);
	stmt->bind(data);

	sql_str = (boost::format("insert into log_integrator_history(undo_id, integrator_name, sa_id, raw_file_name, "
		"file_sn, record_count, record_normal, record_invalid, record_error, break_point, break_normal, "
		"break_invalid, break_error, login_time, finish_time, file_time, error_type, completed, hostname, pid) "
		"values(:undo_id{int}, :integrator_name{char[%1%]}, :sa_id{int}, :raw_file_name{char[%2%]}, :file_sn{int}, "
		":record_count{int}, :record_normal{int}, :record_invalid{int}, :record_error{int}, :break_point{long}, "
		":break_normal{long}, :break_invalid{long}, :break_error{long}, :login_time{datetime}, "
		":finish_time{datetime}, :file_time{datetime}, :error_type{int}, :completed{int}, :hostname{char[%3%]}, "
		":pid{int})")
		% INTEGRATE_NAME_LEN % RAW_FILE_NAME_LEN % MAX_IDENT).str();
	insert_data->setSQL(sql_str);
	insert_stmt->bind(insert_data);

	stmt->setString(1, SAP->_SAP_resource.integrator_name);
	stmt->setInt(2, sa_id);
	stmt->setString(3, SAT->_SAT_hostname);

	auto_ptr<Generic_ResultSet> auto_rset(stmt->executeQuery());
	Generic_ResultSet *rset = auto_rset.get();

	while (rset->next()) {
		string raw_file_name = rset->getString(1);
		int file_sn = rset->getInt(2);
		int record_count = rset->getInt(3);
		int record_normal = rset->getInt(4);
		int record_invalid = rset->getInt(5);
		int record_error = rset->getInt(6);
		long break_point = rset->getLong(7);
		long break_normal = rset->getLong(8);
		long break_invalid = rset->getLong(9);
		long break_error = rset->getLong(10);
		datetime login_time = rset->getDatetime(11);
		datetime finish_time = rset->getDatetime(12);
		datetime file_time = rset->getDatetime(13);
		int error_type = rset->getInt(14);
		int completed = rset->getInt(15);
		pid_t pid = static_cast<pid_t>(rset->getInt(16));

		isrc_mgr->rollback(raw_file_name, file_sn);
		otgt_mgr->rollback(raw_file_name, file_sn);
		oinv_mgr->rollback(raw_file_name, file_sn);
		oerr_mgr->rollback(raw_file_name, file_sn);
		odistr_mgr->rollback(raw_file_name, file_sn);
		ostat_mgr->rollback(raw_file_name, file_sn);

		string db_name = appdir + "/.Component/sa/global_" + boost::lexical_cast<string>(pid) + ".sqlite3";
		(void)unlink(db_name.c_str());

		if (insert_count)
			insert_stmt->addIteration();

		insert_stmt->setInt(1, SAP->_SAP_undo_id);
		insert_stmt->setString(2, SAP->_SAP_resource.integrator_name);
		insert_stmt->setInt(3, sa_id);
		insert_stmt->setString(4, raw_file_name);
		insert_stmt->setInt(5, file_sn);
		insert_stmt->setInt(6, record_count);
		insert_stmt->setInt(7, record_normal);
		insert_stmt->setInt(8, record_invalid);
		insert_stmt->setInt(9, record_error);
		insert_stmt->setLong(10, break_point);
		insert_stmt->setLong(11, break_normal);
		insert_stmt->setLong(12, break_invalid);
		insert_stmt->setLong(13, break_error);
		insert_stmt->setDatetime(14, login_time);
		insert_stmt->setDatetime(15, finish_time);
		insert_stmt->setDatetime(16, file_time);
		insert_stmt->setInt(17, error_type);
		insert_stmt->setInt(18, completed);
		insert_stmt->setString(19, SAT->_SAT_hostname);
		insert_stmt->setInt(20, static_cast<int>(pid));

		if (++insert_count == insert_data->size()) {
			insert_stmt->executeUpdate();
			insert_count = 0;
		}
	}

	if (insert_count > 0)
		insert_stmt->executeUpdate();

	sql_str = (boost::format("delete from log_integrator "
		"where integrator_name = :integrator_name{char[%1%]} and sa_id = :sa_id{int} "
		"and hostname = :hostname{char[%2%]}")
		% INTEGRATE_NAME_LEN % MAX_INT).str();

	data->setSQL(sql_str);
	stmt->bind(data);

	stmt->setString(1, SAP->_SAP_resource.integrator_name);
	stmt->setInt(2, sa_id);
	stmt->setString(3, SAT->_SAT_hostname);
	stmt->executeUpdate();

	isrc_mgr->global_rollback();
	if (!SAP->_SAP_nclean) {
		otgt_mgr->global_rollback();
		oinv_mgr->global_rollback();
		oerr_mgr->global_rollback();
		odistr_mgr->global_rollback();
		ostat_mgr->global_rollback();
	}
}

void sa_base::iclean()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	isrc_mgr->iclean();
}

bool sa_base::call()
{
	bool retval = false;
	int i;
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", &retval);
#endif

	if (execute_count == 0) {
		retval = true;
		return retval;
	}

	sa_base *_this = this;
	BOOST_SCOPE_EXIT((&_this)) {
		_this->execute_count = 0;
	} BOOST_SCOPE_EXIT_END

	// 如果业务键为空，则不需要调用
	string& svc_key = adaptor.parms.svc_key;
	if (svc_key.empty()) {
		for (i = 0; i < execute_count; i++)
			handle_record(i);

		retval = true;
		return retval;
	}

	// 打包前需要设置svc_key
	memcpy(SAT->_SAT_global[GLOBAL_SVC_KEY_SERIAL], svc_key.c_str(), svc_key.length() + 1);

	// 对数据进行打包
	packup();

	if (sndmsg_count > 1) { // We'll invoke acall/getrply concurrently.
		int logged_id = -1;

		for (i = 0; i < sndmsg_count; i++) {
			set_svcname(i);

			while (1) {
				handles[i] = api_mgr.acall(svc_name.c_str(), snd_msgs[i], 0);
				if (handles[i] != -1)
					break;

				if (logged_id != i) {
					api_mgr.write_log(__FILE__, __LINE__, (_("WARN: acall failed, op_name {1} - {2}") % svc_name % api_mgr.strerror()).str(APPLOCALE));
					logged_id = i;
				}

				if (!check_policy())
					return retval;
			}
		}

		for (i = 0; i < sndmsg_count; i++) {
			if (api_mgr.getrply(handles[i], rcv_msgs[i], 0))
				continue;

			set_svcname(i);
			api_mgr.write_log(__FILE__, __LINE__, (_("WARN: getrply failed, op_name {1}, urcode {2} - {3}") % svc_name % api_mgr.get_ur_code() % api_mgr.strerror()).str(APPLOCALE));

			// 忽略错误
			(void)api_mgr.cancel(handles[i]);

			bool call_logged = false;
			while (!api_mgr.call(svc_name.c_str(), snd_msgs[i], rcv_msgs[i], 0)) {
				if (!call_logged) {
					api_mgr.write_log(__FILE__, __LINE__, (_("WARN: call failed, op_name {1}, urcode {2} - {3}") % svc_name % api_mgr.get_ur_code() % api_mgr.strerror()).str(APPLOCALE));
					call_logged = true;
				}

				if (!check_policy())
					return retval;
			}
		}

		for (i = 0; i < sndmsg_count; i++) {
			if (!extract(rcv_msgs[i], i))
				return retval;
		}
	} else {
		// Request the service, waiting for a reply
		set_svcname(0);

		bool call_logged = false;
		while (!api_mgr.call(svc_name.c_str(), snd_msgs[0], rcv_msgs[0], 0)) {
			if (!call_logged) {
				api_mgr.write_log(__FILE__, __LINE__, (_("WARN: call failed, op_name {1}, urcode {2} - {3}") % svc_name % api_mgr.get_ur_code() % api_mgr.strerror()).str(APPLOCALE));
				call_logged = true;
			}

			if (!check_policy())
				return retval;
		}

		if (!extract(rcv_msgs[0], 0))
			return retval;
	}

	retval = true;
	return retval;
}

const std::string& sa_base::get_com_key() const
{
	return adaptor.parms.com_key;
}

const std::string& sa_base::get_svc_key() const
{
	return adaptor.parms.svc_key;
}

const int& sa_base::get_id() const
{
	return sa_id;
}

const sa_adaptor_t& sa_base::get_adaptor() const
{
	return adaptor;
}

sa_input * sa_base::get_sa_input()
{
	return isrc_mgr;
}

const int& sa_base::get_status() const
{
	return status;
}

const char ** sa_base::get_global() const
{
	return const_cast<const char **>(SAT->_SAT_global);
}

char ** sa_base::get_global()
{
	return SAT->_SAT_global;
}

const char *** sa_base::get_inputs() const
{
	return const_cast<const char ***>(inputs);
}

char *** sa_base::get_inputs()
{
	return inputs;
}

const char ** sa_base::get_input() const
{
	return const_cast<const char **>(inputs[execute_count]);
}

const char ** sa_base::get_input(int input_idx) const
{
	return const_cast<const char **>(inputs[input_idx]);
}

char ** sa_base::get_input()
{
	return inputs[execute_count];
}

char ** sa_base::get_input(int input_idx)
{
	return inputs[input_idx];
}

const int& sa_base::get_input_serial() const
{
	return input_serials[execute_count];
}

const int& sa_base::get_input_serial(int input_idx) const
{
	return input_serials[input_idx];
}

int& sa_base::get_input_serial()
{
	return input_serials[execute_count];
}

int& sa_base::get_input_serial(int input_idx)
{
	return input_serials[input_idx];
}

const char ** sa_base::get_output() const
{
	return const_cast<const char **>(output);
}

char ** sa_base::get_output()
{
	return output;
}

bool sa_base::transfer_input() const
{
	return false;
}

void sa_base::pre_init()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif
}

void sa_base::post_init()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif
}

void sa_base::pre_init2()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif
}

void sa_base::post_init2()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif
}

void sa_base::init2()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	pre_init2();

	isrc_mgr->init2();
	otgt_mgr->init2();
	oinv_mgr->init2();
	oerr_mgr->init2();
	osummary_mgr->init2();
	odistr_mgr->init2();
	ostat_mgr->init2();

	post_init2();
}

void sa_base::pre_destroy()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif
}

void sa_base::post_destroy()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif
}

void sa_base::set_svcname(int input_idx)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif
}

void sa_base::set_input2(int input_idx)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif
}

bool sa_base::transfer(int input_idx)
{
	bool retval = false;
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, (_("input_idx={1}") % input_idx).str(APPLOCALE), NULL);
#endif

	if (sa_id + 1 >= SAT->_SAT_sas.size()) {
		retval = true;
		return retval;
	}

	sa_base *next_sa = SAT->_SAT_sas[sa_id + 1];

	// 调整输入记录数
	next_sa->master.record_count++;

	// 只需交换地址即可
	if (transfer_input()) {
		// 该步的输入与下一步的输入一致
		std::swap(inputs[input_idx], next_sa->inputs[next_sa->execute_count]);
	} else {
		// 该步的输出与下一步的输入一致
		std::swap(output, next_sa->inputs[next_sa->execute_count]);
	}

	if (++next_sa->execute_count == next_sa->per_call) {
		retval = next_sa->call();
		return retval;
	}

	retval = true;
	return retval;
}

void sa_base::pre_rollback()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif
}

void sa_base::post_open()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif
}

void sa_base::on_save()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	call();

	otgt_mgr->flush(false);
	oinv_mgr->flush(false);
	oerr_mgr->flush(false);
	osummary_mgr->flush(false);
	odistr_mgr->flush(false);
	ostat_mgr->flush(false);
	isrc_mgr->flush(false);
}

void sa_base::on_fatal()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	execute_count = 0;
	otgt_mgr->clean();
	oinv_mgr->clean();
	oerr_mgr->clean();
	osummary_mgr->clean();
	odistr_mgr->clean();
	ostat_mgr->clean();
	isrc_mgr->clean();
}

void sa_base::on_eof(int stage)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, (_("stage={1}") % stage).str(APPLOCALE), NULL);
#endif

	if (stage == 0) {
		call();

		pass_validate = isrc_mgr->validate();
		if (!pass_validate) {
			otgt_mgr->clean();
			oinv_mgr->clean();
			oerr_mgr->clean();
			osummary_mgr->clean();
			odistr_mgr->clean();
			ostat_mgr->clean();
			isrc_mgr->clean();
		} else {
			otgt_mgr->flush(true);
			oinv_mgr->flush(true);
			oerr_mgr->flush(true);
			osummary_mgr->flush(true);
			odistr_mgr->flush(true);
			ostat_mgr->flush(true);
			isrc_mgr->flush(true);
		}
	} else {
		if (pass_validate) {
			otgt_mgr->close();
			oinv_mgr->close();
			oerr_mgr->close();
			osummary_mgr->close();
			odistr_mgr->close();
			ostat_mgr->close();
			isrc_mgr->close();
		}
	}
}

void sa_base::on_complete()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	otgt_mgr->complete();
	oinv_mgr->complete();
	oerr_mgr->complete();
	osummary_mgr->complete();
	odistr_mgr->complete();
	ostat_mgr->complete();
	isrc_mgr->complete();
}

void sa_base::on_recover()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	otgt_mgr->recover();
	oinv_mgr->recover();
	oerr_mgr->recover();
	osummary_mgr->recover();
	odistr_mgr->recover();
	ostat_mgr->recover();
	isrc_mgr->recover();
}

void sa_base::packup()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	if (concurrency > 1) {
		for (int i = 0; i < execute_count; i++) {
			packup_begin(*snd_msgs[i]);
			packup_global();
			packup_input(i);
			packup_end(*snd_msgs[i]);
		}
		sndmsg_count = execute_count;
	} else {
		packup_begin(*snd_msgs[0]);
		packup_global();

		for (int i = 0; i < execute_count; i++)
			packup_input(i);

		packup_end(*snd_msgs[0]);
		sndmsg_count = 1;
	}
}

// 开始打包
void sa_base::packup_begin(sg_message& msg)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	rqst = reinterpret_cast<sa_rqst_t *>(msg.data());
	strcpy(rqst->svc_key, adaptor.parms.svc_key.c_str());
	rqst->version = adaptor.parms.version;
	rqst->flags = 0;
	rqst->rows = 0;
	rqst->global_size = adaptor.input_globals.size();
	if (input2_size)
		rqst->input_size = input2_size;
	else
		rqst->input_size = adaptor.input_sizes.size();
	rqst->user_id = SAT->_SAT_user_id;
	rqst_data = reinterpret_cast<char *>(&rqst->placeholder);
}

// 打包全局变量
void sa_base::packup_global()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	const vector<int>& input_globals = adaptor.input_globals;
	int *global_sizes = reinterpret_cast<int *>(rqst_data);

	rqst_data = reinterpret_cast<char *>(global_sizes + rqst->global_size);
	for (int i = 0; i < input_globals.size(); i++) {
		const int& idx = input_globals[i];
		int data_size = strlen(SAT->_SAT_global[idx]) + 1;

		global_sizes[i] = data_size ;
		memcpy(rqst_data, SAT->_SAT_global[idx], data_size);
		rqst_data += data_size;
	}
}

// 打包输入变量
void sa_base::packup_input(int input_idx)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, (_("input_idx={1}") % input_idx).str(APPLOCALE), NULL);
#endif

	// 先按INT对齐
	rqst_data = reinterpret_cast<char *>(common::round_up(reinterpret_cast<long>(rqst_data), static_cast<long>(sizeof(int))));

	int *record_serial = reinterpret_cast<int *>(rqst_data);
	*record_serial = input_serials[input_idx];

	int *input_sizes = record_serial + 1;
	char **input;

	if (input2_size) {
		set_input2(input_idx);
		input = inputs2[input_idx];
	} else {
		input = inputs[input_idx];
	}

	rqst_data = reinterpret_cast<char *>(input_sizes + rqst->input_size);
	for (int i = 0; i < rqst->input_size; i++) {
		int data_size = strlen(input[i]) + 1;

		input_sizes[i] = data_size;
		memcpy(rqst_data, input[i], data_size);
		rqst_data += data_size;
	}

	rqst->rows++;
}

// 结束打包
void sa_base::packup_end(sg_message& msg)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	rqst->datalen = rqst_data - reinterpret_cast<char *>(rqst);
	msg.set_length(rqst->datalen);
}

// 解包
bool sa_base::extract(message_pointer& rcv_msg, int input_idx)
{
	bool retval = false;
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, (_("input_idx={1}") % input_idx).str(APPLOCALE), &retval);
#endif

	extract_begin(rcv_msg);
	extract_global();

	for (int i = 0; i < rply->rows; i++) {
		extract_output(i);
		if (!handle_record(input_idx + i))
			return retval;
	}

	extract_end();

	retval = true;
	return retval;
}

// 开始提取
void sa_base::extract_begin(message_pointer& rcv_msg)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	rply = reinterpret_cast<sa_rply_t *>(rcv_msg->data());
	results = &rply->placeholder;
	rply_data = reinterpret_cast<char *>(results + rply->rows);
}

// 提取全局变量
void sa_base::extract_global()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	const vector<int>& output_globals = adaptor.output_globals;
	int *global_sizes = reinterpret_cast<int *>(rply_data);

	if (rply->global_size != output_globals.size())
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: operation contract failure, global_size doesn't match.")).str(APPLOCALE));

	rply_data = reinterpret_cast<char *>(global_sizes + rply->global_size);
	for (int i = 0; i < rply->global_size; i++) {
		const int& idx = output_globals[i];
		int& data_size = global_sizes[i];

		memcpy(SAT->_SAT_global[idx], rply_data, data_size);
		rply_data += data_size;
	}
}

// 提取输出变量
void sa_base::extract_output(int input_idx)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, (_("input_idx={1}") % input_idx).str(APPLOCALE), NULL);
#endif

	// 先按INT对齐
	rply_data = reinterpret_cast<char *>(common::round_up(reinterpret_cast<long>(rply_data), static_cast<long>(sizeof(int))));

	const vector<output_field_t>& output_fields = adaptor.output_fields;
	int *output_sizes = reinterpret_cast<int *>(rply_data);

	if (rply->output_size > output_fields.size() - FIXED_INPUT_SIZE)
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: operation contract failure, output_size doesn't match.")).str(APPLOCALE));

	// 把输入中的固定字段拷贝到输出中
	char **input = get_input(input_idx);
	for (int i = 0; i < FIXED_INPUT_SIZE; i++)
		strcpy(output[i], input[i]);

	rply_data = reinterpret_cast<char *>(output_sizes + rply->output_size);
	for (int i = 0; i < rply->output_size; i++) {
		int& data_size = output_sizes[i];

		memcpy(output[i + FIXED_INPUT_SIZE], rply_data, data_size);
		rply_data += data_size;
	}

	// 设置状态
	status = *results++;
}

// 提取结束
void sa_base::extract_end()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif
}

bool sa_base::check_policy()
{
	bool retval = false;
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", &retval);
#endif

	if (SGP->_SGP_shutdown)
		return retval;

	switch (adaptor.parms.exception_policy) {
	case EXCEPTION_POLICY_RETRY:
		if (adaptor.parms.exception_waits > 0)
			boost::this_thread::sleep(bp::milliseconds(adaptor.parms.exception_waits));
		break;
	case EXCEPTION_POLICY_DONE:
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Encounter exception and done this file.")).str(APPLOCALE));
	case EXCEPTION_POLICY_EXIT:
		GPP->write_log(__FILE__, __LINE__, (_("ERROR: Fatal error encountered, exiting")).str(APPLOCALE));
		exit(1);
	default:
		break;
	}

	retval = true;
	return retval;
}

bool sa_base::handle_record(int input_idx)
{
	bool retval = false;
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, (_("input_idx={1}") % input_idx).str(APPLOCALE), &retval);
#endif

	if (status == 0) { // Correct record
		otgt_mgr->write(input_idx);
		if (odistr_mgr->write(input_idx) == 0 && adaptor.distr.dst_files.size() != 0) {
			status = -STATUS_INVALID;
			oinv_mgr->write(input_idx);
			status = 0;
		}
		ostat_mgr->write(input_idx);
		master.record_normal++;

		// 移交数据到下一步
		retval = transfer(input_idx);
		return retval;
	} else if (status > 0) { // Error record
		oerr_mgr->write(input_idx);
		master.record_error++;
	} else { // Invalid recrd
		oinv_mgr->write(input_idx);
		master.record_invalid++;
	}

	retval = true;
	return retval;
}

void sa_base::set_fixed_input()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	char **input = get_input();

	sprintf(input[INPUT_RECORD_SN_SERIAL], "%d", master.record_count + 1);
}

}
}

