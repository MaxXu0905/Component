#include "sa_internal.h"
#include "schd_rpc.h"

namespace ai
{
namespace app
{

using namespace ai::sg;
using namespace ai::scci;

boost::thread_specific_ptr<sat_ctx> sat_ctx::instance_;

sat_ctx::sat_ctx()
{
	_SAT_user_id = -1;
	_SAT_file_sn = -1;
	_SAT_last_pid = 0;

	// 设置日志数组
	sap_ctx *SAP = sap_ctx::instance();
	_SAT_dblog.resize(SAP->_SAP_adaptors.size());

	_SAT_global = NULL;
	_SAT_error_pos = -1;
	_SAT_enable_err_info = false;
	_SAT_db = NULL;

	for (int i = 0; i < SA_TOTAL_MANAGER; i++)
		_SAT_mgr_array[i] = NULL;
}

sat_ctx::~sat_ctx()
{
	// ORACLE等第三方软件退出时有问题，导致core dump，这里忽略该错误
	signal(SIGABRT, on_exit);

	for (int i = 0; i < SA_TOTAL_MANAGER; i++) {
		if (_SAT_mgr_array[i] != NULL) {
			delete _SAT_mgr_array[i];
			_SAT_mgr_array[i] = NULL;
		}
	}

	BOOST_FOREACH(sa_base *sa, _SAT_sas) {
		delete sa;
	}
	_SAT_sas.clear();

	delete _SAT_db;
	_SAT_db = NULL;

	sap_ctx *SAP = sap_ctx::instance();
	if (_SAT_global != NULL) {
		const vector<global_field_t>& global_fields = SAP->_SAP_global.global_fields;

		for (int i = 0; i < global_fields.size(); i++)
			delete []_SAT_global[i];

		delete []_SAT_global;
		_SAT_global = NULL;
	}
}

sat_ctx * sat_ctx::instance()
{
	sat_ctx *SAT = instance_.get();
	if (SAT == NULL) {
		SAT = new sat_ctx();
		instance_.reset(SAT);
	}
	return SAT;
}

void sat_ctx::clear()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	BOOST_FOREACH(dblog_item_t& item, _SAT_dblog) {
		item.clear();
	}
}

bool sat_ctx::validate()
{
	bool retval = false;
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", &retval);
#endif
	gpp_ctx *GPP = gpp_ctx::instance();

	for (int i = 0; i < _SAT_sas.size() - 1; i++) {
		sa_base *curr_sa = _SAT_sas[i];
		sa_base *next_sa = _SAT_sas[i + 1];
		const sa_adaptor_t& curr_adaptor = curr_sa->get_adaptor();
		const sa_adaptor_t& next_adaptor = next_sa->get_adaptor();

		if (curr_sa->transfer_input()) {
			const vector<int>& curr_input_sizes = curr_adaptor.input_sizes;
			const vector<int>& next_input_sizes = next_adaptor.input_sizes;

			if (curr_input_sizes != next_input_sizes) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: SA(sa_id={1}, com_key={2}, svc_key={3})'s "
					"input array size doesn't match SA(sa_id={4}, com_key={5}, svc_key={6})'s input array size")
					% curr_sa->get_id() % curr_sa->get_com_key() % curr_sa->get_svc_key()
					% next_sa->get_id() % next_sa->get_com_key() % next_sa->get_svc_key()).str(APPLOCALE));
				return retval;
			}
		} else {
			const vector<output_field_t>& next_output_fields = curr_adaptor.output_fields;
			const vector<int>& next_input_sizes = next_adaptor.input_sizes;

			bool error_flag = false;
			if (next_output_fields.size() != next_input_sizes.size()) {
				error_flag = true;
			} else {
				for (int j = 0; j < next_output_fields.size(); j++) {
					if (next_output_fields[j].field_size != next_input_sizes[j]) {
						error_flag = true;
						break;
					}
				}
			}

			if (error_flag) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: SA(sa_id={1}, com_key={2}, svc_key={3})'s "
					"output array size doesn't match SA(sa_id={4}, com_key={5}, svc_key={6})'s input array size")
					% curr_sa->get_id() % curr_sa->get_com_key() % curr_sa->get_svc_key()
					% next_sa->get_id() % next_sa->get_com_key() % next_sa->get_svc_key()).str(APPLOCALE));
				return retval;
			}
		}
	}

	init_global();

	retval = true;
	return retval;
}

void sat_ctx::on_save(bool do_report)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, (_("do_report={1}") % do_report).str(APPLOCALE), NULL);
#endif

	BOOST_FOREACH(sa_base *sa, _SAT_sas) {
		sa->on_save();
	}

	// Save log
	dblog_manager& dblog_mgr = dblog_manager::instance(this);
	dblog_mgr.update();

	_SAT_db->commit();

	// 保存全局变量
	sa_global& global_mgr = sa_global::instance(this);
	global_mgr.save();

	//if (do_report)
	//	report_info();
}

void sat_ctx::on_fatal()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	BOOST_FOREACH(sa_base *sa, _SAT_sas) {
		sa->on_fatal();
	}

	// Save log
	dblog_manager& dblog_mgr = dblog_manager::instance(this);
	dblog_mgr.update();

	_SAT_db->commit();
	report_info();
}

void sat_ctx::on_eof()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	BOOST_FOREACH(sa_base *sa, _SAT_sas) {
		sa->on_eof(0);
	}

	// 更新日志为待完成
	dblog_manager& dblog_mgr = dblog_manager::instance(this);
	dblog_mgr.update();
	_SAT_db->commit();

	//report_info();

	// 需要按相反顺序执行结束动作
	for (std::vector<sa_base *>::reverse_iterator riter = _SAT_sas.rbegin(); riter != _SAT_sas.rend(); ++riter) {
		(*riter)->on_eof(1);
	}

	// 更新日志为已完成
	dblog_mgr.complete();
	_SAT_db->commit();

	report_info();

	_SAT_file_log.record_count += _SAT_dblog[0].master.record_count;
	_SAT_file_log.record_normal += (*_SAT_dblog.rbegin()).master.record_normal;
	for (int i = 0; i < _SAT_dblog.size(); i++) {
		const master_dblog_t& master = _SAT_dblog[i].master;

		_SAT_file_log.record_invalid += master.record_invalid;
		_SAT_file_log.record_error += master.record_error;
	}
}

void sat_ctx::on_complete()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	// 需要按相反顺序执行结束动作
	for (std::vector<sa_base *>::reverse_iterator riter = _SAT_sas.rbegin(); riter != _SAT_sas.rend(); ++riter) {
		(*riter)->on_complete();
	}

	// 更新日志为已完成
	dblog_manager& dblog_mgr = dblog_manager::instance(this);
	dblog_mgr.complete();
	_SAT_db->commit();

	report_info();

	_SAT_file_log.record_count += _SAT_dblog[0].master.record_count;
	_SAT_file_log.record_normal += (*_SAT_dblog.rbegin()).master.record_normal;
	for (int i = 0; i < _SAT_dblog.size(); i++) {
		const master_dblog_t& master = _SAT_dblog[i].master;

		_SAT_file_log.record_invalid += master.record_invalid;
		_SAT_file_log.record_error += master.record_error;
	}
}

void sat_ctx::db_rollback()
{
	_SAT_db->rollback();
}

void sat_ctx::on_recover()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	BOOST_FOREACH(sa_base *sa, _SAT_sas) {
		sa->on_recover();
	}

	for (int i = 0; i < _SAT_dblog.size(); i++) {
		master_dblog_t& master = _SAT_dblog[i].master;

		master.record_count = 0;
		master.record_normal = 0;
		master.record_invalid = 0;
		master.record_error = 0;
		master.completed = 0;
	}

	report_info();

	try {
		_SAT_db->rollback();
	} catch (exception& ex) {
	}

	// 重新连接数据库
	delete _SAT_db;
	_SAT_db = NULL;

	try {
		map<string, string> conn_info;
		sap_ctx *SAP = sap_ctx::instance();

		database_factory& factory_mgr = database_factory::instance();
		factory_mgr.parse(SAP->_SAP_resource.openinfo, conn_info);

		_SAT_db = factory_mgr.create(SAP->_SAP_resource.dbname);
		_SAT_db->connect(conn_info);
	} catch (exception& ex) {
		delete _SAT_db;
		_SAT_db = NULL;
		throw bad_sql(__FILE__, __LINE__, SGEOS, 0, ex.what());
	}
}

void sat_ctx::init_global()
{
	sap_ctx *SAP = sap_ctx::instance();
	const vector<global_field_t>& global_fields = SAP->_SAP_global.global_fields;

	_SAT_global = new char *[global_fields.size()];

	for (int i = 0; i < global_fields.size(); i++) {
		_SAT_global[i] = new char[global_fields[i].field_size + 1];
		_SAT_global[i][0] = '\0';
	}
}

// 发送进度报告
void sat_ctx::report_info()
{
	schd_ctx *SCHD = schd_ctx::instance();
	if (!SCHD->enable_service())
		return;

	schd_rpc& rpc_mgr = schd_rpc::instance();
	sap_ctx *SAP = sap_ctx::instance();
	gpp_ctx *GPP = gpp_ctx::instance();
	bpt::iptree opt;
	bpt::iptree v_pt;
	ostringstream os;

	v_pt.put("type", SCHD_RPLY_PROC_PROGRESS);
	v_pt.put("hostname", GPP->uname());
	v_pt.put("pid", SAP->_SAP_pid);

	v_pt.put("file_count", _SAT_file_log.file_count);
	v_pt.put("file_normal", _SAT_file_log.file_normal);
	v_pt.put("file_fatal", _SAT_file_log.file_fatal);
	v_pt.put("file_dup", _SAT_file_log.file_dup);

	v_pt.put("file_name", _SAT_raw_file_name);
	v_pt.put("login_time", _SAT_login_time);
	v_pt.put("finish_time", _SAT_finish_time);

	long record_count = _SAT_file_log.record_count + _SAT_dblog[0].master.record_count;
	const int& completed = _SAT_dblog[0].master.completed;
	long record_normal = _SAT_file_log.record_normal + (*_SAT_dblog.rbegin()).master.record_normal;

	long record_invalid = _SAT_file_log.record_invalid;
	long record_error = _SAT_file_log.record_error;
	for (int i = 0; i < _SAT_dblog.size(); i++) {
		const master_dblog_t& master = _SAT_dblog[i].master;

		record_invalid += master.record_invalid;
		record_error += master.record_error;
	}

	v_pt.put("record_count", record_count);
	v_pt.put("record_normal", record_normal);
	v_pt.put("record_invalid", record_invalid);
	v_pt.put("record_error", record_error);
	v_pt.put("completed", completed);

	opt.add_child("info", v_pt);
	bpt::write_xml(os, opt);
	(void)rpc_mgr.report_info(os.str());
}

void sat_ctx::on_exit(int signo)
{
	_exit(0);
}

}
}

