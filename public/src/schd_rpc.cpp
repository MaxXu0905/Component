#include "schd_rpc.h"
#include "schd_policy.h"

using namespace ai::sg;

namespace ai
{
namespace app
{

const char * schd_rqst_t::data() const
{
	return reinterpret_cast<const char *>(&placeholder);
}

char * schd_rqst_t::data()
{
	return reinterpret_cast<char *>(&placeholder);
}

const char * schd_rply_t::data() const
{
	return reinterpret_cast<const char *>(&placeholder);
}

char * schd_rply_t::data()
{
	return reinterpret_cast<char *>(&placeholder);
}

schd_rpc schd_rpc::_instance;

schd_rpc& schd_rpc::instance()
{
	return _instance;
}

bool schd_rpc::do_init(int mid)
{
	bool retval = false;
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, (_("mid={1}") % mid).str(APPLOCALE), &retval);
#endif

	message_pointer rqst_msg = sg_message::create();
	message_pointer rply_msg = sg_message::create();
	int datalen = schd_rqst_t::need(0);
	rqst_msg->set_length(datalen);
	schd_rqst_t *rqst = reinterpret_cast<schd_rqst_t *>(rqst_msg->data());

	rqst->opcode = OSCHD_INIT;
	rqst->datalen = datalen;

	sgc_ctx *SGC = SGT->SGC();
	SGC->_SGC_special_mid = mid;
	BOOST_SCOPE_EXIT((&SGC)) {
		SGC->_SGC_special_mid = BADMID;
	} BOOST_SCOPE_EXIT_END

	if (!api_mgr.call(SCHD->_SCHD_svc_name.c_str(), rqst_msg, rply_msg, 0)) {
		GPP->write_log(__FILE__, __LINE__, (_("ERROR: Call operation {1} failed - {2}") % SCHD->_SCHD_svc_name % SGT->strerror()).str(APPLOCALE));
		return retval;
	}

	schd_rply_t *rply = reinterpret_cast<schd_rply_t *>(rply_msg->data());
	if (rply->rtn != 0) {
		GPP->write_log(__FILE__, __LINE__, (_("ERROR: Failed to init agent - {1}") % rply->rtn).str(APPLOCALE));
		return retval;
	}

	retval = true;
	return retval;
}

// 启动指定进程
bool schd_rpc::do_start(int mid, const string& entry_name, const string& hostname, const string& command)
{
	bool retval = false;
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, (_("mid={1}, entry_name={2}, hostname={3}, command={4}") % mid % entry_name % hostname % command).str(APPLOCALE), &retval);
#endif

	message_pointer rqst_msg = sg_message::create();
	message_pointer rply_msg = sg_message::create();
	int datalen = schd_rqst_t::need(entry_name.length() + hostname.length() + command.length() + 3);
	rqst_msg->set_length(datalen);
	schd_rqst_t *rqst = reinterpret_cast<schd_rqst_t *>(rqst_msg->data());

	rqst->opcode = OSCHD_START;
	rqst->datalen = datalen;

	char *data = rqst->data();
 	memcpy(data, entry_name.c_str(), entry_name.length() + 1);
	data += entry_name.length() + 1;
	memcpy(data, hostname.c_str(), hostname.length() + 1);
	data += hostname.length() + 1;
	memcpy(data, command.c_str(), command.length() + 1);

	sgc_ctx *SGC = SGT->SGC();
	SGC->_SGC_special_mid = mid;
	BOOST_SCOPE_EXIT((&SGC)) {
		SGC->_SGC_special_mid = BADMID;
	} BOOST_SCOPE_EXIT_END

	if (!api_mgr.call(SCHD->_SCHD_svc_name.c_str(), rqst_msg, rply_msg, 0)) {
		GPP->write_log(__FILE__, __LINE__, (_("ERROR: Call operation {1} failed - {2}") % SCHD->_SCHD_svc_name % SGT->strerror()).str(APPLOCALE));
		return retval;
	}

	schd_rply_t *rply = reinterpret_cast<schd_rply_t *>(rply_msg->data());
	if (rply->rtn != 0) {
		GPP->write_log(__FILE__, __LINE__, (_("ERROR: Failed to start process {1} - {2}") % command % rply->rtn).str(APPLOCALE));
		return retval;
	}

	schd_agent_proc_t *agent_proc = reinterpret_cast<schd_agent_proc_t *>(rply->data());
	schd_proc_t item(mid, *agent_proc);
	SCHD->_SCHD_procs.insert(item);

	// 报告变更
	report(item);

	GPP->write_log(__FILE__, __LINE__, (_("INFO: Process {1} for entry_name {2} on {3} started successfully - {4}.") % agent_proc->pid % entry_name % hostname % command).str(APPLOCALE));
	retval = true;
	return retval;
}

// 主动停止进程
bool schd_rpc::do_stop(schd_entry_t& entry, schd_proc_t& proc)
{
	bool retval = false;
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, (_("mid={1}, pid={2}") % proc.mid % proc.pid).str(APPLOCALE), &retval);
#endif

	message_pointer rqst_msg = sg_message::create();
	message_pointer rply_msg = sg_message::create();
	int datalen = schd_rqst_t::need(sizeof(schd_stop_rqst_t));
	rqst_msg->set_length(datalen);
	schd_rqst_t *rqst = reinterpret_cast<schd_rqst_t *>(rqst_msg->data());

	rqst->opcode = OSCHD_STOP;
	rqst->datalen = datalen;

	schd_stop_rqst_t *data = reinterpret_cast<schd_stop_rqst_t *>(rqst->data());
	data->pid = proc.pid;
	data->start_time = proc.start_time;
	data->wait_time = SCHD->_SCHD_stop_timeout;
	data->signo = SCHD->_SCHD_stop_signo;

	sgc_ctx *SGC = SGT->SGC();
	SGC->_SGC_special_mid = proc.mid;
	BOOST_SCOPE_EXIT((&SGC)) {
		SGC->_SGC_special_mid = BADMID;
	} BOOST_SCOPE_EXIT_END

	if (!api_mgr.call(SCHD->_SCHD_svc_name.c_str(), rqst_msg, rply_msg, 0)) {
		GPP->write_log(__FILE__, __LINE__, (_("ERROR: Call operation {1} failed - {2}") % SCHD->_SCHD_svc_name % SGT->strerror()).str(APPLOCALE));
		return retval;
	}

	schd_rply_t *rply = reinterpret_cast<schd_rply_t *>(rply_msg->data());
	if (rply->rtn != 0) {
		GPP->write_log(__FILE__, __LINE__, (_("ERROR: Failed to stop process {1} - {2}") % proc.pid % rply->rtn).str(APPLOCALE));
		return retval;
	}

	schd_agent_proc_t *agent_proc = reinterpret_cast<schd_agent_proc_t *>(rply->data());
	proc.status = agent_proc->status;
	proc.stop_time = agent_proc->stop_time;

	SCHD->dec_proc(proc.hostname, proc.entry_name);
	if (--entry.processes == 0)
		--SCHD->_SCHD_running_entries;

	// 报告变更
	report(proc);

	GPP->write_log(__FILE__, __LINE__, (_("INFO: Process {1} for entry_name {2} on {3} stopped successfully.") % proc.pid % proc.entry_name % proc.hostname).str(APPLOCALE));
	retval = true;
	return retval;
}

// 获取指定节点的所有进程状态
bool schd_rpc::do_status(int mid)
{
	bool retval = false;
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, (_("mid={1}") % mid).str(APPLOCALE), &retval);
#endif

	message_pointer rqst_msg = sg_message::create();
	message_pointer rply_msg = sg_message::create();
	int datalen = schd_rqst_t::need(0);
	rqst_msg->set_length(datalen);
	schd_rqst_t *rqst = reinterpret_cast<schd_rqst_t *>(rqst_msg->data());

	rqst->opcode = OSCHD_STATUS;
	rqst->datalen = datalen;

	sgc_ctx *SGC = SGT->SGC();
	SGC->_SGC_special_mid = mid;
	BOOST_SCOPE_EXIT((&SGC)) {
		SGC->_SGC_special_mid = BADMID;
	} BOOST_SCOPE_EXIT_END

	if (!api_mgr.call(SCHD->_SCHD_svc_name.c_str(), rqst_msg, rply_msg, 0)) {
		GPP->write_log(__FILE__, __LINE__, (_("ERROR: Call operation {1} failed - {2}") % SCHD->_SCHD_svc_name % SGT->strerror()).str(APPLOCALE));
		return retval;
	}

	schd_rply_t *rply = reinterpret_cast<schd_rply_t *>(rply_msg->data());
	if (rply->rtn != 0) {
		GPP->write_log(__FILE__, __LINE__, (_("ERROR: Failed to do status - {1}") % rply->rtn).str(APPLOCALE));
		return retval;
	}

	schd_agent_proc_t *agent_proc = reinterpret_cast<schd_agent_proc_t *>(rply->data());
	for (int i = 0; i < rply->num; i++, agent_proc++) {
		schd_proc_t item(mid, *agent_proc);
		SCHD->_SCHD_procs.insert(item);

		map<string, schd_entry_t>::iterator iter = SCHD->_SCHD_entries.find(item.entry_name);
		if (iter == SCHD->_SCHD_entries.end()) {
			GPP->write_log(__FILE__, __LINE__, (_("FATAL: entry_name {1} not found in configuration") % item.entry_name).str(APPLOCALE));
			exit(1);
		}

		schd_entry_t& entry = iter->second;
		if (!(agent_proc->status & SCHD_FLAG_IGNORE)) {
			entry.status |= agent_proc->status;

			if (agent_proc->status == SCHD_RUNNING) {
				entry.processes++;
				SCHD->inc_proc(item.hostname, item.entry_name);
			}
		}
	}

	retval = true;
	return retval;
}

// 获取指定节点的进程状态，如果有变化，则返回true
bool schd_rpc::do_check(int mid)
{
	bool retval = false;
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, (_("mid={1}") % mid ).str(APPLOCALE), &retval);
#endif

	schd_policy& policy_mgr = schd_policy::instance();
	message_pointer rqst_msg = sg_message::create();
	message_pointer rply_msg = sg_message::create();
	int datalen = schd_rqst_t::need(0);
	rqst_msg->set_length(datalen);
	schd_rqst_t *rqst = reinterpret_cast<schd_rqst_t *>(rqst_msg->data());

	rqst->opcode = OSCHD_CHECK;
	rqst->datalen = datalen;

	sgc_ctx *SGC = SGT->SGC();
	SGC->_SGC_special_mid = mid;
	BOOST_SCOPE_EXIT((&SGC)) {
		SGC->_SGC_special_mid = BADMID;
	} BOOST_SCOPE_EXIT_END

	if (!api_mgr.call(SCHD->_SCHD_svc_name.c_str(), rqst_msg, rply_msg, 0)) {
		GPP->write_log(__FILE__, __LINE__, (_("ERROR: Call operation {1} failed - {2}") % SCHD->_SCHD_svc_name % SGT->strerror()).str(APPLOCALE));
		return retval;
	}

	schd_rply_t *rply = reinterpret_cast<schd_rply_t *>(rply_msg->data());
	if (rply->rtn != 0) {
		GPP->write_log(__FILE__, __LINE__, (_("ERROR: Failed to get status - {1}") % rply->rtn).str(APPLOCALE));
		return retval;
	}

	schd_agent_proc_t *agent_proc = reinterpret_cast<schd_agent_proc_t *>(rply->data());
	for (int i = 0; i < rply->num; i++, agent_proc++) {
		if (agent_proc->status & (SCHD_STOPPED | SCHD_FLAG_IGNORE))
			continue;

		schd_proc_t key;
		key.mid = mid;
		key.pid = agent_proc->pid;
		key.start_time = agent_proc->start_time;

		set<schd_proc_t>::iterator iter = SCHD->_SCHD_procs.find(key);
		if (iter == SCHD->_SCHD_procs.end())
			continue;

		if (iter->status == agent_proc->status)
			continue;

		// Copy it since the entry may be erased
		string entry_name = agent_proc->entry_name;
		schd_proc_t& proc = const_cast<schd_proc_t&>(*iter);
		proc.status = agent_proc->status;
		proc.stop_time = agent_proc->stop_time;

		map<string, schd_entry_t>::iterator entry_iter = SCHD->_SCHD_entries.find(entry_name);
		if (entry_iter == SCHD->_SCHD_entries.end()) {
			GPP->write_log(__FILE__, __LINE__, (_("FATAL: Can't find configuration for {1}") % entry_name).str(APPLOCALE));
			exit(1);
		}

		schd_entry_t& entry = entry_iter->second;
		retval = true;

		if (proc.status == SCHD_FINISH) {
			SCHD->dec_proc(agent_proc->hostname, entry_name);
			if (--entry.processes == 0) {
				entry.status &= ~(SCHD_INIT | SCHD_READY | SCHD_RUNNING);

				if (entry.status & SCHD_CLEANING) {
					if (entry.status & SCHD_ICLEAN) {
						entry.status &= ~SCHD_ICLEAN;
						entry.status |= SCHD_PENDING;
					} else {
						entry.status &= ~SCHD_CLEANING;
						if (!(entry.status & (SCHD_TERMINATED | SCHD_STOPPED))) {
							// 先设置为初始状态
							entry.status = SCHD_INIT;
							policy_mgr.save(entry_name, entry);

							// 删除远程进程
							do_erase(entry_name);
							continue;
						}
					}
				} else {
					--SCHD->_SCHD_running_entries;
				}
			}

			entry.status |= SCHD_FINISH;
		} else if (proc.status == SCHD_TERMINATED) {
			if (entry.retries > 0 && !(entry.status & SCHD_CLEANING)) {
				if (!do_start(mid, entry_name, proc.hostname, proc.command)) {
					// 有异常时，不允许再自动启动
					entry.status |= SCHD_TERMINATED;
					entry.retries = 0;
					SCHD->dec_proc(proc.hostname, proc.entry_name);
					if (--entry.processes == 0) {
						entry.status &= ~(SCHD_INIT | SCHD_READY | SCHD_RUNNING | SCHD_CLEANING);
						--SCHD->_SCHD_running_entries;
					}
				} else {
					--entry.retries;
				}
			} else {
				SCHD->dec_proc(proc.hostname, proc.entry_name);
				if (--entry.processes == 0) {
					entry.status &= ~(SCHD_INIT | SCHD_READY | SCHD_RUNNING | SCHD_CLEANING);
					--SCHD->_SCHD_running_entries;
				}

				// 有异常时，不允许再自动启动
				entry.status |= SCHD_TERMINATED;
				entry.retries = 0;
			}
		}

		policy_mgr.save(entry_name, entry);
		report(entry_name, entry);
		report(proc);
	}

	return retval;
}

bool schd_rpc::do_erase(const string& entry_name)
{
	bool retval = false;
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, (_("entry_name={1}") % entry_name).str(APPLOCALE), &retval);
#endif

	sgc_ctx *SGC = SGT->SGC();

	map<string, schd_cfg_entry_t>::const_iterator cfg_iter = SCHD->_SCHD_cfg_entries.find(entry_name);
	if (cfg_iter == SCHD->_SCHD_cfg_entries.end()) {
		GPP->write_log(__FILE__, __LINE__, (_("FATAL: Can't find configuration for {1}") % entry_name).str(APPLOCALE));
		exit(1);
	}

	const schd_cfg_entry_t& cfg_entry = cfg_iter->second;

	for (int i = 0; i < cfg_entry.mids.size(); i++) {
		int mid;

		if (cfg_entry.mids[i] == BADMID)
			mid = SGC->_SGC_proc.mid;
		else
			mid = cfg_entry.mids[i];

		if (!do_erase_internal(mid, entry_name)) {
			retval = false;
			return retval;
		}
	}

	// 删除节点相关的所有进程
	for (set<schd_proc_t>::iterator iter = SCHD->_SCHD_procs.begin(); iter != SCHD->_SCHD_procs.end();) {
		if (entry_name != iter->entry_name) {
			++iter;
			continue;
		}

		set<schd_proc_t>::iterator tmp_iter = iter++;
		SCHD->_SCHD_procs.erase(tmp_iter);
	}

	// 报告节点初始化
	schd_rpc& rpc_mgr = schd_rpc::instance();
	rpc_mgr.report(entry_name);

	retval = true;
	return retval;
}

bool schd_rpc::do_mark(const string& entry_name)
{
	bool retval = false;
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, (_("entry_name={1}") % entry_name).str(APPLOCALE), &retval);
#endif

	sgc_ctx *SGC = SGT->SGC();

	map<string, schd_cfg_entry_t>::const_iterator cfg_iter = SCHD->_SCHD_cfg_entries.find(entry_name);
	if (cfg_iter == SCHD->_SCHD_cfg_entries.end()) {
		GPP->write_log(__FILE__, __LINE__, (_("FATAL: Can't find configuration for {1}") % entry_name).str(APPLOCALE));
		exit(1);
	}

	const schd_cfg_entry_t& cfg_entry = cfg_iter->second;

	for (int i = 0; i < cfg_entry.mids.size(); i++) {
		int mid;

		if (cfg_entry.mids[i] == BADMID)
			mid = SGC->_SGC_proc.mid;
		else
			mid = cfg_entry.mids[i];

		if (!do_mark_internal(mid, entry_name)) {
			retval = false;
			return retval;
		}
	}

	// 删除节点相关的所有进程
	for (set<schd_proc_t>::iterator iter = SCHD->_SCHD_procs.begin(); iter != SCHD->_SCHD_procs.end();) {
		if (entry_name != iter->entry_name) {
			++iter;
			continue;
		}

		set<schd_proc_t>::iterator tmp_iter = iter++;
		SCHD->_SCHD_procs.erase(tmp_iter);
	}

	retval = true;
	return retval;
}


// 在多节点上执行输入清理操作
bool schd_rpc::do_iclean(const string& entry_name, schd_entry_t& entry, const schd_cfg_entry_t& cfg_entry)
{
	bool retval = false;
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, (_("entry_name={1}") % entry_name).str(APPLOCALE), &retval);
#endif

	sgc_ctx *SGC = SGT->SGC();

	for (int i = 0; i < cfg_entry.mids.size(); i++) {
		string command;
		int mid;
		const string& hostname = cfg_entry.hostnames[i];

		if (cfg_entry.mids[i] == BADMID) {
			command = "schd_agent";
			if (!cfg_entry.usrname.empty()) {
				command += " -u ";
				command += cfg_entry.usrname;
			}
			command += " -h ";
			command += hostname;
			command += " -e \"";
			command += cfg_entry.iclean;
			command += "\" -t ";
			command += boost::lexical_cast<string>(SCHD->_SCHD_polltime);
			mid = SGC->_SGC_proc.mid;
		} else if (cfg_entry.enable_iproxy) {
			command = "schd_agent -e \"";
			command += cfg_entry.iclean;
			command += "\" -t ";
			command += boost::lexical_cast<string>(SCHD->_SCHD_polltime);
			mid = cfg_entry.mids[i];
		} else {
			command = cfg_entry.iclean;
			mid = cfg_entry.mids[i];
		}

		if (!do_start(mid, entry_name, hostname, command)) {
			retval = false;
			return retval;
		} else {
			entry.processes++;
			SCHD->inc_proc(hostname, entry_name);
		}
	}

	retval = true;
	return retval;
}

// 在多节点上执行回退操作
bool schd_rpc::do_undo(const string& entry_name, schd_entry_t& entry, const schd_cfg_entry_t& cfg_entry, int undo_id)
{
	bool retval = false;
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, (_("entry_name={1}, undo_id={2}") % entry_name % undo_id).str(APPLOCALE), &retval);
#endif

	sgc_ctx *SGC = SGT->SGC();
	gpenv& env_mgr = gpenv::instance();

	for (int i = 0; i < cfg_entry.mids.size(); i++) {
		string command;
		int mid;
		const string& hostname = cfg_entry.hostnames[i];
		string real_command;
		string env_str = (boost::format("SGSCHDUNDOID=%1%") % undo_id).str();
		env_mgr.putenv(env_str.c_str());
		env_mgr.expand_var(real_command, cfg_entry.undo);

		if (cfg_entry.mids[i] == BADMID) {
			command = "schd_agent";
			if (!cfg_entry.usrname.empty()) {
				command += " -u ";
				command += cfg_entry.usrname;
			}
			command += " -h ";
			command += hostname;
			command += " -e \"";
			command += real_command;
			command += "\" -t ";
			command += boost::lexical_cast<string>(SCHD->_SCHD_polltime);
			mid = SGC->_SGC_proc.mid;
		} else if (cfg_entry.enable_uproxy) {
			command = "schd_agent -e \"";
			command += real_command;
			command += "\" -t ";
			command += boost::lexical_cast<string>(SCHD->_SCHD_polltime);
			mid = cfg_entry.mids[i];
		} else {
			command = real_command;
			mid = cfg_entry.mids[i];
		}

		if (!do_start(mid, entry_name, hostname, command)) {
			retval = false;
			return retval;
		} else {
			entry.processes++;
			SCHD->inc_proc(hostname, entry_name);
		}
	}

	retval = true;
	return retval;
}

// 在单节点上执行回退操作
bool schd_rpc::do_autoclean(const string& entry_name, schd_entry_t& entry, const schd_cfg_entry_t& cfg_entry, int undo_id)
{
	bool retval = false;
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, (_("entry_name={1}, undo_id={2}") % entry_name % undo_id).str(APPLOCALE), &retval);
#endif

	sgc_ctx *SGC = SGT->SGC();
	gpenv& env_mgr = gpenv::instance();

	if (cfg_entry.mids.size()>0) {
		int i = 0;
		string command;
		int mid;
		const string& hostname = cfg_entry.hostnames[i];
		string real_command;
		string env_str = (boost::format("SGSCHDUNDOID=%1%") % undo_id).str();
		env_mgr.putenv(env_str.c_str());
		env_mgr.expand_var(real_command, cfg_entry.autoclean);


		command = real_command;
		mid = cfg_entry.mids[i];

		if (!do_start(mid, entry_name, hostname, command)) {
			retval = false;
			return retval;
		} else {
			entry.processes++;
			SCHD->inc_proc(hostname, entry_name);
		}
	}

	retval = true;
	return retval;
}


//报告进度信息
bool schd_rpc::report_info(const string& message)
{
	bool retval = false;
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, (_("message={1}") % message).str(APPLOCALE), &retval);
#endif

	message_pointer rqst_msg = sg_message::create();
	int datalen = schd_rqst_t::need(message.length() + 1);
	rqst_msg->set_length(datalen);
	schd_rqst_t *rqst = reinterpret_cast<schd_rqst_t *>(rqst_msg->data());

	rqst->opcode = OSCHD_INFO;
	rqst->datalen = datalen;

	char *data = rqst->data();
	memcpy(data, message.c_str(), message.length() + 1);

	sgc_ctx *SGC = SGT->SGC();
	SGC->_SGC_special_mid = SGC->_SGC_proc.mid;
	BOOST_SCOPE_EXIT((&SGC)) {
		SGC->_SGC_special_mid = BADMID;
	} BOOST_SCOPE_EXIT_END

	if (api_mgr.acall(SCHD->_SCHD_svc_name.c_str(), rqst_msg, 0 | SGNOREPLY) == -1) {
		// 如果没有启服务，则忽略错误
		if (SGT->_SGT_error_code != SGENOENT)
			GPP->write_log(__FILE__, __LINE__, (_("WARN: Call operation {1} failed, - {2}") % SCHD->_SCHD_svc_name % SGT->strerror()).str(APPLOCALE));
		return retval;
	}

	retval = true;
	return retval;
}

// 报告进程正常退出
bool schd_rpc::report_exit()
{
	bool retval = false;
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", &retval);
#endif

	if (!SCHD->enable_service()) {
		retval = true;
		return retval;
	}

	message_pointer rqst_msg = sg_message::create();
	message_pointer rply_msg = sg_message::create();
	int datalen = schd_rqst_t::need(sizeof(pid_t));
	rqst_msg->set_length(datalen);
	schd_rqst_t *rqst = reinterpret_cast<schd_rqst_t *>(rqst_msg->data());

	rqst->opcode = OSCHD_EXIT;
	rqst->datalen = datalen;
	*reinterpret_cast<pid_t *>(rqst->data()) = getpid();

	sgc_ctx *SGC = SGT->SGC();
	SGC->_SGC_special_mid = SGC->_SGC_proc.mid;
	BOOST_SCOPE_EXIT((&SGC)) {
		SGC->_SGC_special_mid = BADMID;
	} BOOST_SCOPE_EXIT_END

	if (!api_mgr.call(SCHD->_SCHD_svc_name.c_str(), rqst_msg, rply_msg, 0)) {
		if (SGT->_SGT_error_code != SGENOENT)
			GPP->write_log(__FILE__, __LINE__, (_("WARN: Call operation {1} failed, - {2}") % SCHD->_SCHD_svc_name % SGT->strerror()).str(APPLOCALE));
		return retval;
	}

	retval = true;
	return retval;
}

// 报告进程的状态
void schd_rpc::report(const schd_proc_t& proc)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, (_("hostname={1}, pid={2}") % proc.hostname % proc.pid).str(APPLOCALE), NULL);
#endif

#if defined(SCHD_DEBUG)
	cout << "timestamp=" << time(0)
		<< ", type=" << SCHD_RPLY_PROC_STATUS
		<< ", entry_name=" << proc.entry_name
		<< ", hostname=" << proc.hostname
		<< ", pid=" << proc.pid
		<< ", status=" << proc.status
		<< ", start_time=" << proc.start_time
		<< ", stop_time=" << proc.stop_time
		<< std::endl;
#endif

	if (!SCHD->_SCHD_connected)
		return;

	try {
		bpt::iptree opt;
		bpt::iptree v_pt;
		ostringstream os;

		v_pt.put("type", SCHD_RPLY_PROC_STATUS);
		v_pt.put("entry_name", proc.entry_name);
		v_pt.put("hostname", proc.hostname);
		v_pt.put("pid", proc.pid);
		v_pt.put("status", proc.status & SCHD_STATUS_MASK);
		v_pt.put("start_time", proc.start_time);
		v_pt.put("stop_time", proc.stop_time);

		opt.add_child("info", v_pt);
		bpt::write_xml(os, opt);

		auto_ptr<cms::TextMessage> auto_msg(SCHD->_SCHD_session->createTextMessage(os.str()));
		cms::TextMessage *msg = auto_msg.get();

		SCHD->_SCHD_producer->send(msg);
	} catch (cms::CMSException& ex) {
		GPP->write_log(__FILE__, __LINE__, ex.what());
	}
}

// 报告节点的状态
void schd_rpc::report(const string& entry_name, const schd_entry_t& entry)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, (_("entry_name={1}") % entry_name).str(APPLOCALE), NULL);
#endif

#if defined(SCHD_DEBUG)
	cout << "timestamp=" << time(0)
		<< ", type=" << SCHD_RPLY_ENTRY_STATUS
		<< ", entry_name=" << entry_name
		<< ", file_count=" << entry.file_count
		<< ", file_pending=" << entry.file_pending
		<< ", processes=" << entry.processes
		<< ", policy=" << entry.policy
		<< ", status=" << entry.status
		<< std::endl;
#endif

	if (!SCHD->_SCHD_connected)
		return;

	try {
		bpt::iptree opt;
		bpt::iptree v_pt;
		ostringstream os;

		v_pt.put("type", SCHD_RPLY_ENTRY_STATUS);
		v_pt.put("entry_name", entry_name);
		v_pt.put("file_count", entry.file_count);
		v_pt.put("file_pending", entry.file_pending);
		v_pt.put("processes", entry.processes);
		v_pt.put("policy", entry.policy);
		v_pt.put("status", entry.status & SCHD_STATUS_MASK);

		opt.add_child("info", v_pt);
		bpt::write_xml(os, opt);

		auto_ptr<cms::TextMessage> auto_msg(SCHD->_SCHD_session->createTextMessage(os.str()));
		cms::TextMessage *msg = auto_msg.get();

		SCHD->_SCHD_producer->send(msg);
	} catch (cms::CMSException& ex) {
		GPP->write_log(__FILE__, __LINE__, ex.what());
	}
}

// 环境初始化报告
void schd_rpc::report()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

#if defined(SCHD_DEBUG)
	cout << "timestamp=" << time(0)
		<< ", type=" << SCHD_RPLY_INIT << std::endl;
#endif

	if (!SCHD->_SCHD_connected)
		return;

	try {
		bpt::iptree opt;
		bpt::iptree v_pt;
		ostringstream os;

		v_pt.put("type", SCHD_RPLY_INIT);

		opt.add_child("info", v_pt);
		bpt::write_xml(os, opt);

		auto_ptr<cms::TextMessage> auto_msg(SCHD->_SCHD_session->createTextMessage(os.str()));
		cms::TextMessage *msg = auto_msg.get();

		SCHD->_SCHD_producer->send(msg);
	} catch (cms::CMSException& ex) {
		GPP->write_log(__FILE__, __LINE__, ex.what());
	}
}

// 报告节点初始化
void schd_rpc::report(const string& entry_name)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, (_("entry_name={1}") % entry_name).str(APPLOCALE), NULL);
#endif

#if defined(SCHD_DEBUG)
	cout << "timestamp=" << time(0)
		<< ", type=" << SCHD_RPLY_ENTRY_INIT
		<< ", entry_name=" << entry_name
		<< std::endl;
#endif

	if (!SCHD->_SCHD_connected)
		return;

	try {
		bpt::iptree opt;
		bpt::iptree v_pt;
		ostringstream os;

		v_pt.put("type", SCHD_RPLY_ENTRY_INIT);
		v_pt.put("entries.entry_name", entry_name);

		opt.add_child("info", v_pt);
		bpt::write_xml(os, opt);

		auto_ptr<cms::TextMessage> auto_msg(SCHD->_SCHD_session->createTextMessage(os.str()));
		cms::TextMessage *msg = auto_msg.get();

		SCHD->_SCHD_producer->send(msg);
	} catch (cms::CMSException& ex) {
		GPP->write_log(__FILE__, __LINE__, ex.what());
	}

	map<string, schd_entry_t>::iterator entry_iter = SCHD->_SCHD_entries.find(entry_name);
	if (entry_iter == SCHD->_SCHD_entries.end()) {
		GPP->write_log(__FILE__, __LINE__, (_("FATAL: Can't find configuration for {1}") % entry_name).str(APPLOCALE));
		exit(1);
	}

	schd_entry_t& entry = entry_iter->second;
	report(entry_name, entry);
}

schd_rpc::schd_rpc()
	: api_mgr(sg_api::instance(SGT))
{
	SCHD = schd_ctx::instance();
}

schd_rpc::~schd_rpc()
{
}

bool schd_rpc::do_erase_internal(int mid, const string& entry_name)
{
	bool retval = false;
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, (_("mid={1}, entry_name={2}") % mid % entry_name).str(APPLOCALE), &retval);
#endif

	message_pointer rqst_msg = sg_message::create();
	message_pointer rply_msg = sg_message::create();
	int datalen = schd_rqst_t::need(entry_name.length() + 1);
	rqst_msg->set_length(datalen);
	schd_rqst_t *rqst = reinterpret_cast<schd_rqst_t *>(rqst_msg->data());

	rqst->opcode = OSCHD_ERASE;
	rqst->datalen = datalen;

	char *data = rqst->data();
 	memcpy(data, entry_name.c_str(), entry_name.length() + 1);
	data += entry_name.length() + 1;

	sgc_ctx *SGC = SGT->SGC();
	SGC->_SGC_special_mid = mid;
	BOOST_SCOPE_EXIT((&SGC)) {
		SGC->_SGC_special_mid = BADMID;
	} BOOST_SCOPE_EXIT_END

	if (!api_mgr.call(SCHD->_SCHD_svc_name.c_str(), rqst_msg, rply_msg, 0)) {
		GPP->write_log(__FILE__, __LINE__, (_("ERROR: Call operation {1} failed - {2}") % SCHD->_SCHD_svc_name % SGT->strerror()).str(APPLOCALE));
		return retval;
	}

	schd_rply_t *rply = reinterpret_cast<schd_rply_t *>(rply_msg->data());
	if (rply->rtn != 0) {
		GPP->write_log(__FILE__, __LINE__, (_("ERROR: Failed to erase entry {1} - {2}") % entry_name % rply->rtn).str(APPLOCALE));
		return retval;
	}

	retval = true;
	return retval;
}

bool schd_rpc::do_mark_internal(int mid, const string& entry_name)
{
	bool retval = false;
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, (_("mid={1}, entry_name={2}") % mid % entry_name).str(APPLOCALE), &retval);
#endif

	message_pointer rqst_msg = sg_message::create();
	message_pointer rply_msg = sg_message::create();
	int datalen = schd_rqst_t::need(entry_name.length() + 1);
	rqst_msg->set_length(datalen);
	schd_rqst_t *rqst = reinterpret_cast<schd_rqst_t *>(rqst_msg->data());

	rqst->opcode = OSCHD_MARK;
	rqst->datalen = datalen;

	char *data = rqst->data();
 	memcpy(data, entry_name.c_str(), entry_name.length() + 1);
	data += entry_name.length() + 1;

	sgc_ctx *SGC = SGT->SGC();
	SGC->_SGC_special_mid = mid;
	BOOST_SCOPE_EXIT((&SGC)) {
		SGC->_SGC_special_mid = BADMID;
	} BOOST_SCOPE_EXIT_END

	if (!api_mgr.call(SCHD->_SCHD_svc_name.c_str(), rqst_msg, rply_msg, 0)) {
		GPP->write_log(__FILE__, __LINE__, (_("ERROR: Call operation {1} failed - {2}") % SCHD->_SCHD_svc_name % SGT->strerror()).str(APPLOCALE));
		return retval;
	}

	schd_rply_t *rply = reinterpret_cast<schd_rply_t *>(rply_msg->data());
	if (rply->rtn != 0) {
		GPP->write_log(__FILE__, __LINE__, (_("ERROR: Failed to erase entry {1} - {2}") % entry_name % rply->rtn).str(APPLOCALE));
		return retval;
	}

	retval = true;
	return retval;
}

}
}

