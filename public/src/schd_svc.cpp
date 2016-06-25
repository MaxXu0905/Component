#include "schd_svc.h"
#include "schd_rpc.h"

using namespace ai::sg;

namespace ai
{
namespace app
{

schd_svc::schd_svc()
{
	SCHD = schd_ctx::instance();
	rply_msg = sg_message::create(SGT);
}

schd_svc::~schd_svc()
{
}

svc_fini_t schd_svc::svc(message_pointer& svcinfo)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	const schd_rqst_t& rqst = *reinterpret_cast<schd_rqst_t *>(svcinfo->data());
	switch (rqst.opcode) {
	case OSCHD_INIT:
		do_init(rqst);
		break;
	case OSCHD_START:
		do_start(rqst);
		break;
	case OSCHD_STOP:
		do_stop(rqst);
		break;
	case OSCHD_STATUS:
		do_status(rqst);
		break;
	case OSCHD_CHECK:
		do_check(rqst);
		break;
	case OSCHD_ERASE:
		do_erase(rqst);
		break;
	case OSCHD_MARK:
		do_mark(rqst);
		break;
	case OSCHD_INFO:
		do_info(rqst);
		break;
	case OSCHD_EXIT:
		do_exit(rqst);
		break;
	default:
		return svc_fini(SGFAIL, 0, rply_msg);
	}

	return svc_fini(SGSUCCESS, 0, rply_msg);
}

// 设置到初始时刻，没有进程启动
void schd_svc::do_init(const schd_rqst_t& rqst)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	int datalen = schd_rply_t::need(0);
	rply_msg->set_length(datalen);
	schd_rply_t *rply = reinterpret_cast<schd_rply_t *>(rply_msg->data());
	rply->flags = 0;
	rply->num = 0;
	rply->datalen = datalen;

	SCHD->_SCHD_managed_procs.clear();
	if (!truncate()) {
		rply->error_code = SGESYSTEM;
		rply->rtn = -1;
		GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't truncate table on sqlite")).str(APPLOCALE));
	} else {
		rply->error_code = 0;
		rply->rtn = 0;
	}
}

// 启动进程
void schd_svc::do_start(const schd_rqst_t& rqst)
{
#if defined(DEBUG)
	scoped_debug<int> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	const char *data = rqst.data();
 	const char *entry_name = data;
	data += strlen(entry_name) + 1;
	const char *hostname = data;
	data += strlen(hostname) + 1;
	const char *command = data;

	sig_action sig(SIGCHLD, SIG_IGN);
	int retval = -1;
	int tochild[2];
	char ack;

	int datalen = schd_rply_t::need(sizeof(schd_agent_proc_t));
	rply_msg->set_length(datalen);
	schd_rply_t *rply = reinterpret_cast<schd_rply_t *>(rply_msg->data());
	schd_agent_proc_t *agent_proc = reinterpret_cast<schd_agent_proc_t *>(rply->data());
	rply->flags = 0;

	if (pipe(tochild) < 0) {
		rply->error_code = SGEOS;
		rply->rtn = -1;
		rply->num = 0;
		rply->datalen = schd_rply_t::need(0);
		rply_msg->set_length(rply->datalen);
		return;
	}

	pid_t pid = ::fork();
	switch (pid) {
	case 0:
		close(tochild[1]);
		if (read(tochild[0], &ack, 1) != 1) {
			close(tochild[0]);
			exit(retval);
		}
		close(tochild[0]);

		retval = ::execl("/bin/sh", "/bin/sh", "-c", command, (char *)0);
		::exit(retval);
		break;
	case -1:
		rply->error_code = SGEOS;
		rply->rtn = -1;
		rply->num = 0;
		rply->datalen = schd_rply_t::need(0);
		rply_msg->set_length(rply->datalen);
		return;
	default:
		close(tochild[0]);

		agent_proc->pid = pid;
		strcpy(agent_proc->entry_name, entry_name);
		strcpy(agent_proc->hostname, hostname);
		agent_proc->status = SCHD_RUNNING;
		agent_proc->start_time = time(0);
		agent_proc->stop_time = 0;

		if (!insert(*agent_proc)) {
			GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't insert process on sqlite")).str(APPLOCALE));
			rply->error_code = SGESYSTEM;
			rply->rtn = -1;
			rply->num = 0;
			rply->datalen = schd_rply_t::need(0);
			rply_msg->set_length(rply->datalen);
			close(tochild[1]);
			return;
		}

 		write(tochild[1], "0", 1);
		close(tochild[1]);

		SCHD->_SCHD_managed_procs.insert(*agent_proc);

		rply->error_code = 0;
		rply->rtn = 0;
		rply->num = 1;
		rply->datalen = datalen;
		break;
	}
}

// 停止进程
void schd_svc::do_stop(const schd_rqst_t& rqst)
{
#if defined(DEBUG)
	scoped_debug<int> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	int i;
	const char *data = rqst.data();
	const schd_stop_rqst_t& stop_rqst = *reinterpret_cast<const schd_stop_rqst_t *>(data);
	int datalen = schd_rply_t::need(sizeof(schd_agent_proc_t));
	rply_msg->set_length(datalen);
	schd_rply_t *rply = reinterpret_cast<schd_rply_t *>(rply_msg->data());
	schd_agent_proc_t *agent_proc = reinterpret_cast<schd_agent_proc_t *>(rply->data());

	rply->flags = 0;
	rply->num = 0;
	rply->datalen = datalen;

	schd_agent_proc_t key;
	key.pid = stop_rqst.pid;
	key.start_time = stop_rqst.start_time;

	set<schd_agent_proc_t>::iterator iter = SCHD->_SCHD_managed_procs.find(key);
	if (iter == SCHD->_SCHD_managed_procs.end()) {
		rply->error_code = SGENOENT;
		rply->rtn = -1;
		return;
	}

	schd_agent_proc_t& item = const_cast<schd_agent_proc_t&>(*iter);
	int millisec = stop_rqst.wait_time * 1000;
	switch (item.status) {
	case SCHD_RUNNING:
		for (i = 0; i < millisec; i++) {
			if (kill(stop_rqst.pid, stop_rqst.signo) == -1) {
				switch (errno) {
				case ESRCH:
					rply->error_code = 0;
					rply->rtn = 0;
					break;
				case EINVAL:
					rply->error_code = SGEINVAL;
					rply->rtn = -1;
					return;
				case EPERM:
					rply->error_code = SGEPERM;
					rply->rtn = -1;
					return;
				default:
					rply->error_code = SGEOS;
					rply->rtn = -1;
					return;
				}

				break;
			}

			// 休眠1毫秒
			usleep(1000);
		}

		if (i == millisec) {
			kill(stop_rqst.pid, SIGKILL);
			GPP->write_log(__FILE__, __LINE__, (_("WARN: process {1} for entry {2} on {3} is stopped forcefully") %  item.entry_name % item.pid % item.hostname).str(APPLOCALE));
			rply->error_code = 0;
			rply->rtn = 0;
		}

		item.status = SCHD_STOPPED;
		item.stop_time = time(0);

		if (!update(item)) {
			GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't update entry on sqlite")).str(APPLOCALE));
			rply->error_code = SGESYSTEM;
			rply->rtn = -1;
			return;
		}

		break;
	case SCHD_FINISH:
	case SCHD_STOPPED:
	case SCHD_TERMINATED:
		rply->error_code = 0;
		rply->rtn = 0;
		break;
	default:
		rply->error_code = SGESYSTEM;
		rply->rtn = -1;
		return;
	}

	*agent_proc = item;
}

// 获取管理节点的状态
void schd_svc::do_status(const schd_rqst_t& rqst)
{
#if defined(DEBUG)
	scoped_debug<int> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	int entries = SCHD->_SCHD_managed_procs.size();
	int datalen = schd_rply_t::need(sizeof(schd_agent_proc_t) * entries);
	rply_msg->set_length(datalen);
	schd_rply_t *rply = reinterpret_cast<schd_rply_t *>(rply_msg->data());

	schd_agent_proc_t *target_entry = reinterpret_cast<schd_agent_proc_t *>(rply->data());
	BOOST_FOREACH(const schd_agent_proc_t& agent_proc, SCHD->_SCHD_managed_procs) {
		*target_entry = agent_proc;
		target_entry++;
	}

	rply->error_code = 0;
	rply->rtn = 0;
	rply->flags = 0;
	rply->num = entries;
	rply->datalen = datalen;
}

// 检查进程状态，返回所有节点列表
void schd_svc::do_check(const schd_rqst_t& rqst)
{
#if defined(DEBUG)
	scoped_debug<int> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	for (set<schd_agent_proc_t>::iterator iter = SCHD->_SCHD_managed_procs.begin(); iter != SCHD->_SCHD_managed_procs.end(); ++iter) {
		schd_agent_proc_t& agent_proc = const_cast<schd_agent_proc_t&>(*iter);

		if (!(agent_proc.status & SCHD_RUNNING))
			continue;

		if (kill(agent_proc.pid, 0) == -1) {
			agent_proc.status = SCHD_TERMINATED;
			agent_proc.stop_time = time(0);

			if (!update(agent_proc))
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't update entry on sqlite")).str(APPLOCALE));
		}
	}

	int entries = SCHD->_SCHD_managed_procs.size();
	int datalen = schd_rply_t::need(sizeof(schd_agent_proc_t) * entries);
	rply_msg->set_length(datalen);
	schd_rply_t *rply = reinterpret_cast<schd_rply_t *>(rply_msg->data());

	schd_agent_proc_t *target_entry = reinterpret_cast<schd_agent_proc_t *>(rply->data());
	BOOST_FOREACH(const schd_agent_proc_t& agent_proc, SCHD->_SCHD_managed_procs) {
		*target_entry = agent_proc;
		target_entry++;
	}

	rply->error_code = 0;
	rply->rtn = 0;
	rply->flags = 0;
	rply->num = entries;
	rply->datalen = datalen;
}

// 删除指定的节点
void schd_svc::do_erase(const schd_rqst_t& rqst)
{
#if defined(DEBUG)
	scoped_debug<int> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif
 	string entry_name = rqst.data();

	for (set<schd_agent_proc_t>::iterator iter = SCHD->_SCHD_managed_procs.begin(); iter != SCHD->_SCHD_managed_procs.end();) {
		if (entry_name != iter->entry_name) {
			++iter;
			continue;
		}

		(void)erase(*iter);
		set<schd_agent_proc_t>::iterator tmp_iter = iter++;
		SCHD->_SCHD_managed_procs.erase(tmp_iter);
	}

	int datalen = schd_rply_t::need(0);
	rply_msg->set_length(datalen);
	schd_rply_t *rply = reinterpret_cast<schd_rply_t *>(rply_msg->data());
	rply->error_code = 0;
	rply->rtn = 0;
	rply->flags = 0;
	rply->num = 0;
	rply->datalen = datalen;
}

// 标记指定的节点，忽略其状态
void schd_svc::do_mark(const schd_rqst_t& rqst)
{
	int retval = 0;
#if defined(DEBUG)
	scoped_debug<int> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif
 	string entry_name = rqst.data();

	for (set<schd_agent_proc_t>::iterator iter = SCHD->_SCHD_managed_procs.begin(); iter != SCHD->_SCHD_managed_procs.end(); ++iter) {
		if (entry_name == iter->entry_name) {
			schd_agent_proc_t& agent_proc = const_cast<schd_agent_proc_t&>(*iter);
			if (!(agent_proc.status & (SCHD_FINISH | SCHD_TERMINATED | SCHD_STOPPED)))
				continue;

			agent_proc.status |= SCHD_FLAG_IGNORE;
			if (!update(agent_proc)) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't update entry on sqlite")).str(APPLOCALE));
				retval = -1;
				break;
			}
		}
	}

	int datalen = schd_rply_t::need(0);
	rply_msg->set_length(datalen);
	schd_rply_t *rply = reinterpret_cast<schd_rply_t *>(rply_msg->data());
	rply->error_code = 0;
	rply->rtn = retval;
	rply->flags = 0;
	rply->num = 0;
	rply->datalen = datalen;
}


// 转发JMS消息
void schd_svc::do_info(const schd_rqst_t& rqst)
{
#if defined(DEBUG)
	scoped_debug<int> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	// 尝试重新连接
	if (!SCHD->_SCHD_connected && time(0) >= SCHD->_SCHD_retry_time) {
		SCHD->connect();
		if (!SCHD->_SCHD_connected)
			return;
	}

	try {
		const char *data = rqst.data();
		string text = string(data, data + rqst.datalen - schd_rqst_t::need(0));
		auto_ptr<cms::TextMessage> message(SCHD->_SCHD_session->createTextMessage(text));

		SCHD->_SCHD_producer->send(message.get());
	} catch (cms::CMSException& ex) {
		GPP->write_log(__FILE__, __LINE__, ex.what());
	}
}

//进程准备退出
void schd_svc::do_exit(const schd_rqst_t& rqst)
{
#if defined(DEBUG)
	scoped_debug<int> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	schd_agent_proc_t key;
	key.pid = *reinterpret_cast<const pid_t *>(rqst.data());
	key.start_time = 0;

	set<schd_agent_proc_t>::iterator iter = SCHD->_SCHD_managed_procs.lower_bound(key);
	set<schd_agent_proc_t>::iterator match_iter = SCHD->_SCHD_managed_procs.end();
	while (iter != SCHD->_SCHD_managed_procs.end()) {
		if (iter->pid != key.pid)
			break;

		match_iter = iter++;
	}

	if (match_iter != SCHD->_SCHD_managed_procs.end()) {
		schd_agent_proc_t& agent_proc = const_cast<schd_agent_proc_t&>(*match_iter);

		agent_proc.status = SCHD_FINISH;
		agent_proc.stop_time = time(0);

		if (!update(agent_proc))
			GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't update entry on sqlite")).str(APPLOCALE));
	}

	int datalen = schd_rply_t::need(0);
	rply_msg->set_length(datalen);
	schd_rply_t *rply = reinterpret_cast<schd_rply_t *>(rply_msg->data());
	rply->error_code = 0;
	rply->rtn = 0;
	rply->flags = 0;
	rply->num = 0;
	rply->datalen = datalen;
}

// 插入SQLITE节点
bool schd_svc::insert(const schd_agent_proc_t& entry)
{
	bool retval = false;
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", &retval);
#endif

	sqlite3 *& db = SCHD->_SCHD_sqlite3_db;
	sqlite3_stmt *& stmt = SCHD->_SCHD_insert_stmt;

	if (sqlite3_reset(stmt) != SQLITE_OK) {
		GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't reset insert statement, {1}") % sqlite3_errmsg(db)).str(APPLOCALE));
		return retval;
	}

	if (sqlite3_bind_int(stmt, 1, entry.pid) != SQLITE_OK) {
		GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't bind insert statement, {1}") % sqlite3_errmsg(db)).str(APPLOCALE));
		return retval;
	}

	if (sqlite3_bind_text(stmt, 2, entry.entry_name, -1, SQLITE_STATIC) != SQLITE_OK) {
		GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't bind insert statement, {1}") % sqlite3_errmsg(db)).str(APPLOCALE));
		return retval;
	}

	if (sqlite3_bind_text(stmt, 3, entry.hostname, -1, SQLITE_STATIC) != SQLITE_OK) {
		GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't bind insert statement, {1}") % sqlite3_errmsg(db)).str(APPLOCALE));
		return retval;
	}

	if (sqlite3_bind_int(stmt, 4, entry.status) != SQLITE_OK) {
		GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't bind insert statement, {1}") % sqlite3_errmsg(db)).str(APPLOCALE));
		return retval;
	}

	if (sqlite3_bind_int64(stmt, 5, entry.start_time) != SQLITE_OK) {
		GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't bind insert statement, {1}") % sqlite3_errmsg(db)).str(APPLOCALE));
		return retval;
	}

	if (sqlite3_bind_int64(stmt, 6, entry.stop_time) != SQLITE_OK) {
		GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't bind insert statement, {1}") % sqlite3_errmsg(db)).str(APPLOCALE));
		return retval;
	}

	while (1) {
		switch (sqlite3_step(stmt)) {
		case SQLITE_DONE:		// 正确
			break;
		case SQLITE_BUSY:		// 系统忙，重试
			boost::this_thread::yield();
			continue;
		default:
			GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't step insert statement, {1}") % sqlite3_errmsg(db)).str(APPLOCALE));
			return retval;
		}

		break;
	}

	retval = true;
	return retval;
}

// 更新SQLITE节点
bool schd_svc::update(const schd_agent_proc_t& entry)
{
	bool retval = false;
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", &retval);
#endif

	sqlite3 *& db = SCHD->_SCHD_sqlite3_db;
	sqlite3_stmt *& stmt = SCHD->_SCHD_update_stmt;

	if (sqlite3_reset(stmt) != SQLITE_OK) {
		GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't reset update statement, {1}") % sqlite3_errmsg(db)).str(APPLOCALE));
		return retval;
	}

	if (sqlite3_bind_int(stmt, 1, entry.status) != SQLITE_OK) {
		GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't bind update statement, {1}") % sqlite3_errmsg(db)).str(APPLOCALE));
		return retval;
	}

	if (sqlite3_bind_int64(stmt, 2, entry.stop_time) != SQLITE_OK) {
		GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't bind update statement, {1}") % sqlite3_errmsg(db)).str(APPLOCALE));
		return retval;
	}

	if (sqlite3_bind_int(stmt, 3, entry.pid) != SQLITE_OK) {
		GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't bind update statement, {1}") % sqlite3_errmsg(db)).str(APPLOCALE));
		return retval;
	}

	if (sqlite3_bind_int(stmt, 4, entry.start_time) != SQLITE_OK) {
		GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't bind update statement, {1}") % sqlite3_errmsg(db)).str(APPLOCALE));
		return retval;
	}

	while (1) {
		switch (sqlite3_step(stmt)) {
		case SQLITE_DONE:		// 正确
			break;
		case SQLITE_BUSY:		// 系统忙，重试
			boost::this_thread::yield();
			continue;
		default:
			GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't step update statement, {1}") % sqlite3_errmsg(db)).str(APPLOCALE));
			return retval;
		}

		break;
	}

	retval = true;
	return retval;
}

// 删除SQLITE节点
bool schd_svc::erase(const schd_agent_proc_t& item)
{
	bool retval = false;
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", &retval);
#endif

	sqlite3 *& db = SCHD->_SCHD_sqlite3_db;
	sqlite3_stmt *& stmt = SCHD->_SCHD_delete_stmt;

	if (sqlite3_reset(stmt) != SQLITE_OK) {
		GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't reset delete statement, {1}") % sqlite3_errmsg(db)).str(APPLOCALE));
		return retval;
	}

	if (sqlite3_bind_int(stmt, 1, item.pid) != SQLITE_OK) {
		GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't bind delete statement, {1}") % sqlite3_errmsg(db)).str(APPLOCALE));
		return retval;
	}

	if (sqlite3_bind_int(stmt, 2, item.start_time) != SQLITE_OK) {
		GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't bind delete statement, {1}") % sqlite3_errmsg(db)).str(APPLOCALE));
		return retval;
	}

	while (1) {
		switch (sqlite3_step(stmt)) {
		case SQLITE_DONE:		// 正确
			break;
		case SQLITE_BUSY:		// 系统忙，重试
			boost::this_thread::yield();
			continue;
		default:
			GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't step delete statement, {1}") % sqlite3_errmsg(db)).str(APPLOCALE));
			return retval;
		}

		break;
	}

	retval = true;
	return retval;
}

//清空SQLITE表
bool schd_svc::truncate()
{
	bool retval = false;
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", &retval);
#endif

	sqlite3 *& db = SCHD->_SCHD_sqlite3_db;

	if (sqlite3_exec(db, "delete from schd_table", NULL, NULL, NULL) != SQLITE_OK) {
		GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't truncate table, {1}") % sqlite3_errmsg(db)).str(APPLOCALE));
		return retval;
	}

	retval = true;
	return retval;
}

}
}

