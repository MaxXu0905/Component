#if !defined(__SCHD_CTX_H__)
#define __SCHD_CTX_H__

#include "app_locale.h"
#include "sg_internal.h"
#include <sqlite3.h>
#include <activemq/library/ActiveMQCPP.h>
#include <cms/ConnectionFactory.h>
#include <cms/Connection.h>
#include <cms/Session.h>
#include <cms/MessageConsumer.h>
#include <cms/MessageListener.h>
#include <cms/Destination.h>
#include <cms/ExceptionListener.h>
#include <cms/CMSException.h>

using namespace ai::sg;

namespace ai
{
namespace app
{

int const SCHD_RELEASE = 1;

// 初始状态
int const SCHD_INIT = 0x1;
// 具备运行条件
int const SCHD_READY = 0x2;
// 正在运行
int const SCHD_RUNNING = 0x4;
// 运行正常完成
int const SCHD_FINISH = 0x8;
// 运行异常终止
int const SCHD_TERMINATED = 0x10;
// 主动停止
int const SCHD_STOPPED = 0x20;
// 启动前需要清理输入
int const SCHD_ICLEAN = 0x40;
// 正在清理
int const SCHD_CLEANING = 0x80;
// 待清理
int const SCHD_PENDING = 0x100;
// 状态掩码
int const SCHD_STATUS_MASK = 0xffff;
//可重启
int const SCHD_FLAG_RESTARTABLE = 0x10000;
// 节点/进程状态需忽略
int const SCHD_FLAG_IGNORE = 0x20000;
// 节点开始监控目录
int const SCHD_FLAG_WATCH = 0x40000;
// 是否已经记录错误
int const SCHD_FLAG_LOGGED = 0x80000;

int const SCHD_POLICY_BYHAND = 0x1;
int const SCHD_POLICY_AUTO = 0x2;

const char SCHD_SVC[] = "SCHD_SVC";
const char DFLT_BROKER_URI[] = "tcp://127.0.0.1:61616";
const char PREFIX_PRODUCER_URI[] = "ai.app.info.";
const char PREFIX_CONSUMER_URI[] = "ai.app.schd.";
const char PREFIX_LOGGER_URI[] = "ai.app.log.";

int const SCHD_RQST_STATUS = 1;	// 获取节点运行状态
int const SCHD_RQST_REDO = 2;		// 节点重入
int const SCHD_RQST_POLICY = 3;	// 节点策略变更
int const SCHD_RQST_STOP = 4;		// 节点停止

int const SCHD_RPLY_SCHD_STATUS = 1;		// 调度本身的状态
int const SCHD_RPLY_ENTRY_STATUS = 2;		// 节点状态
int const SCHD_RPLY_PROC_STATUS = 3;		// 进程状态
int const SCHD_RPLY_PROC_PROGRESS = 4;	// 进程进度
int const SCHD_RPLY_INIT = 5;				// 环境初始化
int const SCHD_RPLY_ENTRY_INIT = 6;		// 节点初始化

int const ENTRY_NAME_SIZE = 31;
int const HOSTNAME_SIZE = 31;

struct schd_directory_t
{
	string sftp_prefix;
	string directory;
};

struct schd_cfg_entry_t
{
	string command;
	string iclean;
	string undo;
	string autoclean;
	string usrname;
	vector<string> hostnames;
	vector<int> mids;
	int flags;
	int maxgen;
	int min;
	int max;
	vector<schd_directory_t> directories;
	string pattern;
	int policy;
	bool enable_proxy;
	bool enable_cproxy;
	bool enable_iproxy;
	bool enable_uproxy;
};

struct schd_entry_t
{
	int file_count;
	int file_pending;
	int processes;
	int policy;
	int status;
	int retries;
	int min;
	int max;
	int undo_id;

	schd_entry_t() {
		file_count = 0;
		file_pending = 0;
		processes = 0;
		policy = SCHD_POLICY_BYHAND;
		status = SCHD_INIT;
		retries = 0;
		min = 0;
		max = 0;
		undo_id = 0;
	}
};

struct schd_agent_proc_t
{
	pid_t pid;
	char entry_name[ENTRY_NAME_SIZE + 1];
	char hostname[HOSTNAME_SIZE + 1];
	int status;
	time_t start_time;
	time_t stop_time;

	bool operator<(const schd_agent_proc_t& rhs) const {
		if (pid < rhs.pid)
			return true;
		else if (pid > rhs.pid)
			return false;

		return start_time < rhs.start_time;
	}
};

struct schd_proc_t
{
	int mid;
	pid_t pid;
	string entry_name;
	string hostname;
	string command;
	int status;
	time_t start_time;
	time_t stop_time;

	bool operator<(const schd_proc_t& rhs) const {
		if (mid < rhs.mid)
			return true;
		else if (mid > rhs.mid)
			return false;

		if (pid < rhs.pid)
			return true;
		else if (pid > rhs.pid)
			return false;

		return start_time < rhs.start_time;
	}

	schd_proc_t() {
	}

	schd_proc_t(int _mid, const schd_agent_proc_t& proc) {
		mid = _mid;
		pid = proc.pid;
		entry_name = proc.entry_name;
		hostname = proc.hostname;
		status = proc.status;
		start_time = proc.start_time;
		stop_time = proc.stop_time;
	}
};

struct schd_host_entry_t
{
	string hostname;
	string entry_name;
	int processes;
	bool startable;

	bool operator<(const schd_host_entry_t& rhs) const {
		if (hostname < rhs.hostname)
			return true;
		else if (hostname > rhs.hostname)
			return false;

		return entry_name < rhs.entry_name;
	}
};

struct schd_condition_t
{
	string entry_name;
	bool finish;
};

class schd_ctx
{
public:
	static schd_ctx * instance();
	~schd_ctx();

	void pmid2mid(sgt_ctx *SGT);
	void connect();
	void disconnect();
	int count_files(const std::string& dir, const std::string& pattern, const std::string& sftp_prefix = "");
	void set_service();
	bool enable_service() const;
	void inc_proc(const std::string& hostname, const std::string& entry_name);
	void dec_proc(const std::string& hostname, const std::string& entry_name);

	bool _SCHD_shutdown;

	// 以下是服务端使用的数据结构

	// 代理管理的进程列表
	set<schd_agent_proc_t> _SCHD_managed_procs;
	// 数据库名称
	std::string _SCHD_db_name;
	// SQLite数据库连接
	sqlite3 *_SCHD_sqlite3_db;
	sqlite3_stmt *_SCHD_select_stmt;
	sqlite3_stmt *_SCHD_insert_stmt;
	sqlite3_stmt *_SCHD_update_stmt;
	sqlite3_stmt *_SCHD_delete_stmt;

	// 以下是客户端使用的数据结构

	// 配置节点信息列表
	map<string, schd_cfg_entry_t> _SCHD_cfg_entries;
	// 节点间依赖关系
	map<string, vector<string> > _SCHD_deps;
	// 节点间反向依赖关系
	map<string, vector<string> > _SCHD_rdeps;
	// 节点启动时的动作
	map<string, vector<string> > _SCHD_actions;
	// 节点启动条件
	map<string, vector<schd_condition_t> > _SCHD_conditions;
	// 所有相关的mid列表
	vector<int> _SCHD_mids;
	// 进程详细信息
	set<schd_proc_t> _SCHD_procs;
	// 节点状态
	map<string, schd_entry_t> _SCHD_entries;

	// 启动参数
	int _SCHD_parallel_entries;
	int _SCHD_parallel_procs;
	int _SCHD_polltime;
	int _SCHD_robustint;
	bool _SCHD_resume;
	int _SCHD_stop_timeout;
	int _SCHD_stop_signo;

	// JMS相关
	bool _SCHD_connected;
	string _SCHD_brokerURI;
	string _SCHD_suffixURI;
	string _SCHD_producerURI;
	string _SCHD_consumerURI;
	string _SCHD_svc_name;
	cms::Connection *_SCHD_connection;
	cms::Session *_SCHD_session;
	cms::MessageProducer *_SCHD_producer;
	cms::MessageConsumer *_SCHD_consumer;
	cms::Destination *_SCHD_producer_destination;
	cms::Destination *_SCHD_consumer_destination;
	time_t _SCHD_retry_time;

	int _SCHD_running_entries;
	map<string, int> _SCHD_host_procs;
	set<schd_host_entry_t> _SCHD_entry_procs;

private:
	schd_ctx();

	static void init_once();

	static boost::once_flag once_flag;
	static schd_ctx *_instance;
};

}
}

#endif

