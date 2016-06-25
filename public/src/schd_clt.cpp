#include "schd_clt.h"
#include "schd_rpc.h"
#include "schd_policy.h"

using namespace ai::sg;
namespace po = boost::program_options;
typedef boost::tokenizer<boost::char_separator<char> > tokenizer;

namespace ai
{
namespace app
{

int const DFLT_SCHD_PARALLEL_ENTRIES = std::numeric_limits<int>::max();
int const DFLT_SCHD_PARALLEL_PROCS = 0;
int const DFLT_SCHD_POLLTIME = 6;
int const DFLT_SCHD_ROBUSTINT = 10;
int const DFLT_STOP_TIMEOUT = 6;

schd_clt::schd_clt()
{
	SCHD = schd_ctx::instance();
	rqst_msg = sg_message::create(SGT);
	rply_msg = sg_message::create(SGT);
}

schd_clt::~schd_clt()
{
	sg_api& api_mgr = sg_api::instance(SGT);

	api_mgr.fini();
}

int schd_clt::init(int argc, char **argv)
{
	int retval = -1;
#if defined(DEBUG)
	scoped_debug<int> debug(500, __PRETTY_FUNCTION__, (_("argc={1}") % argc).str(APPLOCALE), &retval);
#endif
	string config_file;
	string schd_key;
	int schd_version;

	po::options_description desc((_("Allowed options")).str(APPLOCALE));
	desc.add_options()
		("help", (_("produce help message")).str(APPLOCALE).c_str())
		("version,v", (_("print current run_schd version")).str(APPLOCALE).c_str())
		("background,b", (_("run scheduler in background mode")).str(APPLOCALE).c_str())
		("conf,c", po::value<string>(&config_file), (_("scheduler configuration file")).str(APPLOCALE).c_str())
		("schd_key,k", po::value<string>(&schd_key), (_("specify schedule key")).str(APPLOCALE).c_str())
		("schd_version,y", po::value<int>(&schd_version)->default_value(-1), (_("specify schedule version")).str(APPLOCALE).c_str())
		("entries,e", po::value<int>(&SCHD->_SCHD_parallel_entries)->default_value(DFLT_SCHD_PARALLEL_ENTRIES), (_("specify maximum parallel running entries")).str(APPLOCALE).c_str())
		("processes,p", po::value<int>(&SCHD->_SCHD_parallel_procs)->default_value(DFLT_SCHD_PARALLEL_PROCS), (_("specify maximum parallel running processes")).str(APPLOCALE).c_str())
		("polltime,t", po::value<int>(&SCHD->_SCHD_polltime)->default_value(DFLT_SCHD_POLLTIME), (_("specify seconds to check alive status")).str(APPLOCALE).c_str())
		("robustint,r", po::value<int>(&SCHD->_SCHD_robustint)->default_value(DFLT_SCHD_ROBUSTINT), (_("how much polltime to count pending files")).str(APPLOCALE).c_str())
		("resume,R", (_("continue from last status")).str(APPLOCALE).c_str())
		("brokerURI,B", po::value<string>(&SCHD->_SCHD_brokerURI)->default_value(DFLT_BROKER_URI), (_("specify JMS broker URI")).str(APPLOCALE).c_str())
		("suffixURI,S", po::value<string>(&SCHD->_SCHD_suffixURI)->required(), (_("specify JMS producer/consumer topic URI suffix")).str(APPLOCALE).c_str())
		("timeout,T", po::value<int>(&SCHD->_SCHD_stop_timeout)->default_value(DFLT_STOP_TIMEOUT), (_("specify stop timeout in seconds before kill forcefully")).str(APPLOCALE).c_str())
		("signo,s", po::value<int>(&SCHD->_SCHD_stop_signo)->default_value(SIGTERM), (_("specify stop signo via kill")).str(APPLOCALE).c_str())
	;

	po::variables_map vm;

	try {
		po::store(po::parse_command_line(argc, argv, desc), vm);

		if (vm.count("help")) {
			std::cout << desc << std::endl;
			GPP->write_log((_("INFO: {1} exit normally in help mode.") % GPP->_GPP_procname).str(APPLOCALE));
			::exit(0);
		} else if (vm.count("version")) {
			std::cout << (_("run_schd version ")).str(APPLOCALE) << SCHD_RELEASE << std::endl;
			std::cout << (_("Compiled on ")).str(APPLOCALE) << __DATE__ << " " << __TIME__ << std::endl;
			::exit(0);
		}

		po::notify(vm);
	} catch (exception& ex) {
		std::cout << ex.what() << std::endl;
		std::cout << desc << std::endl;
		SGT->_SGT_error_code = SGEINVAL;
		GPP->write_log(__FILE__, __LINE__, (_("ERROR: {1} start failure, {2}.") % GPP->_GPP_procname % ex.what()).str(APPLOCALE));
		return retval;
	}

	if (vm.count("resume"))
		SCHD->_SCHD_resume = true;
	else
		SCHD->_SCHD_resume = false;

	if (config_file.empty() && schd_key.empty()) {
		std::cout << (_("ERROR: --conf or --schd_key must be specify at least once\n")).str(APPLOCALE);
		std::cout << desc << std::endl;
		SGT->_SGT_error_code = SGEINVAL;
		GPP->write_log(__FILE__, __LINE__, (_("ERROR: {1} start failure.") % GPP->_GPP_procname).str(APPLOCALE));
		return retval;
	}

	if (vm.count("background")) {
		sys_func& sys_mgr = sys_func::instance();
		sys_mgr.background();
	}

	if (SCHD->_SCHD_parallel_entries <= 0)
		SCHD->_SCHD_parallel_entries = DFLT_SCHD_PARALLEL_ENTRIES;
	if (SCHD->_SCHD_parallel_procs < 0)
		SCHD->_SCHD_parallel_entries = DFLT_SCHD_PARALLEL_PROCS;
	if (SCHD->_SCHD_polltime <= 0)
		SCHD->_SCHD_polltime = DFLT_SCHD_POLLTIME;
	if (SCHD->_SCHD_robustint <= 0)
		SCHD->_SCHD_robustint = DFLT_SCHD_ROBUSTINT;
	if (SCHD->_SCHD_stop_timeout <= 0)
		SCHD->_SCHD_stop_timeout = DFLT_STOP_TIMEOUT;
	if (SCHD->_SCHD_stop_signo <= 0)
		SCHD->_SCHD_stop_signo = SIGTERM;

	SCHD->_SCHD_producerURI = PREFIX_PRODUCER_URI;
	SCHD->_SCHD_producerURI += SCHD->_SCHD_suffixURI;
	SCHD->_SCHD_consumerURI = PREFIX_CONSUMER_URI;
	SCHD->_SCHD_consumerURI += SCHD->_SCHD_suffixURI;
	SCHD->set_service();

	// 设置环境变量
	gpenv& env_mgr = gpenv::instance();
	string str = (boost::format("SASUFFIXURI=%1%") % SCHD->_SCHD_suffixURI).str();
	env_mgr.putenv(str.c_str());

	try {
		if (!config_file.empty()) {
			// 加载配置文件
			schd_config config_mgr;
			config_mgr.load(config_file);
		}

		// 登录到SG
		string config;
		sg_api& api_mgr = sg_api::instance(SGT);

		if (!api_mgr.init()) {
			api_mgr.write_log(__FILE__, __LINE__, (_("ERROR: Can't login on ServiceGalaxy.")).str(APPLOCALE));
			return retval;
		}

		// 需要在init()之后才能获取
		if (config_file.empty()) {
			string key = string("SCHD.") + schd_key;

			// 获取scheduler配置文件
			if (!api_mgr.get_config(config, key, schd_version)) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: Failure to load scheduler configuration")).str(APPLOCALE));
				return retval;
			}

			schd_config config_mgr;
			config_mgr.load_xml(config);
		}

		SCHD->pmid2mid(SGT);
	} catch (exception& ex) {
		GPP->write_log(__FILE__, __LINE__, ex.what());
		std::cout << ex.what() << std::endl;
		return retval;
	}

	// 与JMS建立连接
	SCHD->connect();

	retval = 0;
	return retval;
}

int schd_clt::run()
{
	int retval = -1;
#if defined(DEBUG)
	scoped_debug<int> debug(500, __PRETTY_FUNCTION__, "", &retval);
#endif
	time_t last_time = time(0);

	if (!init_agent())
		return retval;

	drain();

	while (1) {
		check_ready();

		if (!run_ready())
			return retval;

		if (!run_undo())
			return retval;

		if (!check_status()) {
			if (!run_increase())
				return retval;

			receive(SCHD->_SCHD_polltime);
		} else {
			receive(0);
		}

		time_t current = time(0);
		if (current - last_time >= SCHD->_SCHD_polltime * SCHD->_SCHD_robustint) {
			check_pending();
			last_time = current;
		}

		if (SCHD->_SCHD_shutdown) {
			SGT->_SGT_error_code = SGGOTSIG;
			return retval;
		}

		// 尝试重新连接
		if (!SCHD->_SCHD_connected && time(0) >= SCHD->_SCHD_retry_time) {
			SCHD->connect();
			if (SCHD->_SCHD_connected)
				report_status();
		}
	}

	retval = 0;
	return retval;
}

// 报告调度程序本身的状态变化
void schd_clt::report(int status, const string& text)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, (_("status={1}, text={2}") % status % text).str(APPLOCALE), NULL);
#endif

	if (!SCHD->_SCHD_connected)
		return;

	try {
		bpt::iptree opt;
		bpt::iptree v_pt;
		ostringstream os;

		v_pt.put("type", SCHD_RPLY_SCHD_STATUS);
		v_pt.put("hostname", GPP->uname());
		v_pt.put("pid", getpid());
		v_pt.put("status", status);
		v_pt.put("message", text);

		opt.add_child("info", v_pt);
		bpt::write_xml(os, opt);

		auto_ptr<cms::TextMessage> auto_msg(SCHD->_SCHD_session->createTextMessage(os.str()));
		cms::TextMessage *msg = auto_msg.get();

		SCHD->_SCHD_producer->send(msg);
	} catch (cms::CMSException& ex) {
		GPP->write_log(__FILE__, __LINE__, ex.what());
	}
}

// 初始化agent
bool schd_clt::init_agent()
{
	bool retval = false;
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", &retval);
#endif

	schd_rpc& rpc_mgr = schd_rpc::instance();
	schd_policy& policy_mgr = schd_policy::instance();

	// 初始化所有节点
	for (map<string, schd_cfg_entry_t>::const_iterator iter = SCHD->_SCHD_cfg_entries.begin(); iter != SCHD->_SCHD_cfg_entries.end(); ++iter) {
		const schd_cfg_entry_t& cfg_entry = iter->second;
		schd_entry_t& entry = SCHD->_SCHD_entries[iter->first];

		entry.policy = cfg_entry.policy;
		entry.retries = cfg_entry.maxgen;
		entry.min = cfg_entry.min;
		entry.max = cfg_entry.max;
	}

	if (!SCHD->_SCHD_resume) {
		// 重置状态信息
		policy_mgr.reset();

		BOOST_FOREACH(const int& mid, SCHD->_SCHD_mids) {
			if (!rpc_mgr.do_init(mid))
				return retval;
		}

		// 发送初始化请求
		rpc_mgr.report();
	} else {
		// 加载状态信息
		policy_mgr.load();

		BOOST_FOREACH(const int& mid, SCHD->_SCHD_mids) {
			if (!rpc_mgr.do_status(mid))
				return retval;
		}

		// 修正状态信息
		for (map<string, schd_entry_t>::iterator entry_iter = SCHD->_SCHD_entries.begin(); entry_iter != SCHD->_SCHD_entries.end(); ++entry_iter) {
			schd_entry_t& entry = entry_iter->second;

			if (entry.processes == 0) {
				if (entry.status & SCHD_CLEANING) {
					if (entry.status & (SCHD_TERMINATED | SCHD_STOPPED)) {
						entry.status &= ~SCHD_CLEANING;
					} else {
						if (entry.status & SCHD_ICLEAN) {
							entry.status &= ~SCHD_ICLEAN;
							entry.status |= SCHD_PENDING;
						} else {
							entry.status &= ~SCHD_CLEANING;
							if (!(entry.status & (SCHD_TERMINATED | SCHD_STOPPED)))
								rpc_mgr.do_erase(entry_iter->first);
						}
					}
				} else if (entry.status & SCHD_RUNNING) {
					entry.status &= ~SCHD_RUNNING;
					if (!(entry.status & (SCHD_FINISH | SCHD_TERMINATED | SCHD_STOPPED)))
						entry.status |= SCHD_INIT;
				}
			}

			if (entry.status & SCHD_STOPPED)
				entry.status &= ~(SCHD_INIT | SCHD_READY | SCHD_RUNNING | SCHD_FINISH | SCHD_TERMINATED);

			if (entry.status & (SCHD_RUNNING | SCHD_FINISH | SCHD_TERMINATED))
				entry.status &= ~(SCHD_INIT | SCHD_READY);
		}
	}

	report_status();
	retval = true;
	return retval;
}

// 检查是否有可运行的节点
void schd_clt::check_ready()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	schd_rpc& rpc_mgr = schd_rpc::instance();
	schd_policy& policy_mgr = schd_policy::instance();

	for (map<string, schd_entry_t>::iterator iter = SCHD->_SCHD_entries.begin(); iter != SCHD->_SCHD_entries.end(); ++iter) {
		const string& entry_name = iter->first;
		schd_entry_t& entry = iter->second;

		if (!(entry.status & SCHD_INIT))
			continue;

		bool all_done = true;
		map<string, vector<string> >::iterator diter = SCHD->_SCHD_deps.find(entry_name);

		if (diter != SCHD->_SCHD_deps.end()) {
			BOOST_FOREACH(const string& dname, diter->second) {
				map<string, schd_entry_t>::iterator entry_iter = SCHD->_SCHD_entries.find(dname);
				if (entry_iter == SCHD->_SCHD_entries.end()) {
					GPP->write_log(__FILE__, __LINE__, (_("FATAL: Can't find configuration for {1}") % dname).str(APPLOCALE));
					exit(1);
				}

				const schd_entry_t& entry= entry_iter->second;
				if ((entry.status & SCHD_STATUS_MASK & ~SCHD_ICLEAN) != SCHD_FINISH) {
					all_done = false;
					break;
				}
			}
		}

		if (all_done) {
			map<string, schd_cfg_entry_t>::const_iterator cfg_iter = SCHD->_SCHD_cfg_entries.find(entry_name);
			if (cfg_iter == SCHD->_SCHD_cfg_entries.end()) {
				GPP->write_log(__FILE__, __LINE__, (_("FATAL: Can't find configuration for {1}") % entry_name).str(APPLOCALE));
				exit(1);
			}

			const schd_cfg_entry_t& cfg_entry = cfg_iter->second;
			if (entry.file_count == 0) {
				BOOST_FOREACH(const schd_directory_t& item, cfg_entry.directories)
					entry.file_count += SCHD->count_files(item.directory, cfg_entry.pattern, item.sftp_prefix);

				entry.file_pending = entry.file_count;
			}

			entry.status &= ~SCHD_INIT;
			entry.status |= SCHD_READY;

			// 报告节点变更
			policy_mgr.save(entry_name, entry);
			rpc_mgr.report(entry_name, entry);
		}
	}
}

// 启动可运行的节点
bool schd_clt::run_ready()
{
	bool retval = false;
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", &retval);
#endif

	gpenv& env_mgr = gpenv::instance();
	schd_rpc& rpc_mgr = schd_rpc::instance();
	schd_policy& policy_mgr = schd_policy::instance();

	if (SCHD->_SCHD_running_entries >= SCHD->_SCHD_parallel_entries) {
		retval = true;
		return retval;
	}

	for (map<string, schd_entry_t>::iterator iter = SCHD->_SCHD_entries.begin(); iter != SCHD->_SCHD_entries.end(); ++iter) {
		const string& entry_name = iter->first;
		schd_entry_t& entry = iter->second;

		if (!(entry.status & SCHD_READY))
			continue;

		if (entry.policy == SCHD_POLICY_BYHAND)
			continue;

		map<string, schd_cfg_entry_t>::const_iterator cfg_iter = SCHD->_SCHD_cfg_entries.find(entry_name);
		if (cfg_iter == SCHD->_SCHD_cfg_entries.end()) {
			GPP->write_log(__FILE__, __LINE__, (_("FATAL: Can't find configuration for {1}") % entry_name).str(APPLOCALE));
			exit(1);
		}

		const schd_cfg_entry_t& cfg_entry = cfg_iter->second;
		sgc_ctx *SGC = SGT->SGC();

		map<string, vector<schd_condition_t> >::const_iterator cond_iter = SCHD->_SCHD_conditions.find(entry_name);
		if (cond_iter != SCHD->_SCHD_conditions.end()) {
			const vector<schd_condition_t>& conditions = cond_iter->second;
			bool matched = true;

			BOOST_FOREACH(const schd_condition_t& condition, conditions) {
				map<string, schd_entry_t>::iterator cond_iter = SCHD->_SCHD_entries.find(condition.entry_name);
				if (cond_iter == SCHD->_SCHD_entries.end())
					continue;

				schd_entry_t& cond_entry = cond_iter->second;
				int status = cond_entry.status & SCHD_STATUS_MASK & ~SCHD_ICLEAN;
				if (condition.finish && status != SCHD_FINISH) {
					if (!(cond_entry.status & SCHD_FLAG_LOGGED)) {
						cond_entry.status |= SCHD_FLAG_LOGGED;
						GPP->write_log(__FILE__, __LINE__, (_("WARN: Waiting for entry {1} to finish") % condition.entry_name).str(APPLOCALE));
					}
					matched = false;
					break;
				} else if (!condition.finish && !(status & (SCHD_INIT | SCHD_READY))) {
					if (!(cond_entry.status & SCHD_FLAG_LOGGED)) {
						cond_entry.status |= SCHD_FLAG_LOGGED;
						GPP->write_log(__FILE__, __LINE__, (_("WARN: Waiting for entry {1} to init") % condition.entry_name).str(APPLOCALE));
					}
					matched = false;
					break;
				}
			}

			if (!matched)
				continue;
		}

		map<string, vector<string> >::const_iterator action_iter = SCHD->_SCHD_actions.find(entry_name);
		if (action_iter != SCHD->_SCHD_actions.end()) {
			const vector<string>& affected_names = action_iter->second;
			bool matched = true;

			BOOST_FOREACH(const string& affected_name, affected_names) {
				map<string, schd_entry_t>::iterator affected_iter = SCHD->_SCHD_entries.find(affected_name);
				if (affected_iter == SCHD->_SCHD_entries.end())
					continue;

				schd_entry_t& affected_entry = affected_iter->second;
				int status = affected_entry.status & SCHD_STATUS_MASK & ~SCHD_ICLEAN;
				if (status == SCHD_INIT) {
					continue;
				} else if (status == SCHD_READY) {
					affected_entry.status &= ~SCHD_READY;
					affected_entry.status |= SCHD_INIT;
					continue;
				} else if (status != SCHD_FINISH) {
					if (!(affected_entry.status & SCHD_FLAG_LOGGED)) {
						affected_entry.status |= SCHD_FLAG_LOGGED;
						GPP->write_log(__FILE__, __LINE__, (_("ERROR: status of affected entry {1} should be FINISH") % affected_name).str(APPLOCALE));
					}
					matched = false;
					break;
				}

				if (!rpc_mgr.do_mark(affected_name)) {
					GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't mark entry {1}") % affected_name).str(APPLOCALE));
					matched = false;
					break;
				}

				affected_entry.policy = SCHD_POLICY_BYHAND;
				affected_entry.status &= ~(SCHD_FINISH | SCHD_FLAG_LOGGED);
				affected_entry.status |= SCHD_INIT;

				// 报告节点变更
				policy_mgr.save(affected_name, affected_entry);
				rpc_mgr.report(affected_name);
			}

			if (!matched)
				continue;
		}

		entry.status &= ~(SCHD_READY | SCHD_FLAG_LOGGED);
		entry.status |= SCHD_RUNNING | SCHD_FLAG_WATCH;
		for (int j = 0; j < cfg_entry.min; j++) {
			for (int i = 0; i < cfg_entry.mids.size(); i++) {
				string command;
				int mid;
				string real_command;
				string env_str = (boost::format("SGSCHDID=%1%") % (j + 1)).str();
				env_mgr.putenv(env_str.c_str());
				env_mgr.expand_var(real_command, cfg_entry.command);

				if (cfg_entry.mids[i] == BADMID) {
					command = "schd_agent";
					if (!cfg_entry.usrname.empty()) {
						command += " -u ";
						command += cfg_entry.usrname;
					}
					command += " -h ";
					command += cfg_entry.hostnames[i];
					command += " -e \"";
					command += real_command;
					command += "\" -t ";
					command += boost::lexical_cast<string>(SCHD->_SCHD_polltime);
					mid = SGC->_SGC_proc.mid;
				} else if (cfg_entry.enable_cproxy) {
					command = "schd_agent -e \"";
					command += real_command;
					command += "\" -t ";
					command += boost::lexical_cast<string>(SCHD->_SCHD_polltime);
					mid = cfg_entry.mids[i];
				} else {
					command = real_command;
					mid = cfg_entry.mids[i];
				}

				if (!rpc_mgr.do_start(mid, entry_name, cfg_entry.hostnames[i], command)) {
					entry.status |= SCHD_TERMINATED;
					entry.retries = 0;
				} else {
					// 调整进程数
					entry.processes++;
					SCHD->inc_proc(cfg_entry.hostnames[i], entry_name);
				}
			}
		}

		if (entry.processes == 0)
			entry.status &= ~SCHD_RUNNING;
		else
			++SCHD->_SCHD_running_entries;

		// 报告节点变更
		policy_mgr.save(entry_name, entry);
		rpc_mgr.report(entry_name, entry);

		if (SCHD->_SCHD_running_entries == SCHD->_SCHD_parallel_entries)
			break;
	}

	retval = true;
	return retval;
}

// 在已启动的节点上增加进程
bool schd_clt::run_increase()
{
	bool retval = false;
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", &retval);
#endif

	gpenv& env_mgr = gpenv::instance();
	schd_rpc& rpc_mgr = schd_rpc::instance();
	schd_policy& policy_mgr = schd_policy::instance();

	for (map<string, int>::const_iterator host_iter = SCHD->_SCHD_host_procs.begin(); host_iter != SCHD->_SCHD_host_procs.end(); ++host_iter) {
		const string& hostname = host_iter->first;

		// 指定主机已经启动了足够的进程
		if (host_iter->second >= SCHD->_SCHD_parallel_procs)
			continue;

		// 可启动的节点排序列表
		multimap<int, string> start_map;

		for (map<string, schd_entry_t>::iterator entry_iter = SCHD->_SCHD_entries.begin(); entry_iter != SCHD->_SCHD_entries.end(); ++entry_iter) {
			const string& entry_name = entry_iter->first;
			const schd_entry_t& entry = entry_iter->second;

			if (!(entry.status & SCHD_RUNNING))
				continue;

			// 只要有进程异常或主动停止，都不再自动增加
			if (entry.status & (SCHD_TERMINATED | SCHD_STOPPED))
				continue;

			schd_host_entry_t host_entry;
			host_entry.hostname = hostname;
			host_entry.entry_name = entry_name;
			set<schd_host_entry_t>::const_iterator iter = SCHD->_SCHD_entry_procs.find(host_entry);

			// 该节点在指定主机上不存在
			if (iter == SCHD->_SCHD_entry_procs.end())
				continue;

			if (!iter->startable)
				continue;

			if (iter->processes >= entry.max)
				continue;

			int additional = iter->processes - entry.min;
			start_map.insert(std::pair<int, string>(additional, entry_name));
		}

		// 最大可启动个数
		int max_start = SCHD->_SCHD_parallel_procs - host_iter->second;

		for (multimap<int, string>::const_iterator start_iter = start_map.begin(); start_iter != start_map.end(); ++start_iter) {
			const string& entry_name = start_iter->second;

			map<string, schd_entry_t>::iterator entry_iter = SCHD->_SCHD_entries.find(entry_name);
			if (entry_iter == SCHD->_SCHD_entries.end()) {
				GPP->write_log(__FILE__, __LINE__, (_("FATAL: Can't find configuration for {1}") % entry_name).str(APPLOCALE));
				exit(1);
			}

			schd_entry_t& entry = entry_iter->second;

			map<string, schd_cfg_entry_t>::const_iterator cfg_iter = SCHD->_SCHD_cfg_entries.find(entry_name);
			if (cfg_iter == SCHD->_SCHD_cfg_entries.end()) {
				GPP->write_log(__FILE__, __LINE__, (_("FATAL: Can't find configuration for {1}") % entry_name).str(APPLOCALE));
				exit(1);
			}

			const schd_cfg_entry_t& cfg_entry = cfg_iter->second;

			sgc_ctx *SGC = SGT->SGC();
			string command;
			int i;
			int mid;
			string real_command;

			// 获取已启动进程数，以设置环境变量
			schd_host_entry_t host_entry;
			host_entry.hostname = hostname;
			host_entry.entry_name = entry_name;
			set<schd_host_entry_t, int>::const_iterator iter = SCHD->_SCHD_entry_procs.find(host_entry);

			// 该节点在指定主机上不存在
			if (iter == SCHD->_SCHD_entry_procs.end())
				continue;

			string env_str = (boost::format("SGSCHDID=%1%") % (iter->processes + 1)).str();
			env_mgr.putenv(env_str.c_str());
			env_mgr.expand_var(real_command, cfg_entry.command);

			for (i = 0; i < cfg_entry.hostnames.size(); i++) {
				if (cfg_entry.hostnames[i] == hostname)
					break;
			}

			// 该节点不能在指定主机启动
			if (i == cfg_entry.hostnames.size())
				continue;

			if (cfg_entry.mids[i] == BADMID) {
				command = "schd_agent";
				if (!cfg_entry.usrname.empty()) {
					command += " -u ";
					command += cfg_entry.usrname;
				}
				command += " -h ";
				command += cfg_entry.hostnames[i];
				command += " -e \"";
				command += real_command;
				command += "\" -t ";
				command += boost::lexical_cast<string>(SCHD->_SCHD_polltime);
				mid = SGC->_SGC_proc.mid;
			} else if (cfg_entry.enable_cproxy) {
				command = "schd_agent -e \"";
				command += real_command;
				command += "\" -t ";
				command += boost::lexical_cast<string>(SCHD->_SCHD_polltime);
				mid = cfg_entry.mids[i];
			} else {
				command = real_command;
				mid = cfg_entry.mids[i];
			}

			if (!rpc_mgr.do_start(mid, entry_name, cfg_entry.hostnames[i], command)) {
				entry.status |= SCHD_TERMINATED;
				entry.retries = 0;
			} else {
				// 调整进程数
				entry.processes++;
				SCHD->inc_proc(hostname, entry_name);
			}

			// 报告节点变更
			policy_mgr.save(entry_name, entry);
			rpc_mgr.report(entry_name, entry);

			if (--max_start == 0)
				break;
		}
	}

	retval = true;
	return retval;
}


// 启动可回退的节点
bool schd_clt::run_undo()
{
	bool retval = false;
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", &retval);
#endif

	schd_rpc& rpc_mgr = schd_rpc::instance();
	schd_policy& policy_mgr = schd_policy::instance();

	for (map<string, schd_entry_t>::iterator iter = SCHD->_SCHD_entries.begin(); iter != SCHD->_SCHD_entries.end(); ++iter) {
		const string& entry_name = iter->first;
		schd_entry_t& entry = iter->second;

		if (!(entry.status & SCHD_PENDING))
			continue;

		bool all_done = true;
		map<string, vector<string> >::iterator diter = SCHD->_SCHD_rdeps.find(entry_name);

		if (diter != SCHD->_SCHD_rdeps.end()) {
			BOOST_FOREACH(const string& dname, diter->second) {
				map<string, schd_entry_t>::iterator entry_iter = SCHD->_SCHD_entries.find(dname);
				if (entry_iter == SCHD->_SCHD_entries.end()) {
					GPP->write_log(__FILE__, __LINE__, (_("FATAL: Can't find configuration for {1}") % dname).str(APPLOCALE));
					exit(1);
				}

				schd_entry_t& dentry = entry_iter->second;
				int status = dentry.status & SCHD_STATUS_MASK & ~SCHD_ICLEAN;
				if (status != SCHD_INIT && status != SCHD_FINISH) {
					// 已去掉清理标记，则表示有异常
					if (!(status & (SCHD_PENDING | SCHD_CLEANING))) {
						entry.status |= SCHD_TERMINATED;
						entry.status &= ~(SCHD_PENDING | SCHD_CLEANING);

						// 报告节点变更
						policy_mgr.save(entry_name, entry);
						rpc_mgr.report(entry_name, entry);
					}

					all_done = false;
					break;
				}
			}
		}

		if (all_done) {
			map<string, schd_cfg_entry_t>::const_iterator cfg_iter = SCHD->_SCHD_cfg_entries.find(entry_name);
			if (cfg_iter == SCHD->_SCHD_cfg_entries.end()) {
				GPP->write_log(__FILE__, __LINE__, (_("FATAL: Can't find configuration for {1}") % entry_name).str(APPLOCALE));
				exit(1);
			}

			const schd_cfg_entry_t& cfg_entry = cfg_iter->second;
			entry.status &= ~(SCHD_FINISH | SCHD_TERMINATED | SCHD_STOPPED | SCHD_PENDING);
			entry.status |= SCHD_CLEANING;
			policy_mgr.save(entry_name, entry);
			rpc_mgr.report(entry_name, entry);

			if (entry.status & SCHD_ICLEAN) {
				if (cfg_entry.iclean.empty()) {
					entry.status &= ~SCHD_ICLEAN;
				} else if (!rpc_mgr.do_iclean(entry_name, entry, cfg_entry)) {
					entry.status |= SCHD_TERMINATED;
					entry.status &= ~SCHD_CLEANING;
				}

				// 报告节点变更
				policy_mgr.save(entry_name, entry);
				rpc_mgr.report(entry_name, entry);

				if (!cfg_entry.iclean.empty())
					continue;
			}

			if (cfg_entry.undo.empty()) {
				if (!rpc_mgr.do_erase(entry_name)) {
					entry.status |= SCHD_TERMINATED;
					entry.status &= ~SCHD_CLEANING;
				} else {
					entry.status = SCHD_INIT;
				}
			} else if (!rpc_mgr.do_undo(entry_name, entry, cfg_entry, entry.undo_id)) {
				entry.status |= SCHD_TERMINATED;
				entry.status &= ~SCHD_CLEANING;
			}

			// 报告节点变更
			policy_mgr.save(entry_name, entry);
			rpc_mgr.report(entry_name, entry);
		}
	}

	retval = true;
	return retval;
}

// 检查节点状态是否有变化，有变化则返回true
bool schd_clt::check_status()
{
	bool retval = false;
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", &retval);
#endif

	schd_rpc& rpc_mgr = schd_rpc::instance();

	BOOST_FOREACH(const int& mid, SCHD->_SCHD_mids) {
		// 检查并获取各个节点的状态，并记录是否变化
		if (rpc_mgr.do_check(mid))
			retval = true;
	}

	return retval;
}

// 清空请求队列
void schd_clt::drain()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	if (!SCHD->_SCHD_connected)
		return;

	while (SCHD->_SCHD_consumer->receiveNoWait())
		;
}

// 接收JMS请求
void schd_clt::receive(int seconds)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, (_("seconds={1}") % seconds).str(APPLOCALE), NULL);
#endif

	if (!SCHD->_SCHD_connected)
		return;

	time_t last_time = 0;
	while (1) {
		cms::TextMessage *msg;

		if (seconds <= 0) {
			msg = reinterpret_cast<cms::TextMessage *>(SCHD->_SCHD_consumer->receiveNoWait());
		} else {
			if (last_time == 0)
				last_time = time(0);
			msg = reinterpret_cast<cms::TextMessage *>(SCHD->_SCHD_consumer->receive(seconds * 1000));
			seconds -= (time(0) - last_time);
		}

		if (msg == NULL)
			return;

		try {
			bpt::iptree pt;
			istringstream is(msg->getText());
			bpt::read_xml(is, pt);
			int type = pt.get<int>("schd.type");

#if defined(SCHD_DEBUG)
			cout << "received message from JMS, timestamp=" << time(0) << ", type=" << type << std::endl;
#endif

			switch (type) {
			case SCHD_RQST_STATUS:
				report_status();
				break;
			case SCHD_RQST_REDO:
				do_redo(pt);
				break;
			case SCHD_RQST_POLICY:
				change_policy(pt);
				break;
			case SCHD_RQST_STOP:
				do_stop(pt);
				break;
			default:
				break;
			}
		} catch (cms::CMSException& ex) {
			GPP->write_log(__FILE__, __LINE__, ex.what());
		}
	}
}

// 检查文件积压数
void schd_clt::check_pending()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	schd_rpc& rpc_mgr = schd_rpc::instance();
	schd_policy& policy_mgr = schd_policy::instance();

	for (map<string, schd_entry_t>::iterator iter = SCHD->_SCHD_entries.begin(); iter != SCHD->_SCHD_entries.end(); ++iter) {
		const string& entry_name = iter->first;
		schd_entry_t& entry = iter->second;

		if (!(entry.status & SCHD_FLAG_WATCH))
			continue;

		map<string, schd_cfg_entry_t>::const_iterator cfg_iter = SCHD->_SCHD_cfg_entries.find(entry_name);
		if (cfg_iter == SCHD->_SCHD_cfg_entries.end()) {
			GPP->write_log(__FILE__, __LINE__, (_("FATAL: Can't find configuration for {1}") % entry_name).str(APPLOCALE));
			exit(1);
		}

		const schd_cfg_entry_t& cfg_entry = cfg_iter->second;

		int file_pending = 0;
		BOOST_FOREACH(const schd_directory_t& item, cfg_entry.directories)
			file_pending += SCHD->count_files(item.directory, cfg_entry.pattern, item.sftp_prefix);

		if (file_pending != entry.file_pending) {
			entry.file_pending = file_pending;
			rpc_mgr.report(entry_name, entry);
		}

		if (!(entry.status & SCHD_RUNNING))
			entry.status &= ~SCHD_FLAG_WATCH;

		if (entry.status == SCHD_FINISH) {
			if (!cfg_entry.autoclean.empty()) {
				rpc_mgr.do_autoclean(entry_name, entry, cfg_entry, 1);
			}
		}
		policy_mgr.save(entry_name, entry);
	}
}

// 报告节点和进程状态
void schd_clt::report_status()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	schd_rpc& rpc_mgr = schd_rpc::instance();

	// 节点状态
	for (map<string, schd_entry_t>::iterator iter = SCHD->_SCHD_entries.begin(); iter != SCHD->_SCHD_entries.end(); ++iter) {
		rpc_mgr.report(iter->first, iter->second);
	}

	// 进程状态
	BOOST_FOREACH(const schd_proc_t& proc, SCHD->_SCHD_procs) {
		rpc_mgr.report(proc);
	}
}

// 执行重做操作
void schd_clt::do_redo(const bpt::iptree& ipt)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	set<string> done_entries;
	set<string> entry_names;

	BOOST_FOREACH(const bpt::iptree::value_type& v_pt, ipt.get_child("schd.entries")) {
		if (v_pt.first != "entry_name")
			continue;

		string entry_name = v_pt.second.get_value<string>();

#if defined(SCHD_DEBUG)
		cout << "timestamp=" << time(0) << ", entry_name=" << entry_name << std::endl;
#endif

		map<string, schd_entry_t>::iterator entry_iter = SCHD->_SCHD_entries.find(entry_name);
		if (entry_iter == SCHD->_SCHD_entries.end()) {
			GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't find configuration for {1}") % entry_name).str(APPLOCALE));
			return;
		}

		if (entry_iter->second.status & SCHD_CLEANING) {
			GPP->write_log(__FILE__, __LINE__, (_("ERROR: redo is not allowed since entry {1} is in clean") % entry_name).str(APPLOCALE));
			return;
		}

		entry_names.insert(entry_name);
	}

	// 检查进程是否满足条件
	if (check_dep(entry_names)) {
		// 停止进程
		stop_dep(entry_names);

		// 回退所有节点
		do_undo(done_entries, entry_names, static_cast<int>(time(0)));
	}

	// 报告变更
	schd_rpc& rpc_mgr = schd_rpc::instance();
	BOOST_FOREACH(const string& entry_name, done_entries) {
		map<string, schd_entry_t>::iterator entry_iter = SCHD->_SCHD_entries.find(entry_name);
		if (entry_iter == SCHD->_SCHD_entries.end())
			continue;

		rpc_mgr.report(entry_iter->first, entry_iter->second);
	}
}

// 更新执行策略
void schd_clt::change_policy(const bpt::iptree& ipt)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	int policy = ipt.get<int>("schd.policy");
	if (policy != SCHD_POLICY_AUTO && policy != SCHD_POLICY_BYHAND) {
		GPP->write_log(__FILE__, __LINE__, (_("ERROR: policy {1} invalid") % policy).str(APPLOCALE));
		return;
	}

	BOOST_FOREACH(const bpt::iptree::value_type& v_pt, ipt.get_child("schd.entries")) {
		if (v_pt.first != "entry_name")
			continue;

		string entry_name = v_pt.second.get_value<string>();

#if defined(SCHD_DEBUG)
		cout << "timestamp=" << time(0) << ", policy=" << policy << ", entry_name=" << entry_name << std::endl;
#endif

		map<string, schd_entry_t>::iterator entry_iter = SCHD->_SCHD_entries.find(entry_name);
		if (entry_iter == SCHD->_SCHD_entries.end()) {
			GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't find configuration for {1}") % entry_name).str(APPLOCALE));
			return;
		}

		schd_policy& policy_mgr = schd_policy::instance();
		schd_rpc& rpc_mgr = schd_rpc::instance();

		schd_entry_t& entry = entry_iter->second;
		entry.policy = policy;
		if (policy == SCHD_POLICY_AUTO && entry.processes == 0
			&& entry.status != SCHD_INIT && entry.status != SCHD_READY && entry.status != SCHD_FINISH) {
			if (!rpc_mgr.do_mark(entry_name))
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't mark entry {1}") % entry_name).str(APPLOCALE));

			entry.status = SCHD_INIT;

			map<string, schd_cfg_entry_t>::const_iterator cfg_iter = SCHD->_SCHD_cfg_entries.find(entry_name);
			if (cfg_iter == SCHD->_SCHD_cfg_entries.end()) {
				GPP->write_log(__FILE__, __LINE__, (_("FATAL: Can't find configuration for {1}") % entry_name).str(APPLOCALE));
				exit(1);
			}

			const schd_cfg_entry_t& cfg_entry = cfg_iter->second;
			entry.retries = cfg_entry.maxgen;
		}

		// 保存变更
		policy_mgr.save(entry_name, entry);

		// 报告变更
		rpc_mgr.report(entry_name, entry);
	}
}

// 停止进程
void schd_clt::do_stop(const bpt::iptree& ipt)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	BOOST_FOREACH(const bpt::iptree::value_type& v_pt, ipt.get_child("schd.entries")) {
		if (v_pt.first != "entry_name")
			continue;

		string entry_name = v_pt.second.get_value<string>();

#if defined(SCHD_DEBUG)
		cout << "timestamp=" << time(0) << ", entry_name=" << entry_name << std::endl;
#endif

		map<string, schd_entry_t>::iterator entry_iter = SCHD->_SCHD_entries.find(entry_name);
		if (entry_iter == SCHD->_SCHD_entries.end()) {
			GPP->write_log(__FILE__, __LINE__, (_("ERROR: entry_name {1} not found in configuration") % entry_name).str(APPLOCALE));
			return;
		}

		schd_entry_t& entry = entry_iter->second;
		schd_rpc& rpc_mgr = schd_rpc::instance();
		schd_policy& policy_mgr = schd_policy::instance();

		stop_entry(entry_name, entry);

		// 即使没有变更，也需要应答
		policy_mgr.save(entry_name, entry);
		rpc_mgr.report(entry_name, entry);
	}
}

// 检查依赖的所有进程
bool schd_clt::check_dep(const set<string>& entry_names)
{
	bool retval = false;
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", &retval);
#endif
	map<string, schd_entry_t>::iterator entry_iter;
	map<string, vector<string> >::const_iterator deps_iter;

	BOOST_FOREACH(const string& entry_name, entry_names) {
		// 检查反向依赖的节点是否状态正确
		deps_iter = SCHD->_SCHD_rdeps.find(entry_name);
		if (deps_iter != SCHD->_SCHD_rdeps.end()) {
			BOOST_FOREACH(const string& rdep_name, deps_iter->second) {
				if (entry_names.find(rdep_name) != entry_names.end())
					continue;

				entry_iter = SCHD->_SCHD_entries.find(rdep_name);
				if (entry_iter == SCHD->_SCHD_entries.end()) {
					GPP->write_log(__FILE__, __LINE__, (_("FATAL: Can't find configuration for {1}") % rdep_name).str(APPLOCALE));
					exit(1);
				}

				schd_entry_t& rdep_entry = entry_iter->second;
				int status = rdep_entry.status & SCHD_STATUS_MASK & ~SCHD_ICLEAN;
				if (status != SCHD_INIT && status != SCHD_READY && status != SCHD_FINISH) {
					GPP->write_log(__FILE__, __LINE__, (_("ERROR: status for entry_name {1} should be init, ready or finish") % rdep_name).str(APPLOCALE));
					return retval;
				}
			}
		}

		entry_iter = SCHD->_SCHD_entries.find(entry_name);
		if (entry_iter == SCHD->_SCHD_entries.end()) {
			GPP->write_log(__FILE__, __LINE__, (_("FATAL: Can't find configuration for {1}") % entry_name).str(APPLOCALE));
			exit(1);
		}

		// 如果当前节点未启动，则对依赖的节点无要求
		schd_entry_t& entry = entry_iter->second;
		if (entry.status & (SCHD_INIT | SCHD_READY))
			continue;

		// 检查依赖的节点是否状态正确
		deps_iter = SCHD->_SCHD_deps.find(entry_name);
		if (deps_iter != SCHD->_SCHD_deps.end()) {
			BOOST_FOREACH(const string& dep_name, deps_iter->second) {
				if (entry_names.find(dep_name) != entry_names.end())
					continue;

				entry_iter = SCHD->_SCHD_entries.find(dep_name);
				if (entry_iter == SCHD->_SCHD_entries.end()) {
					GPP->write_log(__FILE__, __LINE__, (_("FATAL: Can't find configuration for {1}") % dep_name).str(APPLOCALE));
					exit(1);
				}

				schd_entry_t& dep_entry = entry_iter->second;
				int status = dep_entry.status & SCHD_STATUS_MASK & ~SCHD_ICLEAN;
				if (status != SCHD_FINISH) {
					GPP->write_log(__FILE__, __LINE__, (_("ERROR: status for entry_name {1} should be init, ready or finish") % dep_name).str(APPLOCALE));
					return retval;
				}
			}
		}
	}

	retval = true;
	return retval;
}

// 停止依赖的所有进程
void schd_clt::stop_dep(const set<string>& entry_names)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	BOOST_FOREACH(const string& entry_name, entry_names) {
		map<string, schd_entry_t>::iterator entry_iter = SCHD->_SCHD_entries.find(entry_name);
		if (entry_iter == SCHD->_SCHD_entries.end()) {
			GPP->write_log(__FILE__, __LINE__, (_("FATAL: Can't find configuration for {1}") % entry_name).str(APPLOCALE));
			exit(1);
		}

		stop_entry(entry_name, entry_iter->second);
	}
}

// 回退依赖的所有进程
void schd_clt::do_undo(set<string>& done_entries, const set<string>& entry_names, int undo_id)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, (_("undo_id={1}") % undo_id).str(APPLOCALE), NULL);
#endif

	schd_policy& policy_mgr = schd_policy::instance();

	BOOST_FOREACH(const string& entry_name, entry_names) {
		// 直接后续需要标记为清理输入
		map<string, vector<string> >::const_iterator iter = SCHD->_SCHD_rdeps.find(entry_name);
		if (iter != SCHD->_SCHD_rdeps.end()) {
			BOOST_FOREACH(const string& rdep_name, iter->second) {
				if (entry_names.find(rdep_name) != entry_names.end())
					continue;

				map<string, schd_entry_t>::iterator entry_iter = SCHD->_SCHD_entries.find(rdep_name);
				if (entry_iter == SCHD->_SCHD_entries.end()) {
					GPP->write_log(__FILE__, __LINE__, (_("FATAL: Can't find configuration for {1}") % rdep_name).str(APPLOCALE));
					exit(1);
				}

				schd_entry_t& entry = entry_iter->second;
				if (entry.status & SCHD_INIT)
					continue;

				if (entry.status & SCHD_READY) {
					entry.status &= ~SCHD_READY;
					entry.status |= SCHD_INIT;
					entry.file_count = 0;
					entry.file_pending = 0;
				} else {
					// 不在需要重做的列表中，需要清理输入
					entry.status |= SCHD_ICLEAN;
				}

				policy_mgr.save(rdep_name, entry);
				done_entries.insert(rdep_name);
			}
		}

		map<string, schd_entry_t>::iterator entry_iter = SCHD->_SCHD_entries.find(entry_name);
		if (entry_iter == SCHD->_SCHD_entries.end()) {
			GPP->write_log(__FILE__, __LINE__, (_("FATAL: Can't find configuration for {1}") % entry_name).str(APPLOCALE));
			exit(1);
		}

		schd_entry_t& entry = entry_iter->second;

		map<string, schd_cfg_entry_t>::const_iterator cfg_iter = SCHD->_SCHD_cfg_entries.find(entry_name);
		if (cfg_iter == SCHD->_SCHD_cfg_entries.end()) {
			GPP->write_log(__FILE__, __LINE__, (_("FATAL: Can't find configuration for {1}") % entry_name).str(APPLOCALE));
			exit(1);
		}

		const schd_cfg_entry_t& cfg_entry = cfg_iter->second;
		entry.policy = cfg_entry.policy;
		entry.retries = cfg_entry.maxgen;
		done_entries.insert(entry_name);

		/*
		 * 标记开始执行回退操作，即使是未启动状态，也需要回退
		 * 假设后续节点做了回退，如果当前节点不做回退，把后续节点
		 * 输入目录下的文件删除，就可能导致后续节点有两份输入文件
		 * 待处理。
		 */
		entry.status |= SCHD_PENDING;
		entry.status &= ~(SCHD_INIT | SCHD_READY | SCHD_FINISH | SCHD_TERMINATED | SCHD_STOPPED);
		entry.file_count = 0;
		entry.file_pending = 0;
		entry.undo_id = undo_id;

		policy_mgr.save(entry_name, entry);
	}
}

// 停止节点
void schd_clt::stop_entry(const string& entry_name, schd_entry_t& entry)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, (_("entry_name={1}") % entry_name).str(APPLOCALE), NULL);
#endif

	schd_rpc& rpc_mgr = schd_rpc::instance();
	schd_policy& policy_mgr = schd_policy::instance();

	if (entry.status & SCHD_INIT) {
		// 标记为手动
		entry.policy = SCHD_POLICY_BYHAND;
	} else if (entry.status & SCHD_RUNNING) {
		// 标记为手动
		entry.policy = SCHD_POLICY_BYHAND;
		entry.status = SCHD_STOPPED;

		// 停止相应的进程
		BOOST_FOREACH(const schd_proc_t& proc, SCHD->_SCHD_procs) {
			if (proc.entry_name != entry_name)
				continue;

			if (proc.status != SCHD_RUNNING)
				continue;

			if (!rpc_mgr.do_stop(entry, const_cast<schd_proc_t&>(proc))) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't kill process {1} on node {2}") % proc.pid % proc.mid).str(APPLOCALE));
				exit(1);
			}
		}
	}

	policy_mgr.save(entry_name, entry);
	rpc_mgr.report(entry_name, entry);
}

}
}

using namespace ai::app;

void on_signal(int signo)
{
	schd_ctx *SCHD = schd_ctx::instance();

	SCHD->_SCHD_shutdown = true;
}

int main(int argc, char **argv)
{
	// 设置可执行文件名称
	gpp_ctx *GPP = gpp_ctx::instance();
	GPP->set_procname(argv[0]);

	signal(SIGHUP, on_signal);
	signal(SIGINT, on_signal);
	signal(SIGQUIT, on_signal);
	signal(SIGTERM, on_signal);

	int retval;
	schd_clt instance;

	try {
		instance.report(SCHD_RUNNING);

		retval = instance.init(argc, argv);
		if (retval) {
			instance.report(SCHD_TERMINATED, (_("ERROR: Can't init context")).str(APPLOCALE));
			return retval;
		}

		retval = instance.run();
		if (retval) {
			sgt_ctx *SGT = sgt_ctx::instance();
			instance.report(SCHD_TERMINATED, SGT->strerror());
			return retval;
		}

		instance.report(SCHD_FINISH);
		return 0;
	} catch (exception& ex) {
		instance.report(SCHD_TERMINATED, ex.what());
		GPP->write_log(__FILE__, __LINE__, ex.what());
		return 1;
	}
}

