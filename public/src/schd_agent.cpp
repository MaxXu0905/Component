#include "schd_agent.h"
#include "schd_rpc.h"

namespace ai
{
namespace app
{

using namespace ai::sg;
namespace po = boost::program_options;

int const DFLT_AGENT_POLLTIME = 6;
const char profile_cmd[] = "if [ -f $HOME/.bash_profile ];then . $HOME/.bash_profile;elif [ -f $HOME/.profile ];then . $HOME/.profile;fi >/dev/null 2>&1;";

boost::once_flag schd_agent::once_flag = BOOST_ONCE_INIT;
schd_agent * schd_agent::_instance = NULL;

schd_agent& schd_agent::instance()
{
	if (_instance == NULL)
		boost::call_once(once_flag, schd_agent::init_once);
	return *_instance;
}

int schd_agent::init(int argc, char **argv)
{
	int retval = -1;
#if defined(DEBUG)
	scoped_debug<int> debug(500, __PRETTY_FUNCTION__, (_("argc={1}") % argc).str(APPLOCALE), &retval);
#endif

	po::options_description desc((_("Allowed options")).str(APPLOCALE));
	desc.add_options()
		("help", (_("produce help message")).str(APPLOCALE).c_str())
		("version,v", (_("print current schd_agent version")).str(APPLOCALE).c_str())
		("background,b", (_("run schd_agent in background mode")).str(APPLOCALE).c_str())
		("usrname,u", po::value<string>(&usrname), (_("user name on which to be executed")).str(APPLOCALE).c_str())
		("hostname,h", po::value<string>(&hostname), (_("hostname on which to be executed")).str(APPLOCALE).c_str())
		("exec,e", po::value<string>(&command)->required(), (_("execute command")).str(APPLOCALE).c_str())
		("suffixURI,S", po::value<string>(&SCHD->_SCHD_suffixURI), (_("specify JMS producer/consumer topic URI suffix")).str(APPLOCALE).c_str())
		("polltime,t", po::value<int>(&polltime)->default_value(DFLT_AGENT_POLLTIME), (_("specify seconds to check alive status")).str(APPLOCALE).c_str())
	;

	po::variables_map vm;

	try {
		po::store(po::parse_command_line(argc, argv, desc), vm);

		if (vm.count("help")) {
			std::cout << desc << std::endl;
			GPP->write_log((_("INFO: {1} exit normally in help mode.") % GPP->_GPP_procname).str(APPLOCALE));
			::exit(0);
		} else if (vm.count("version")) {
			std::cout << (_("schd_agent version ")).str(APPLOCALE) << SCHD_RELEASE << std::endl;
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

	if (vm.count("background")) {
		sys_func& sys_mgr = sys_func::instance();
		sys_mgr.background();
	}

	if (usrname.empty()) {
		const char *ptr = getenv("LOGNAME");
		if (ptr == NULL) {
			GPP->write_log(__FILE__, __LINE__, (_("ERROR: environment variable LOGNAME not set")).str(APPLOCALE));
			return retval;
		}
		usrname = ptr;
	}

	if (SCHD->_SCHD_suffixURI.empty()) {
		gpenv& env_mgr = gpenv::instance();
		schd_ctx *SCHD = schd_ctx::instance();

		char *ptr = env_mgr.getenv("SASUFFIXURI");
		if (ptr == NULL) {
			GPP->write_log(__FILE__, __LINE__, (_("WARN: SASUFFIXURI environment not set, disable JMS reporting")).str(APPLOCALE));
		} else {
			SCHD->_SCHD_suffixURI = ptr;
			SCHD->set_service();
		}
	} else {
		SCHD->set_service();
	}

	// 登录到SG
	sg_api& api_mgr = sg_api::instance(SGT);
	if (!api_mgr.init()) {
		api_mgr.write_log(__FILE__, __LINE__, (_("ERROR: Can't login on ServiceGalaxy.")).str(APPLOCALE));
		return retval;
	}

	GPP->write_log((_("INFO: Login ServiceGalaxy successfully.")).str(APPLOCALE));
	retval = 0;
	return retval;
}

void schd_agent::fini()
{
#if defined(DEBUG)
	scoped_debug<int> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	GPP->write_log((_("INFO: Logout ServiceGalaxy successfully.")).str(APPLOCALE));
	api_mgr.fini();
}

int schd_agent::run()
{
	int retval = -1;
#if defined(DEBUG)
	scoped_debug<int> debug(500, __PRETTY_FUNCTION__, "", &retval);
#endif
	int ret_code;

	try {
		if (!hostname.empty()) {
			string ssh_prefix = usrname;
			ssh_prefix += "@";
			ssh_prefix += hostname;

			string real_command = profile_cmd;
			real_command += "../script/run_proc.sh ";
			real_command += command;

			{
				boost::shared_ptr<ssh_session> session(new ssh_session(ssh_prefix));
				channel_shared_ptr auto_channel(session);
				LIBSSH2_CHANNEL *channel = auto_channel.get();

				if (libssh2_channel_exec(channel, real_command.c_str()) != 0) {
					GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't execute command {1} on {2}") % real_command % ssh_prefix).str(APPLOCALE));
					return retval;
				}

				char buf[4096];
				ssize_t size = libssh2_channel_read(channel, buf, sizeof(buf) - 1);
				if (size <= 0) {
					watched_pid = -1;
				} else {
					buf[size] = '\0';
					watched_pid = atoi(buf);
				}

				int ret_code = auto_channel.get_exit_status();
				if (ret_code) {
					GPP->write_log(__FILE__, __LINE__, (_("ERROR: Failed to execute command {1} with return code {2}") % real_command % ret_code).str(APPLOCALE));
					return retval;
				}
			}

			if (watched_pid == -1) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't get remote pid")).str(APPLOCALE));
				return retval;
			}

			// 等待进程退出
			time_t expire;
			alive_rpc& alive_mgr = alive_rpc::instance(SGT);
			do {
				expire = time(0) + 1;
				if (gotsig_time == 0)
					sleep(polltime);
				else
					usleep(1000);
			} while (alive_mgr.alive(usrname, hostname, expire, watched_pid));

			// 获取进程退出码
			ret_code = get_exit();
			cleanup();
			if (ret_code) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: Failed to execute command {1} with return code {2}") % real_command % ret_code).str(APPLOCALE));
	 			retval = ret_code;
				return retval;
			}
		} else {
			sys_func& func_mgr = sys_func::instance();
			int status;

			watched_pid = ::fork();
			switch (watched_pid) {
			case 0:
				ret_code = ::execl("/bin/sh", "/bin/sh", "-c", command.c_str(), (char *)0);
				::exit(ret_code);
			case -1:
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't fork process")).str(APPLOCALE));
				return retval;
			default:
				while ((ret_code = wait(&status)) != -1 && ret_code != watched_pid)
					status = 0;
				ret_code = func_mgr.gp_status(status);
				if (ret_code) {
					GPP->write_log(__FILE__, __LINE__, (_("ERROR: Failed to execute command {1} with return code {2}") % command % ret_code).str(APPLOCALE));
					retval = ret_code;
					return retval;
				}
			}
		}

		schd_rpc& rpc_mgr = schd_rpc::instance();
		(void)rpc_mgr.report_exit();
	} catch (bad_ssh& ex) {
		// 忽略由于强制停止产生的错误
		if (!gotsig_time)
			GPP->write_log(__FILE__, __LINE__, ex.what());
		return retval;
	} catch (exception& ex) {
		GPP->write_log(__FILE__, __LINE__, ex.what());
		return retval;
	}

	retval = 0;
	return retval;
}

void schd_agent::kill(int signo)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, (_("signo={1}") % signo).str(APPLOCALE), NULL);
#endif

	// 至少间隔1秒执行一次
	if (gotsig_time != 0 && gotsig_time + polltime > time(0))
		return;

	gotsig_time = time(0);

	string ssh_prefix = usrname;
	ssh_prefix += "@";
	ssh_prefix += hostname;

	string real_command = profile_cmd;
	real_command += "../script/stop_child.sh ";
	real_command += boost::lexical_cast<string>(watched_pid);
	real_command += " ";
	real_command += boost::lexical_cast<string>(signo);

	boost::shared_ptr<ssh_session> session(new ssh_session(ssh_prefix));
	channel_shared_ptr auto_channel(session);
	LIBSSH2_CHANNEL *channel = auto_channel.get();

	if (libssh2_channel_exec(channel, real_command.c_str()) != 0)
		GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't execute command {1} on {2}") % real_command % ssh_prefix).str(APPLOCALE));

	int ret_code = auto_channel.get_exit_status();
	if (ret_code)
		GPP->write_log(__FILE__, __LINE__, (_("ERROR: Failed to execute command {1} with return code {2}") % real_command % ret_code).str(APPLOCALE));
}

schd_agent::schd_agent()
{
	SSHP = sshp_ctx::instance();
	SCHD = schd_ctx::instance();
	watched_pid = -1;
	gotsig_time = 0;
}

schd_agent::~schd_agent()
{
}

int schd_agent::get_exit()
{
	int retval = -1;
#if defined(DEBUG)
	scoped_debug<int> debug(500, __PRETTY_FUNCTION__, "", &retval);
#endif

	string ssh_prefix = usrname;
	ssh_prefix += "@";
	ssh_prefix += hostname;

	string real_command = profile_cmd;
	real_command += "cat ../script/.";
	real_command += boost::lexical_cast<string>(watched_pid);
	real_command += ";exit 0";

	boost::shared_ptr<ssh_session> session(new ssh_session(ssh_prefix));
	channel_shared_ptr auto_channel(session);
	LIBSSH2_CHANNEL *channel = auto_channel.get();

	if (libssh2_channel_exec(channel, real_command.c_str()) != 0) {
		GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't execute command {1} on {2}") % real_command % ssh_prefix).str(APPLOCALE));
		return retval;
	}

	char buf[4096];
	ssize_t size = libssh2_channel_read(channel, buf, sizeof(buf) - 1);

	int ret_code = auto_channel.get_exit_status();
	if (ret_code) {
		GPP->write_log(__FILE__, __LINE__, (_("ERROR: Failed to execute command {1} with return code {2}") % real_command % ret_code).str(APPLOCALE));
		return retval;
	}

	if (size > 0) {
		buf[size] = '\0';
		retval = atoi(buf);
	}

	return retval;
}

void schd_agent::cleanup()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	string ssh_prefix = usrname;
	ssh_prefix += "@";
	ssh_prefix += hostname;

	string real_command = profile_cmd;
	real_command += "rm -f ../script/.";
	real_command += boost::lexical_cast<string>(watched_pid);

	boost::shared_ptr<ssh_session> session(new ssh_session(ssh_prefix));
	channel_shared_ptr auto_channel(session);
	LIBSSH2_CHANNEL *channel = auto_channel.get();

	if (libssh2_channel_exec(channel, real_command.c_str()) != 0)
		GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't execute command {1} on {2}") % real_command % ssh_prefix).str(APPLOCALE));

	int ret_code = auto_channel.get_exit_status();
	if (ret_code)
		GPP->write_log(__FILE__, __LINE__, (_("ERROR: Failed to execute command {1} with return code {2}") % real_command % ret_code).str(APPLOCALE));
}

void schd_agent::init_once()
{
	_instance = new schd_agent();
}

}
}

using namespace ai::sg;
using namespace ai::app;

void on_signal(int signo)
{
	schd_agent& agent_mgr = schd_agent::instance();

	agent_mgr.kill(signo);
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
	signal(SIGCHLD, SIG_DFL);

	try {
		schd_agent& agent_mgr = schd_agent::instance();

		int retval = agent_mgr.init(argc, argv);
		if (retval) {
			GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't init context")).str(APPLOCALE));
			return retval;
		}

		 BOOST_SCOPE_EXIT((&agent_mgr)) {
		 	agent_mgr.fini();
		 } BOOST_SCOPE_EXIT_END

		return agent_mgr.run();
	} catch (exception& ex) {
		GPP->write_log(__FILE__, __LINE__, ex.what());
		return 1;
	}
}

