#include <dlfcn.h>
#include "sa_internal.h"
#include "sa_integrator.h"
#include "sdc_api.h"
#include "schd_ctx.h"
#include "schd_rpc.h"

namespace po = boost::program_options;
using namespace ai::sg;
using namespace ai::scci;
typedef boost::tokenizer<boost::char_separator<char> > tokenizer;

namespace ai
{
namespace app
{

sa_integrator::sa_integrator()
{
	// 创建所有SA
	if (!create_adaptors())
		::exit(1);

	// 校验参数
	if (!SAT->validate())
		::exit(1);

	// 进行编译
	try {
		SAP->_SAP_cmpl.build();
	} catch (exception& ex) {
		api_mgr.write_log(__FILE__, __LINE__, ex.what());
		::exit(1);
	}

	string user_name;
	string password;
	if (!parse(user_name, password))
		::exit(1);

	if (!user_name.empty()) {
		sdc_api& sdc_mgr = sdc_api::instance();
		if (!sdc_mgr.login()) {
			GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't login on SDC")).str(APPLOCALE));
			::exit(1);
		}

		if (!sdc_mgr.connect(user_name, password)) {
			GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't connect on SDC")).str(APPLOCALE));
			::exit(1);
		}

		SAT->_SAT_user_id = sdc_mgr.get_user();
		GPP->write_log((_("INFO: Connect to SDC as {1}") % user_name).str(APPLOCALE));
	}

	// 第二次初始化
	BOOST_FOREACH(sa_base *sa, SAT->_SAT_sas) {
		sa->init2();
	}

	// 设置主机名
	sgc_ctx *SGC = SGT->SGC();
	const char *pmid = SGC->mid2pmid(SGC->_SGC_proc.mid);
	if (pmid != NULL)
		SAT->_SAT_hostname = pmid;
}

sa_integrator::~sa_integrator()
{
}

void sa_integrator::run(int flags)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(800, __PRETTY_FUNCTION__, (_("flags={1}") % flags).str(APPLOCALE), NULL);
#endif

	// 需要预先创建数据库连接
	dblog_manager& dblog_mgr = dblog_manager::instance(SAT);
	schd_rpc& rpc_mgr = schd_rpc::instance();

	switch (flags) {
	case 1:
		dblog_mgr.dump(cout);
		BOOST_FOREACH(sa_base *sa, SAT->_SAT_sas) {
			sa->dump(cout);
		}
		// 报告正常退出
		rpc_mgr.report_exit();
		break;
	case 2:
		BOOST_FOREACH(sa_base *sa, SAT->_SAT_sas) {
			sa->rollback();
		}
		SAT->_SAT_db->commit();
		// 报告正常退出
		rpc_mgr.report_exit();
		break;
	case 3:
		BOOST_FOREACH(sa_base *sa, SAT->_SAT_sas) {
			sa->iclean();
		}
		// 报告正常退出
		rpc_mgr.report_exit();
		break;
	default:
		sa_base *starter = SAT->_SAT_sas[0];
		starter->run();
		break;
	}
}

// 创建所有SA
bool sa_integrator::create_adaptors()
{
	bool retval = false;
#if defined(DEBUG)
	scoped_debug<bool> debug(800, __PRETTY_FUNCTION__, "", &retval);
#endif

	for (int i = 0; i < SAP->_SAP_adaptors.size(); i++) {
		const sa_adaptor_t& adaptor = SAP->_SAP_adaptors[i];

		if (SAP->sa_base_factory(adaptor.parms.com_key) == NULL)
			return retval;
	}

	retval = true;
	return retval;
}

bool sa_integrator::parse(sg_init_t& init_info, const string& sginfo)
{
	bool retval = false;
#if defined(DEBUG)
	scoped_debug<bool> debug(800, __PRETTY_FUNCTION__, (_("sginfo={1}") % sginfo).str(APPLOCALE), &retval);
#endif
	sg_api& api_mgr = sg_api::instance();
	boost::escaped_list_separator<char> sep('\\', ';', '\"');
	boost::tokenizer<boost::escaped_list_separator<char> > tok(sginfo, sep);

	memset(&init_info, '\0', sizeof(init_info));

	for (boost::tokenizer<boost::escaped_list_separator<char> >::iterator iter = tok.begin(); iter != tok.end(); ++iter) {
		string str = *iter;

		string::size_type pos = str.find('=');
		if (pos == string::npos)
			continue;

		string key(str.begin(), str.begin() + pos);
		string value(str.begin() + pos + 1, str.end());

		if (key.empty())
			continue;

		if (key[0] == '@') {
			char text[4096];
			common::decrypt(value.c_str(), text);
			value = text;
			key.assign(key.begin() + 1, key.end());
		}

		if (strcasecmp(key.c_str(), "usrname") == 0) {
			if (value.length() >= sizeof(init_info.usrname)) {
				api_mgr.write_log(__FILE__, __LINE__, (_("ERROR: usrname on sginfo too long")).str(APPLOCALE));
				return retval;
			}
			memcpy(init_info.usrname, value.c_str(), value.length() + 1);
		} else if (strcasecmp(key.c_str(), "cltname") == 0) {
			if (value.length() >= sizeof(init_info.cltname)) {
				api_mgr.write_log(__FILE__, __LINE__, (_("ERROR: cltname on sginfo too long")).str(APPLOCALE));
				return retval;
			}
			memcpy(init_info.cltname, value.c_str(), value.length() + 1);
		} else if (strcasecmp(key.c_str(), "passwd") == 0) {
			if (value.length() >= sizeof(init_info.passwd)) {
				api_mgr.write_log(__FILE__, __LINE__, (_("ERROR: passwd on sginfo too long")).str(APPLOCALE));
				return retval;
			}
			memcpy(init_info.passwd, value.c_str(), value.length() + 1);
		} else if (strcasecmp(key.c_str(), "grpname") == 0) {
			if (value.length() >= sizeof(init_info.grpname)) {
				api_mgr.write_log(__FILE__, __LINE__, (_("ERROR: grpname on sginfo too long")).str(APPLOCALE));
				return retval;
			}
			memcpy(init_info.grpname, value.c_str(), value.length() + 1);
		}
	}

	retval = true;
	return retval;
}

bool sa_integrator::parse(string& usrname, string& passwd)
{
	bool retval = false;
#if defined(DEBUG)
	scoped_debug<bool> debug(800, __PRETTY_FUNCTION__, "", &retval);
#endif
	boost::escaped_list_separator<char> sep('\\', ';', '\"');
	boost::tokenizer<boost::escaped_list_separator<char> > tok(SAP->_SAP_resource.dbcinfo, sep);

	usrname = "";
	passwd = "";

	for (boost::tokenizer<boost::escaped_list_separator<char> >::iterator iter = tok.begin(); iter != tok.end(); ++iter) {
		string str = *iter;

		string::size_type pos = str.find('=');
		if (pos == string::npos)
			continue;

		string key(str.begin(), str.begin() + pos);
		string value(str.begin() + pos + 1, str.end());

		if (key.empty())
			continue;

		if (key[0] == '@') {
			char text[4096];
			common::decrypt(value.c_str(), text);
			value = text;
			key.assign(key.begin() + 1, key.end());
		}

		if (strcasecmp(key.c_str(), "usrname") == 0)
			usrname = value;
		else if (strcasecmp(key.c_str(), "passwd") == 0)
			passwd = value;
	}

	retval = true;
	return retval;
}

}
}

using namespace ai::app;

void on_signal(int signo)
{
	sgp_ctx *SGP = sgp_ctx::instance();
	sap_ctx *SAP = sap_ctx::instance();

	if (signo == SIGHUP && SAP->_SAP_undo_id > 0)
		exit(1);

	SGP->_SGP_shutdown++;
}

int main(int argc, char **argv)
{
	string svc_key;
	int version;
	string config_file;
	string sginfo;
	bool background = false;
	int flags = 0;
	gpenv& env_mgr = gpenv::instance();
	sgp_ctx *SGP = sgp_ctx::instance();
	sap_ctx *SAP = sap_ctx::instance();
	schd_ctx *SCHD = schd_ctx::instance();

	po::options_description desc("Allowed options");
	desc.add_options()
		("help", (_("produce help message")).str(APPLOCALE).c_str())
		("version,v", (_("print current run_integrator version")).str(APPLOCALE).c_str())
		("conf,c", po::value<string>(&config_file), (_("integrator configuration file")).str(APPLOCALE).c_str())
		("background,b", (_("run integrator in background mode")).str(APPLOCALE).c_str())
		("dump,d", (_("print preparing information")).str(APPLOCALE).c_str())
		("rollback,r", po::value<int>(&SAP->_SAP_undo_id), (_("rollback to initial state")).str(APPLOCALE).c_str())
		("iclean,i", (_("remove all files of input directory")).str(APPLOCALE).c_str())
		("nclean,n", (_("do not remove stale files in output directories")).str(APPLOCALE).c_str())
		("svckey,k", po::value<string>(&svc_key), (_("specify operation key")).str(APPLOCALE).c_str())
		("svcversion,y", po::value<int>(&version)->default_value(-1), (_("specify operation version")).str(APPLOCALE).c_str())
		("sginfo,s", po::value<string>(&sginfo), (_("specify login info")).str(APPLOCALE).c_str())
		("suffixURI,S", po::value<string>(&SCHD->_SCHD_suffixURI), (_("specify JMS producer/consumer topic URI suffix")).str(APPLOCALE).c_str())
	;

	po::variables_map vm;

	try {
		po::store(po::parse_command_line(argc, argv, desc), vm);

		if (vm.count("help")) {
			std::cout << desc << std::endl;
			::exit(0);
		} else if (vm.count("version")) {
			std::cout << (_("run_integrator version {1}") % SA_RELEASE).str(APPLOCALE)  << std::endl;
			std::cout << (_("Compiled on ")).str(APPLOCALE) << __DATE__ << " " << __TIME__ << std::endl;
			::exit(0);
		}

		po::notify(vm);
	} catch (exception& ex) {
		std::cout << ex.what() << std::endl;
		std::cout << desc << std::endl;
		::exit(1);
	}

	if (vm.count("background"))
		background = true;

	if (vm.count("dump"))
		flags = 1;
	else if (SAP->_SAP_undo_id > 0)
		flags = 2;
	else if (vm.count("iclean"))
		flags = 3;

	if (vm.count("nclean"))
		SAP->_SAP_nclean = true;

	if (config_file.empty() && svc_key.empty()) {
		std::cout << (_("ERROR: --conf or --svckey must be specify at least once")).str(APPLOCALE) <<std::endl;
		std::cout << desc << std::endl;
		::exit(1);
	}

	if (background) {
		sys_func& sys_mgr = sys_func::instance();
		sys_mgr.background();

		// 重新调整pid
		SAP->_SAP_pid = getpid();
	}

	// 设置可执行文件名称
	gpp_ctx *GPP = gpp_ctx::instance();
	GPP->set_procname(argv[0]);

	signal(SIGHUP, on_signal);
	signal(SIGINT, on_signal);
	signal(SIGQUIT, on_signal);
	signal(SIGTERM, on_signal);

	const char *appdir = env_mgr.getenv("APPROOT");
	if (appdir != NULL)
		chdir(appdir);

	try {
		if (!config_file.empty()) {
			// 加载配置文件
			sa_config config_mgr;
			config_mgr.load(config_file);
			sginfo = SAP->_SAP_resource.sginfo;
		}

		// 登录到SG
		sg_init_t init_info;
		string config;
		sg_api& api_mgr = sg_api::instance();

		if (!sa_integrator::parse(init_info, sginfo))
			::exit(1);

		if (!api_mgr.init(&init_info)) {
			api_mgr.write_log(__FILE__, __LINE__, (_("ERROR: Can't login on ServiceGalaxy.")).str(APPLOCALE));
			::exit(1);
		}

		BOOST_SCOPE_EXIT((&api_mgr)) {
			api_mgr.fini();
		} BOOST_SCOPE_EXIT_END

		// 需要在init()之后才能获取
		if (config_file.empty()) {
			string key = string("INT.") + svc_key;

			// 获取integrator配置文件
			if (!api_mgr.get_config(config, key, version)) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: Failure to load integrator configuration")).str(APPLOCALE));
				return 1;
			}

			sa_config config_mgr;
			config_mgr.load_xml(config);

			if (!sginfo.empty())
				SAP->_SAP_resource.sginfo = sginfo;
		}

		if (SCHD->_SCHD_suffixURI.empty()) {
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

		try {
			// 加载动态库
			boost::char_separator<char> sep(" \t\b");
			tokenizer tokens(SAP->_SAP_resource.libs, sep);
			for (tokenizer::iterator iter = tokens.begin(); iter != tokens.end(); ++iter) {
				string lib_name = *iter;

				void *handle = ::dlopen(lib_name.c_str(), RTLD_LAZY);
				if (handle == NULL) {
					GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't load library {1}, {2}") % lib_name % dlerror()).str(APPLOCALE));
					return 1;
				}

				SAP->_SAP_lib_handles.push_back(handle);
			}
		} catch (exception& ex) {
			GPP->write_log(__FILE__, __LINE__, ex.what());
			return 1;
		}

		gpenv& env_mgr = gpenv::instance();
		char *ptr = env_mgr.getenv("SA_AUTO_MKDIR");
		if (ptr != NULL && strcasecmp(ptr, "Y") == 0)
			SAP->_SAP_auto_mkdir = true;

		ai::app::sa_integrator instance;

		instance.run(flags);
	} catch (exception& ex) {
		GPP->write_log(__FILE__, __LINE__, ex.what());
		std::cout << ex.what() << std::endl;
		return 1;
	}

	if (SGP->_SGP_shutdown)
		return 1;
	else
		return 0;
}

