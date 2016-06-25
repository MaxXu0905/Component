#include "bps_svr.h"
#include "dbc_api.h"
#include "sdc_api.h"

namespace bf = boost::filesystem;
namespace po = boost::program_options;
namespace bp = boost::posix_time;
namespace bc = boost::chrono;
using namespace ai::sg;
using namespace ai::scci;

namespace ai
{
namespace app
{

bps_svr::bps_svr()
{
	BPS = bps_ctx::instance();
}

bps_svr::~bps_svr()
{
}

int bps_svr::svrinit(int argc, char **argv)
{
	int retval = -1;
#if defined(DEBUG)
	scoped_debug<int> debug(1000, __PRETTY_FUNCTION__, (_("argc={1}") % argc).str(APPLOCALE), &retval);
#endif
	string dbc_key;
	int dbc_version;

	po::options_description desc("Allowed options");
	desc.add_options()
		("help", (_("produce help message")).str(APPLOCALE).c_str())
		("svc_key,k", po::value<vector<string> >(&BPS->_BPS_svc_keys), (_("specify operation keys")).str(APPLOCALE).c_str() )
		("version,v", po::value<int>(&BPS->_BPS_version)->default_value(-1), (_("specify configuration version")).str(APPLOCALE).c_str() )
		("svc_file,f", po::value<string>(&BPS->_BPS_svc_file), (_( "specify service keys file")).str(APPLOCALE).c_str())
		("dbckey,x", po::value<string>(&dbc_key),(_( "specify DBC's configuration key")).str(APPLOCALE).c_str() )
		("dbcversion,y", po::value<int>(&dbc_version)->default_value(-1), (_("specify DBC's configuration version")).str(APPLOCALE).c_str() )
		("dbc,d",  (_("run in DBC mode")).str(APPLOCALE).c_str())
	;

	po::variables_map vm;

	try {
		po::store(po::parse_command_line(argc, argv, desc), vm);

		if (vm.count("help")) {
			std::cout << desc << std::endl;
			GPP->write_log((_("INFO: {1} exit normally in help mode.") % GPP->_GPP_procname).str(APPLOCALE));
			retval = 0;
			return retval;
		}

		po::notify(vm);
	} catch (po::required_option& ex) {
		std::cout << ex.what() << std::endl;
		std::cout << desc << std::endl;
		SGT->_SGT_error_code = SGEINVAL;
		GPP->write_log(__FILE__, __LINE__, (_("ERROR: {1} start failure, {2}.") % GPP->_GPP_procname % ex.what()).str(APPLOCALE));
		return retval;
	}

	// 首先组装配置参数
	string clopt;
	if (!BPS->_BPS_svc_file.empty()) {
		clopt += "-f ";
		clopt += BPS->_BPS_svc_file;
		clopt += " ";
	}

	clopt += "-k";
	BOOST_FOREACH(const string& svc_key, BPS->_BPS_svc_keys) {
		clopt += " ";
		clopt += svc_key;
	}

	dbcp_ctx *DBCP = dbcp_ctx::instance();
	dbct_ctx *DBCT = dbct_ctx::instance();

	if (BPS->_BPS_svc_keys.empty() && BPS->_BPS_svc_file.empty()) {
		GPP->write_log(__FILE__, __LINE__, (_("ERROR: -k or -f must specify at lease once")).str(APPLOCALE));
		return retval;
	}

	if (!dbc_key.empty()) {
		if (!DBCP->get_config(dbc_key, dbc_version, "", -1))
			return retval;

		dbc_api& api_mgr = dbc_api::instance(DBCT);
		if (!api_mgr.login()) {
			GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't login on DBC - {1}") % DBCT->strerror()).str(APPLOCALE));
			return retval;
		}
	}

	if (vm.count("dbc")) {
		dbc_api& api_mgr = dbc_api::instance();

		if (!api_mgr.login()) {
			GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't login on DBC - {1}") % DBCT->strerror()).str(APPLOCALE));
			return retval;
		}

		BPS->_BPS_cmpl = new compiler(SEARCH_TYPE_DBC);
		BPS->_BPS_use_dbc = true;
		BPS->_BPS_data_mgr = reinterpret_cast<void *>(&api_mgr);
	} else {
		sdc_api& api_mgr = sdc_api::instance();

		if (!api_mgr.login()) {
			GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't login on SDC - {1}") % DBCT->strerror()).str(APPLOCALE));
			return retval;
		}

		if (!api_mgr.get_meta()) {
			GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't connect to SDC - {1}") % DBCT->strerror()).str(APPLOCALE));
			return retval;
		}

		BPS->_BPS_cmpl = new compiler(SEARCH_TYPE_SDC);
		BPS->_BPS_use_dbc = false;
		BPS->_BPS_data_mgr = reinterpret_cast<void *>(&api_mgr);
	}

	if (!BPS->get_config())
		return retval;

	// 创建函数
	compiler& cmpl = *BPS->_BPS_cmpl;
	svcp_ctx *SVCP = svcp_ctx::instance();
	const boost::unordered_map<string, svc_adaptor_t>& adaptors = SVCP->_SVCP_adaptors;
	int functions = 0;

	// 必须保证顺序，以保证在有缓存时函数名与其内容一致
	boost::unordered_map<string, bps_adaptor_t>::iterator bps_iter;
	boost::unordered_map<string, bps_adaptor_t>& bps_adaptors = BPS->_BPS_adaptors;
	set<string> svc_keys;
	for (bps_iter = bps_adaptors.begin(); bps_iter != bps_adaptors.end(); ++bps_iter)
		svc_keys.insert(bps_iter->first);

	BOOST_FOREACH(const string& svc_key, svc_keys) {
		bps_iter = bps_adaptors.find(svc_key);
		if (bps_iter == bps_adaptors.end()) {
			GPP->write_log(__FILE__, __LINE__,(_("ERROR: svc_key {1} not found.") % svc_key).str(APPLOCALE) );
			return retval;
		}
		bps_adaptor_t& bps_adaptor = bps_iter->second;
		vector<bps_process_t>& processes = bps_adaptor.processes;
		vector<int>& indice = bps_adaptor.indice;

		boost::unordered_map<string, svc_adaptor_t>::const_iterator svc_iter = adaptors.find(svc_key);
		if (svc_iter == adaptors.end()) {
			GPP->write_log(__FILE__, __LINE__,(_("ERROR: svc_key {1} not found.") % svc_key).str(APPLOCALE) );
			return retval;
		}
		const svc_adaptor_t& adaptor = svc_iter->second;

		for (int i = 0; i < processes.size(); i++) {
			const bps_process_t& process = processes[i];
			const vector<string>& rules = process.rules;

			string code;
			BOOST_FOREACH(const string& rule, rules) {
				code += rule;
			}

			int fun_idx = cmpl.create_function(code, adaptor.global_map, adaptor.input_maps[i], adaptor.output_map);
			indice.push_back(fun_idx);
		}

		functions += indice.size();
	}

	// 检查是否已经有相同参数的BPS启动

	// 打开缓存文件
	sgc_ctx *SGC = SGT->SGC();
	sg_bboard_t *bbp = SGC->_SGC_bbp;
	sg_bbparms_t& bbparms = bbp->bbparms;
	gpenv& env_mgr = gpenv::instance();
	string appdir = env_mgr.getenv("APPROOT");

	string cache_dir = appdir + "/.Component";
	if (::mkdir(cache_dir.c_str(), 0700) == -1 && errno != EEXIST) {
		DBCT->_DBCT_error_code = DBCEOS;
		GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't create directory {1}") % cache_dir).str(APPLOCALE));
		return retval;
	}

	cache_dir += "/";
	cache_dir += boost::lexical_cast<string>(bbparms.ipckey & ((1 << MCHIDSHIFT) - 1));
	if (::mkdir(cache_dir.c_str(), 0700) == -1 && errno != EEXIST) {
		DBCT->_DBCT_error_code = DBCEOS;
		GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't create directory {1}") % cache_dir).str(APPLOCALE));
		return retval;
	}

	string filename = cache_dir + "/BPS";
	int fd = open(filename.c_str(), O_CREAT | O_RDWR, 0600);
	if (fd == -1) {
		DBCT->_DBCT_error_code = DBCEOS;
		GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't open file {1}") % filename).str(APPLOCALE));
		return retval;
	}

	BOOST_SCOPE_EXIT((&fd)) {
		close(fd);
	} BOOST_SCOPE_EXIT_END

	bool require_build = true;
	off_t offset = 0;
	string libname = cmpl.get_libname();
	compiler_cache_t item;
	while (1) {
		if (read(fd, &item, sizeof(item)) != sizeof(item))
			break;

		if (functions == item.functions && clopt == item.clopt) {
			if (kill(item.pid, 0) == -1)
				break;

			if (access(item.libname, R_OK | X_OK) == -1)
				break;

			string cmd = "cp ";
			cmd += item.libname;
			cmd += " ";
			cmd += libname;

			sys_func& func_mgr = sys_func::instance();
			if (func_mgr.new_task(cmd.c_str(), NULL, 0) != 0)
				break;

			struct stat statbuf;
			if (stat(item.libname, &statbuf) == -1)
				break;

			if (statbuf.st_size != item.size || statbuf.st_mtime != item.mtime)
				break;

			require_build = false;
			break;
		}

		offset += sizeof(item);
	}

	// 编译函数
	cmpl.build(require_build);

	// 写入cache
	while (require_build) {
		item.pid = getpid();
		item.functions = functions;

		if (clopt.length() >= sizeof(item.clopt)) {
			DBCT->_DBCT_error_code = DBCEOS;
			GPP->write_log(__FILE__, __LINE__, (_("ERROR: args {1} too long") % clopt).str(APPLOCALE));
			return retval;
		}
		strcpy(item.clopt, clopt.c_str());

		if (libname.length() >= sizeof(item.libname)) {
			DBCT->_DBCT_error_code = DBCEOS;
			GPP->write_log(__FILE__, __LINE__, (_("ERROR: libname {1} too long") % libname).str(APPLOCALE));
			return retval;
		}
		strcpy(item.libname, libname.c_str());

		struct stat statbuf;
		if (stat(libname.c_str(), &statbuf) == -1) {
			DBCT->_DBCT_error_code = DBCEOS;
			GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't stat file {1}") % libname).str(APPLOCALE));
			return retval;
		}

		item.size = statbuf.st_size;
		item.mtime = statbuf.st_mtime;

		if (lseek(fd, offset, SEEK_SET) == -1) {
			GPP->write_log(__FILE__, __LINE__, (_("WARN: Can't seek file {1} - {2}") % filename % strerror(errno)).str(APPLOCALE));
			break;
		}

		if (write(fd, &item, sizeof(compiler_cache_t)) != sizeof(compiler_cache_t)) {
			GPP->write_log(__FILE__, __LINE__, (_("WARN: Can't seek file {1} - {2}") % filename % strerror(errno)).str(APPLOCALE));
			break;
		}

		break;
	}

	// 获取函数地址
	for (boost::unordered_map<string, bps_adaptor_t>::iterator iter = bps_adaptors.begin(); iter != bps_adaptors.end(); ++iter) {
		bps_adaptor_t& bps_adaptor = iter->second;
		vector<int>& indice = bps_adaptor.indice;
		vector<compiler::func_type>& funs = bps_adaptor.funs;

		for (int i = 0; i < indice.size(); i++) {
			compiler::func_type fun = cmpl.get_function(indice[i]);
			funs.push_back(fun);
		}
	}

	// 发布服务
	if (!advertise())
		return retval;

	GPP->write_log((_("INFO: {1} -g {2} -p {3} started successfully.") % GPP->_GPP_procname % SGC->_SGC_proc.grpid % SGC->_SGC_proc.svrid).str(APPLOCALE));
	retval = 0;
	return retval;
}

int bps_svr::svrfini()
{
	int retval = -1;
#if defined(DEBUG)
	scoped_debug<int> debug(1000, __PRETTY_FUNCTION__, "", &retval);
#endif

	if (!BPS->_BPS_use_dbc) {
		sdc_api& api_mgr = *reinterpret_cast<sdc_api *>(BPS->_BPS_data_mgr);

		api_mgr.disconnect();
		api_mgr.logout();
	}

	{
		dbc_api& api_mgr = dbc_api::instance();

		api_mgr.disconnect();
		api_mgr.logout();
	}

	delete BPS->_BPS_cmpl;
	BPS->_BPS_cmpl = NULL;
	BPS->_BPS_data_mgr = NULL;

	sgc_ctx *SGC = SGT->SGC();
	GPP->write_log((_("INFO: {1} -g {2} -p {3} stopped successfully.") % GPP->_GPP_procname % SGC->_SGC_proc.grpid % SGC->_SGC_proc.svrid).str(APPLOCALE));
	retval = 0;
	return retval;
}

bool bps_svr::advertise()
{
	bool retval = false;
#if defined(DEBUG)
	scoped_debug<bool> debug(1000, __PRETTY_FUNCTION__, "", &retval);
#endif

	sg_svcdsp_t *svcdsp = SGP->_SGP_svcdsp;
	if (svcdsp == NULL || svcdsp->index == -1) {
		GPP->write_log(__FILE__, __LINE__, (_("ERROR: No operation provided on sgcompile time" )).str(APPLOCALE));
		return retval;
	}

	string class_name = svcdsp->class_name;
	sg_api& api_mgr = sg_api::instance(SGT);
	svcp_ctx *SVCP = svcp_ctx::instance();
	boost::unordered_map<std::string, svc_adaptor_t>& adaptors = SVCP->_SVCP_adaptors;

	for (boost::unordered_map<string, svc_adaptor_t>::const_iterator iter = adaptors.begin(); iter != adaptors.end(); ++iter) {
		string svc_name = "BPS_";
		svc_name += iter->first;

		if (!api_mgr.advertise(svc_name, class_name)) {
			GPP->write_log(__FILE__, __LINE__, (_("ERROR: Failed to advertise operation {1}") % svc_name).str(APPLOCALE));
			return retval;
		}
	}

	retval = true;
	return retval;
}

}
}

