#include "dup_svr.h"

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

typedef boost::tokenizer<boost::char_separator<char> > tokenizer;

dup_svr::dup_svr()
{
	DUP = dup_ctx::instance();
}

dup_svr::~dup_svr()
{
}

int dup_svr::svrinit(int argc, char **argv)
{
	int retval = -1;
#if defined(DEBUG)
	scoped_debug<int> debug(1000, __PRETTY_FUNCTION__, (_("argc={1}") % argc).str(APPLOCALE), &retval);
#endif

	po::options_description desc("Allowed options");
	desc.add_options()
		("help", (_("produce help message")).str(APPLOCALE).c_str())
		("svc_key,k", po::value<string>(&DUP->_DUP_svc_key)->required(), (_("specify operation key")).str(APPLOCALE).c_str())
		("version,v", po::value<int>(&DUP->_DUP_version)->default_value(-1), (_("specify configuration version")).str(APPLOCALE).c_str())
		("cache_size,c", po::value<long>(&DUP->_DUP_cache_size)->default_value(0), (_("specify pages/-kibibytes of cache_size")).str(APPLOCALE).c_str())
		("exclusive,e", (_("set locking_mode=exclusive")).str(APPLOCALE).c_str())
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

		if (vm.count("exclusive"))
			DUP->_DUP_exclusive = true;
		else
			DUP->_DUP_exclusive = false;

		po::notify(vm);
	} catch (po::required_option& ex) {
		std::cout << ex.what() << std::endl;
		std::cout << desc << std::endl;
		SGT->_SGT_error_code = SGEINVAL;
		GPP->write_log(__FILE__, __LINE__, (_("ERROR: {1} start failure, {2}.") % GPP->_GPP_procname % ex.what()).str(APPLOCALE));
		return retval;
	}

	if (!DUP->get_config())
		return retval;

	// 调整partition_id
	int partition_offset = SGP->_SGP_svrid - SGP->_SGP_svridmin;
	if (partition_offset < 0 || partition_offset >= DUP->_DUP_partitions) {
		SGT->_SGT_error_code = SGEINVAL;
		GPP->write_log(__FILE__, __LINE__, (_("ERROR: partition out of range.")).str(APPLOCALE));
		return retval;
	}

	DUP->_DUP_partition_id += partition_offset;

	// 初始化服务参数
	load_parms();

	// 调整db_name
	DUP->_DUP_db_name += ".";
	DUP->_DUP_db_name += boost::lexical_cast<string>(DUP->_DUP_partition_id);

	gpenv& env_mgr = gpenv::instance();
	char *ptr = env_mgr.getenv("SA_AUTO_MKDIR");
	if (ptr != NULL && strcasecmp(ptr, "Y") == 0) {
		int size = DUP->_DUP_db_name.length();
		for (string::reverse_iterator riter = DUP->_DUP_db_name.rbegin(); riter != DUP->_DUP_db_name.rend(); ++riter) {
			if (*riter == '/')
				break;

			size--;
		}

		string str = DUP->_DUP_db_name.substr(0, size);
		string part;
		boost::char_separator<char> sep("/");
		tokenizer tokens(str, sep);

		for (tokenizer::iterator iter = tokens.begin(); iter != tokens.end(); ++iter) {
			part += "/";
			part += *iter;

			if (::mkdir(part.c_str(), 0755) == -1) {
				if (errno != EEXIST) {
					GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't create directory {1}") % part).str(APPLOCALE));
					return retval;
				}
			}
		}
	}

	if (!DUP->open_sqlite())
		return retval;

	// 发布DUP特有的服务
	if (!advertise())
		return retval;

	sgc_ctx *SGC = SGT->SGC();
	GPP->write_log((_("INFO: {1} -g {2} -p {3} started successfully.") % GPP->_GPP_procname % SGC->_SGC_proc.grpid % SGC->_SGC_proc.svrid).str(APPLOCALE));
	retval = 0;
	return retval;
}

int dup_svr::svrfini()
{
	int retval = -1;
#if defined(DEBUG)
	scoped_debug<int> debug(1000, __PRETTY_FUNCTION__, "", &retval);
#endif

	sgc_ctx *SGC = SGT->SGC();
	GPP->write_log((_("INFO: {1} -g {2} -p {3} stopped successfully.") % GPP->_GPP_procname % SGC->_SGC_proc.grpid % SGC->_SGC_proc.svrid).str(APPLOCALE));
	retval = 0;
	return retval;
}

bool dup_svr::advertise()
{
	bool retval = false;
#if defined(DEBUG)
	scoped_debug<bool> debug(1000, __PRETTY_FUNCTION__, "", &retval);
#endif

	sg_svcdsp_t *svcdsp = SGP->_SGP_svcdsp;
	if (svcdsp == NULL || svcdsp->index == -1) {
		GPP->write_log(__FILE__, __LINE__,(_("ERROR: No operation provided on sgcompile time")).str(APPLOCALE) );
		return retval;
	}

	string svc_name;
	DUP->set_svcname(DUP->_DUP_partition_id, svc_name);

	string class_name = svcdsp->class_name;
	sg_api& api_mgr = sg_api::instance(SGT);

	if (!api_mgr.advertise(svc_name, class_name)) {
		GPP->write_log(__FILE__, __LINE__, (_("ERROR: Failed to advertise operation {1}") % svc_name).str(APPLOCALE));
		return retval;
	}

	retval = true;
	return retval;
}

void dup_svr::load_parms()
{
	svcp_ctx *SVCP = svcp_ctx::instance();
	boost::unordered_map<std::string, svc_adaptor_t>& adaptors = SVCP->_SVCP_adaptors;

	boost::unordered_map<std::string, svc_adaptor_t>::iterator iter
		= adaptors.insert(std::make_pair(DUP->_DUP_svc_key, svc_adaptor_t())).first;
	svc_adaptor_t& adaptor = iter->second;

	// 通用参数
	svc_parms_t& parms = adaptor.parms;
	parms.svc_key = DUP->_DUP_svc_key;
	parms.version = DUP->_DUP_version;
	parms.disable_global = true;

	// 全局变量定义
	// 不需要全局变量

	// 输入变量定义
	adaptor.input_maps.resize(1);
	field_map_t& input_map = adaptor.input_maps[0];
	vector<int>& input_sizes = adaptor.input_sizes;

	DUP->set_map(input_map);
	input_sizes.push_back(DUP_TABLE_SIZE);
	input_sizes.push_back(DUP_ID_SIZE);
	input_sizes.push_back(DUP_KEY_SIZE);

	if (DUP->_DUP_enable_cross) {
		input_sizes.push_back(DUP_TIME_SIZE);
		input_sizes.push_back(DUP_TIME_SIZE);
	}

	// 输出变量定义
	// 不需要输出变量
}

}
}

