#include "schd_log2jms.h"
#include "schd_ctx.h"
#include "scan_file.h"

using namespace ai::sg;
namespace po = boost::program_options;

namespace ai
{
namespace app
{

int const LOG2JMS_RELEASE = 1;
int const DFLT_LOG2JMS_POLLTIME = 1;
int const LOG_LEVEL_INFO = 1;
int const LOG_LEVEL_WARN = 2;
int const LOG_LEVEL_ERROR = 3;
int const LOG_LEVEL_FATAL = 4;

bool schd_log2jms::shutdown = false;

schd_log2jms::schd_log2jms()
{
	SCHD = schd_ctx::instance();
}

schd_log2jms::~schd_log2jms()
{
}

int schd_log2jms::init(int argc, char **argv)
{
	int retval = -1;
#if defined(DEBUG)
	scoped_debug<int> debug(500, __PRETTY_FUNCTION__, (_("argc={1}") % argc).str(APPLOCALE), &retval);
#endif
	string directory;

	po::options_description desc((_("Allowed options")).str(APPLOCALE));
	desc.add_options()
		("help", (_("produce help message")).str(APPLOCALE).c_str())
		("version,v", (_("print current run_log2jms version")).str(APPLOCALE).c_str())
		("background,b", (_("run_long2jms in background mode")).str(APPLOCALE).c_str())
		("brokerURI,B", po::value<string>(&SCHD->_SCHD_brokerURI)->default_value(DFLT_BROKER_URI), (_("specify JMS broker URI")).str(APPLOCALE).c_str())
		("suffixURI,S", po::value<string>(&SCHD->_SCHD_suffixURI)->required(), (_("specify JMS producer/consumer topic URI suffix")).str(APPLOCALE).c_str())
		("directory,d", po::value<string>(&directory), (_("specify directories to watch")).str(APPLOCALE).c_str())
		("polltime,t", po::value<int>(&polltime)->default_value(DFLT_LOG2JMS_POLLTIME), (_("specify seconds to check log")).str(APPLOCALE).c_str())
		("encoding,e", po::value<string>(&settings.encoding)->default_value("gbk"), (_("specify encoding of log content")).str(APPLOCALE).c_str())
	;

	po::variables_map vm;

	try {
		po::store(po::parse_command_line(argc, argv, desc), vm);

		if (vm.count("help")) {
			std::cout << desc << std::endl;
			GPP->write_log((_("INFO: {1} exit normally in help mode.") % GPP->_GPP_procname).str(APPLOCALE));
			::exit(0);
		} else if (vm.count("version")) {
			std::cout << (_("run_log2jms version ")).str(APPLOCALE) << LOG2JMS_RELEASE << std::endl;
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

	if (polltime <= 0)
		polltime = DFLT_LOG2JMS_POLLTIME;

	log2jms_directory_t item;
	{
		sg_api& api_mgr = sg_api::instance();
		if (!api_mgr.init()) {
			GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't join application")).str(APPLOCALE));
			return retval;
		}

		BOOST_SCOPE_EXIT((&api_mgr)) {
			api_mgr.fini();
		} BOOST_SCOPE_EXIT_END

		sgc_ctx *SGC = SGT->SGC();
		int mid = SGC->_SGC_proc.mid;
		const char *pmid = SGC->mid2pmid(mid);
		if (pmid == NULL) {
			GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't find hostname for node id {1}") % mid).str(APPLOCALE));
			return retval;
		}

		item.hostname = pmid;
	}

	string::size_type pos = GPP->_GPP_ulogpfx.rfind("/");
	if (pos == string::npos) {
		char curr_dir[_POSIX_PATH_MAX];
		::getcwd(curr_dir, sizeof(curr_dir));
		item.directory = curr_dir;
		item.directory += '/';
		item.pattern = "^";
		item.pattern += GPP->_GPP_ulogpfx;
		item.pattern += "_[0-9]{4}_[0-9]{2}_[0-9]{2}\\.log$";
	} else {
		item.directory = string(GPP->_GPP_ulogpfx.begin(), GPP->_GPP_ulogpfx.begin() + pos + 1);
		item.pattern = "^";
		item.pattern += string(GPP->_GPP_ulogpfx.begin() + pos + 1, GPP->_GPP_ulogpfx.end());
		item.pattern += "_[0-9]{4}-[0-9]{2}-[0-9]{2}\\.log$";
	}
	item.style = LOG2JMS_STYLE_NEW;
	directories.push_back(item);

	if (!directory.empty()) {
		common::parse(directory, item.sftp_prefix);
		string::size_type pos = item.sftp_prefix.find('@');
		if (pos == string::npos)
			item.hostname = item.sftp_prefix;
		else
			item.hostname = item.sftp_prefix.substr(pos + 1);
		item.pattern = "^[0-9]{8,10}_[0-9]{4}-[0-9]{2}-[0-9]{2}\\.log$";
		item.style = LOG2JMS_STYLE_OLD;

		item.directory = directory + "/shm/";
		directories.push_back(item);

		item.directory = directory + "/exp/";
		directories.push_back(item);

		item.directory = directory + "/fts/";
		directories.push_back(item);

		item.directory = directory + "/bps/";
		directories.push_back(item);

		item.directory = directory + "/stat/";
		directories.push_back(item);

		item.directory = directory + "/unical/";
		directories.push_back(item);

		item.pattern = "^LOG_[0-9]{4}-[0-9]{2}-[0-9]{2}\\.log$";
		item.style = LOG2JMS_STYLE_RAW;

		item.directory = directory + "/script/";
		directories.push_back(item);
	}

	info_str = (_("INFO:")).str(APPLOCALE);
	warn_str = (_("WARN:")).str(APPLOCALE);
	error_str = (_("ERROR:")).str(APPLOCALE);
	fatal_str = (_("FATAL:")).str(APPLOCALE);

	ppid = getppid();
	if (vm.count("background")) {
		sys_func& sys_mgr = sys_func::instance();
		sys_mgr.background();
	}

	SCHD->_SCHD_producerURI = PREFIX_LOGGER_URI;
	SCHD->_SCHD_producerURI += SCHD->_SCHD_suffixURI;
	SCHD->set_service();

	// 与JMS建立连接
	SCHD->connect();

	retval = 0;
	return retval;
}

int schd_log2jms::run()
{
	int retval = -1;
#if defined(DEBUG)
	scoped_debug<int> debug(500, __PRETTY_FUNCTION__, "", &retval);
#endif
	int log_pass = 0;

	while (!shutdown) {
		if (SCHD->_SCHD_connected) {
			BOOST_FOREACH(log2jms_directory_t& item, directories) {
				try {
					read(item);
				} catch (bad_ssh& ex) {
					if (log_pass == 0)
						GPP->write_log(__FILE__, __LINE__, ex.what());

					// 清理SSH会话缓存
					sshp_ctx *SSHP = sshp_ctx::instance();
					SSHP->clear();
					break;
				}
			}
		}

		if (kill(ppid, 0) == -1)
			break;

		log_pass = (log_pass + 1) % 60;
		sleep(polltime);

		// 尝试重新连接
		if (!SCHD->_SCHD_connected && time(0) >= SCHD->_SCHD_retry_time)
			SCHD->connect();
	}

	if (!shutdown)
		retval = 0;

	return retval;
}

void schd_log2jms::read(log2jms_directory_t& item)
{
#if defined(DEBUG)
	scoped_debug<int> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	map<string, size_t>& size_map = item.size_map;
	vector<string> files;
	scan_file<> scan(item.directory, item.pattern);
	scan.set_sftp(item.sftp_prefix);
	scan.get_files(files);

	BOOST_FOREACH(const string& file, files) {
		string full_name = item.directory;
		full_name += file;

		map<string, size_t>::iterator iter = size_map.find(full_name);
		if (iter == size_map.end())
			iter = size_map.insert(std::make_pair(full_name, 0)).first;
		size_t& file_size = iter->second;

		stream_type_enum stream_type;
		if (item.sftp_prefix.empty())
			stream_type = STREAM_TYPE_IFSTREAM;
		else
			stream_type = STREAM_TYPE_ISFTPSTREAM;

		boost::shared_ptr<istream> auto_is(common::create_istream(stream_type, item.sftp_prefix, full_name.c_str()));
		istream& is = *auto_is;
		if (!is)
			continue;

		if (!is.seekg(file_size))
			continue;

		string text;
		while (is.good()) {
			std::getline(is, text);
			if (text.empty())
				continue;

			string id;
			time_t timestamp;
			int level;
			bool retval;

			try {
				switch (item.style) {
				case LOG2JMS_STYLE_OLD:
					retval = parse_old(text, id, timestamp, level);
					break;
				case LOG2JMS_STYLE_RAW:
					retval = false;
					break;
				default:
					retval = parse_new(text, id, timestamp, level);
				}
			} catch (exception& ex) {
				retval = false;
			}

			if (!retval) {
				id = "";
				timestamp = 0;
				level = LOG_LEVEL_INFO;
			}

			try {
				bpt::iptree opt;
				bpt::iptree v_pt;
				ostringstream os;

				v_pt.put("hostname", item.hostname);
				v_pt.put("id", id);
				v_pt.put("timestamp", timestamp);
				v_pt.put("level", level);
				v_pt.put("text", text);

				opt.add_child("info", v_pt);
				bpt::write_xml(os, opt, settings);

				string data = os.str();
				auto_ptr<cms::BytesMessage> auto_msg(SCHD->_SCHD_session->createBytesMessage(reinterpret_cast<const unsigned char *>(data.c_str()), data.length()));
				cms::BytesMessage *msg = auto_msg.get();

				SCHD->_SCHD_producer->send(msg);
			} catch (cms::CMSException& ex) {
				// 忽略错误
			}
		}

		is.clear();
		file_size = is.tellg();
	}
}

bool schd_log2jms::parse_old(string& text, string& id, time_t& timestamp, int& level)
{
	bool retval = false;
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", &retval);
#endif

	string::size_type pos = text.find(": message[");
	if (pos == string::npos)
		return retval;

	if (pos <= 27)
		return retval;

	if (text[0] != '[' || text[20] != ']')
		return retval;

	int year = boost::lexical_cast<int>(string(text.begin() + 1, text.begin() + 5));
	int month = boost::lexical_cast<int>(string(text.begin() + 6, text.begin() + 8));
	int day = boost::lexical_cast<int>(string(text.begin() + 9, text.begin() + 11));
	int hour = boost::lexical_cast<int>(string(text.begin() + 12, text.begin() + 14));
	int minute = boost::lexical_cast<int>(string(text.begin() + 15, text.begin() + 17));
	int second = boost::lexical_cast<int>(string(text.begin() + 18, text.begin() + 20));
	datetime dt(year, month, day, hour, minute, second);
	timestamp = dt.duration();

	if (text[22] != '[')
		return retval;

	id = "";
	for (string::size_type idx = 23; idx < pos && text[idx] != ']'; idx++)
		id += text[idx];

	text = text.substr(pos + 10, text.length() - pos - 11);
	if (memcmp(text.c_str(), "bad_", 4) == 0)
		level = LOG_LEVEL_ERROR;
	else
		level = LOG_LEVEL_INFO;

	retval = true;
	return retval;
}

bool schd_log2jms::parse_new(string& text, string& id, time_t& timestamp, int& level)
{
	bool retval = false;
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", &retval);
#endif

	string::size_type pos = text.find(":");
	if (pos == string::npos)
		return retval;

	if (pos <= 30)
		return retval;

	if (text[0] != '[' || text[20] != ']')
		return retval;

	int year = boost::lexical_cast<int>(string(text.begin() + 1, text.begin() + 5));
	int month = boost::lexical_cast<int>(string(text.begin() + 6, text.begin() + 8));
	int day = boost::lexical_cast<int>(string(text.begin() + 9, text.begin() + 11));
	int hour = boost::lexical_cast<int>(string(text.begin() + 12, text.begin() + 14));
	int minute = boost::lexical_cast<int>(string(text.begin() + 15, text.begin() + 17));
	int second = boost::lexical_cast<int>(string(text.begin() + 18, text.begin() + 20));
	datetime dt(year, month, day, hour, minute, second);
	timestamp = dt.duration();

	if (text[22] != '[')
		return retval;

	id = "";
	for (string::size_type idx = 23; idx < pos && text[idx] != ']'; idx++)
		id += text[idx];

	text = text.substr(pos + 2);
	level = LOG_LEVEL_INFO;

	const char *ptr = text.c_str();
	const char *ptr_end = ptr + text.length();
	for (; ptr < ptr_end; ptr++) {
		if (memcmp(ptr, info_str.c_str(), info_str.length()) == 0) {
			level = LOG_LEVEL_INFO;
			break;
		} else if (memcmp(ptr, warn_str.c_str(), warn_str.length()) == 0) {
			level = LOG_LEVEL_WARN;
			break;
		} else if (memcmp(ptr, error_str.c_str(), error_str.length()) == 0) {
			level = LOG_LEVEL_ERROR;
			break;
		} else if (memcmp(ptr, fatal_str.c_str(), fatal_str.length()) == 0) {
			level = LOG_LEVEL_FATAL;
			break;
		}
	}

	retval = true;
	return retval;
}

}
}

using namespace ai::app;

void on_signal(int signo)
{
	schd_log2jms::shutdown = true;
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
	schd_log2jms instance;

	try {
		retval = instance.init(argc, argv);
		if (retval)
			return retval;

		retval = instance.run();
		if (retval)
			return retval;

		return 0;
	} catch (exception& ex) {
		GPP->write_log(__FILE__, __LINE__, ex.what());
		return 1;
	}
}

