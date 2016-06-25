#if !defined(__SCHD_LOG2JMS_H__)
#define __SCHD_LOG2JMS_H__

#include "app_locale.h"
#include "sg_internal.h"
#include "schd_ctx.h"

using namespace ai::sg;

namespace ai
{
namespace app
{

int const LOG2JMS_STYLE_OLD = 0;
int const LOG2JMS_STYLE_RAW = 1;
int const LOG2JMS_STYLE_NEW = 2;

struct log2jms_directory_t
{
	string sftp_prefix;
	string hostname;
	string directory;
	string pattern;
	int style;
	map<string, size_t> size_map;
};

class schd_log2jms : public sg_manager
{
public:
	schd_log2jms();
	~schd_log2jms();

	int init(int argc, char **argv);
	int run();

	static bool shutdown;

private:
	void read(log2jms_directory_t& item);
	bool parse_old(string& text, string& id, time_t& timestamp, int& level);
	bool parse_new(string& text, string& id, time_t& timestamp, int& level);

	schd_ctx *SCHD;
	bpt::xml_writer_settings<char> settings;
	int polltime;
	vector<log2jms_directory_t> directories;
	string info_str;
	string warn_str;
	string error_str;
	string fatal_str;
	pid_t ppid;
};

}
}

#endif

