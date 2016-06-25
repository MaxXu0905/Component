#if !defined(__SCHD_AGENT_H__)
#define __SCHD_AGENT_H__

#include "schd_ctx.h"
#include "sa_internal.h"

namespace ai
{
namespace app
{

using namespace ai::sg;

class schd_agent : public sa_manager
{
public:
	static schd_agent& instance();

	int init(int argc, char **argv);
	void fini();
	int run();
	void kill(int signo);

private:
	schd_agent();
	~schd_agent();

	int get_exit();
	void cleanup();

	static void init_once();

	sshp_ctx *SSHP;
	schd_ctx *SCHD;
	std::string usrname;
	std::string hostname;
	std::string command;
	int polltime;
	pid_t watched_pid;
	time_t gotsig_time;

	static boost::once_flag once_flag;
	static schd_agent *_instance;
};

}
}

#endif
