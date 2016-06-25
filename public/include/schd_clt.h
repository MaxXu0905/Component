#if !defined(__SCHD_CLT_H__)
#define __SCHD_CLT_H__

#include "sat_ctx.h"
#include "schd_ctx.h"
#include "schd_config.h"

using namespace ai::sg;

namespace ai
{
namespace app
{

class schd_clt : public sg_manager
{
public:
	schd_clt();
	~schd_clt();

	int init(int argc, char **argv);
	int run();
	void report(int status, const string& text = "");

private:
	bool init_agent();
	void check_ready();
	bool run_ready();
	bool run_increase();
	bool run_undo();
	bool check_status();
	void drain();
	void receive(int seconds);
	void check_pending();
	void report_status();
	void do_redo(const bpt::iptree& ipt);
	void change_policy(const bpt::iptree& ipt);
	void do_stop(const bpt::iptree& ipt);
	bool check_dep(const set<string>& entry_names);
	void stop_dep(const set<string>& entry_names);
	void do_undo(set<string>& done_entries, const set<string>& entry_names, int undo_id);
	void stop_entry(const string& entry_name, schd_entry_t& entry);

	sat_ctx *SAT;
	schd_ctx *SCHD;
	message_pointer rqst_msg;
	message_pointer rply_msg;
};

}
}

#endif

