#if !defined(__SCHD_RPC_H__)
#define __SCHD_RPC_H__

#include <limits>
#include "sg_struct.h"
#include "sg_message.h"
#include "sg_manager.h"
#include "schd_ctx.h"

using namespace ai::sg;

namespace ai
{
namespace app
{

enum schd_opcode_t
{
	OSCHD_BASE = 0,
	OSCHD_INIT,
	OSCHD_START,
	OSCHD_STOP,
	OSCHD_STATUS,
	OSCHD_CHECK,
	OSCHD_ERASE,
	OSCHD_MARK,
	OSCHD_INFO,
	OSCHD_EXIT
};

struct schd_rqst_t
{
	int opcode;
	int datalen;
	long placeholder;

	const char * data() const;
	char * data();
	static size_t need(int size) { return (sizeof(schd_rqst_t) - sizeof(long) + size); }
};

struct schd_rply_t
{
	int error_code;
	int rtn;
	int flags;
	int num;
	int datalen;
	long placeholder;

	const char * data() const;
	char * data();
	static size_t need(int size) { return (sizeof(schd_rply_t) - sizeof(long) + size); }
};

struct schd_stop_rqst_t
{
	pid_t pid;
	time_t start_time;
	int wait_time;
	int signo;
};

class schd_rpc : public sg_manager
{
public:
	static schd_rpc& instance();

	bool do_init(int mid);
	bool do_start(int mid, const string& entry_name, const string& hostname, const string& command);
	bool do_stop(schd_entry_t& entry, schd_proc_t& proc);
	bool do_status(int mid);
	bool do_check(int mid);
	bool do_erase(const string& entry_name);
	bool do_mark(const string& entry_name);
	bool do_iclean(const string& entry_name, schd_entry_t& entry, const schd_cfg_entry_t& cfg_entry);
	bool do_undo(const string& entry_name, schd_entry_t& entry, const schd_cfg_entry_t& cfg_entry, int undo_id);
	bool do_autoclean(const string& entry_name, schd_entry_t& entry, const schd_cfg_entry_t& cfg_entry, int undo_id);
	bool report_info(const string& message);
	bool report_exit();
	void report(const schd_proc_t& proc);
	void report(const string& entry_name, const schd_entry_t& entry);
	void report();
	void report(const string& entry_name);

private:
	schd_rpc();
	virtual ~schd_rpc();

	bool do_erase_internal(int mid, const string& entry_name);
	bool do_mark_internal(int mid, const string& entry_name);

	sg_api& api_mgr;
	schd_ctx *SCHD;

	static schd_rpc _instance;
};

}
}

#endif

