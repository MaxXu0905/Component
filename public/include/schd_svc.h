#if !defined(__SCHD_SVC_H__)
#define __SCHD_SVC_H__

#include "sg_public.h"
#include "schd_ctx.h"
#include "schd_rpc.h"

using namespace ai::sg;

namespace ai
{
namespace app
{

class schd_svc : public sg_svc
{
public:
	schd_svc();
	~schd_svc();

	svc_fini_t svc(message_pointer& svcinfo);

private:
	void do_init(const schd_rqst_t& rqst);
	void do_start(const schd_rqst_t& rqst);
	void do_stop(const schd_rqst_t& rqst);
	void do_status(const schd_rqst_t& rqst);
	void do_check(const schd_rqst_t& rqst);
	void do_erase(const schd_rqst_t& rqst);
	void do_mark(const schd_rqst_t& rqst);
	void do_info(const schd_rqst_t& rqst);
	void do_exit(const schd_rqst_t& rqst);

	bool insert(const schd_agent_proc_t& item);
	bool update(const schd_agent_proc_t& item);
	bool erase(const schd_agent_proc_t& item);
	bool truncate();

	schd_ctx *SCHD;
	message_pointer rply_msg;
};

}
}

#endif

