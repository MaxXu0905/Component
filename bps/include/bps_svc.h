#if !defined(__BPS_SVC_H__)
#define __BPS_SVC_H__

#include "sg_public.h"
#include "dbct_ctx.h"
#include "bps_ctx.h"
#include "svc_base.h"

using namespace ai::sg;

namespace ai
{
namespace app
{

class bps_svc : public svc_base
{
public:
	bps_svc();
	~bps_svc();

private:
	void set_user(int64_t user_id);
	bool handle_record(int output_idx);

	dbct_ctx *DBCT;
	bps_ctx *BPS;
};

}
}

#endif

