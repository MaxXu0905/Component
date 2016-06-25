#if !defined(__BPS_BASE_H__)
#define __BPS_BASE_H__

#include "sa_internal.h"
#include "bps_ctx.h"

namespace ai
{
namespace app
{

using namespace ai::sg;

class bps_base : public sa_base
{
public:
	bps_base(int sa_id_);
	virtual ~bps_base();

private:
	virtual bool support_batch() const;
	virtual bool support_concurrency() const;
	virtual void set_svcname(int input_idx);
	virtual void post_init2();
};

}
}

#endif

