#if !defined(__BPS_SVR_H__)
#define __BPS_SVR_H__

#include "sg_svr.h"
#include "bps_ctx.h"

using namespace ai::sg;

namespace ai
{
namespace app
{

class bps_svr : public sg_svr
{
public:
	bps_svr();
	virtual ~bps_svr();

protected:
	virtual int svrinit(int argc, char **argv);
	virtual int svrfini();

private:
	bool advertise();

	bps_ctx *BPS;
};

}
}

#endif

