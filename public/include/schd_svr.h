#if !defined(__SCHD_SVR_H__)
#define __SCHD_SVR_H__

#include "sg_public.h"
#include "schd_ctx.h"

using namespace ai::sg;

namespace ai
{
namespace app
{

class schd_svr : public sg_svr
{
public:
	schd_svr();
	virtual ~schd_svr();

protected:
	virtual int svrinit(int argc, char **argv);
	virtual int svrfini();

private:
	bool prepare();
	bool select();
	bool advertise();

	sg_api& api_mgr;
	schd_ctx *SCHD;
};

}
}

#endif

