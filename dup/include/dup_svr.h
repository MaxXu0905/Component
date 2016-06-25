#if !defined(__DUP_SVR_H__)
#define __DUP_SVR_H__

#include "sg_svr.h"
#include "dup_ctx.h"

using namespace ai::sg;

namespace ai
{
namespace app
{

class dup_svr : public sg_svr
{
public:
	dup_svr();
	virtual ~dup_svr();

protected:
	virtual int svrinit(int argc, char **argv);
	virtual int svrfini();

private:
	bool advertise();
	void load_parms();

	dup_ctx *DUP;
};

}
}

#endif

