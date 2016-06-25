#if !defined(__SGSSH_SGIP__H__)
#define __SGSSH_SGIP__H__

#include "sgssh.h"
#include "dbc_internal.h"

namespace ai
{
namespace sgssh_sgip
{

class sgip
{
public:
	sgip();
	~sgip();
	void load_sgconf();
	set<string> sgip_addr;
protected:
	sgt_ctx *SGT;	
};
}
}
#endif
