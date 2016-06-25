#if !defined(__CAL_MANAGER_H__)
#define __CAL_MANAGER_H__

#include "sa_internal.h"
#include "cal_struct.h"

namespace ai
{
namespace app
{

using namespace ai::sg;

class calp_ctx;
class calt_ctx;

enum cal_manager_t
{
	CAL_TREE_MANAGER,
	CAL_POLICY_MANAGER,
	CAL_RULE_MANAGER,
	CAL_TOTAL_MANAGER
};

class cal_manager : public sa_manager
{
public:
	cal_manager();
	virtual ~cal_manager();

protected:
	calp_ctx *CALP;
	calt_ctx *CALT;
};

}
}

#endif

