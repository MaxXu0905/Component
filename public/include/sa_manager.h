#if !defined(__SA_MANAGER_H__)
#define __SA_MANAGER_H__

#include "sg_public.h"
#include "sa_struct.h"

namespace ai
{
namespace sg
{

class sg_api;

}
}

namespace ai
{
namespace app
{

using namespace ai::sg;

class sap_ctx;
class sat_ctx;

enum sa_manager_t {
	SA_DBLOG_MANAGER,
	SA_GLOBAL_MANAGER,
	SA_TOTAL_MANAGER
};

class sa_manager : public sg_manager
{
public:
	sa_manager();
	virtual ~sa_manager();

protected:
	sg_api& api_mgr;
	sap_ctx *SAP;
	sat_ctx *SAT;
};

}
}

#endif

