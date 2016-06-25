#include "sa_internal.h"

namespace ai
{
namespace app
{

using namespace ai::sg;

sa_manager::sa_manager()
	: api_mgr(sg_api::instance())
{
	SAP = sap_ctx::instance();
	SAT = sat_ctx::instance();
}

sa_manager::~sa_manager()
{
}

}
}

