#include "cal_manager.h"
#include "calp_ctx.h"
#include "calt_ctx.h"

namespace ai
{
namespace app
{

cal_manager::cal_manager()
{
	CALP = calp_ctx::instance();
	CALT = calt_ctx::instance();
}

cal_manager::~cal_manager()
{
}

}
}

