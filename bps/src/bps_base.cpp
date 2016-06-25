#include "bps_base.h"
#include "sdc_api.h"

namespace ai
{
namespace app
{

using namespace ai::sg;
using namespace ai::scci;

bps_base::bps_base(int sa_id_)
	: sa_base(sa_id_)
{
}

bps_base::~bps_base()
{
}

bool bps_base::support_batch() const
{
	bool retval = true;
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", &retval);
#endif

	return retval;
}

bool bps_base::support_concurrency() const
{
	bool retval = true;
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", &retval);
#endif

	return retval;
}

void bps_base::set_svcname(int input_idx)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	svc_name = "BPS_";
	svc_name += adaptor.parms.svc_key;
}

void bps_base::post_init2()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	sdc_api& sdc_mgr = sdc_api::instance();
	if (!sdc_mgr.connected())
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Missing connecting to SDC")).str(APPLOCALE));
}

DECLARE_SA_BASE("BPS", bps_base)

}
}

