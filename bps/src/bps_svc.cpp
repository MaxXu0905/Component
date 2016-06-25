#include "bps_svc.h"
#include "dbc_api.h"
#include "sdc_api.h"

using namespace std;
using namespace ai::sg;

namespace ai
{
namespace app
{

bps_svc::bps_svc()
{
	DBCT = dbct_ctx::instance();
	BPS = bps_ctx::instance();
}

bps_svc::~bps_svc()
{
}

void bps_svc::set_user(int64_t user_id)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, (_("user_id={1}") % user_id).str(APPLOCALE), NULL);
#endif

	if (BPS->_BPS_use_dbc)
		reinterpret_cast<dbc_api *>(BPS->_BPS_data_mgr)->set_user(user_id);
	else
		reinterpret_cast<sdc_api *>(BPS->_BPS_data_mgr)->set_user(user_id);
}

bool bps_svc::handle_record(int output_idx)
{
	bool retval = false;
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, (_("output_idx={1}") % output_idx).str(APPLOCALE), &retval);
#endif

	SGT->_SGT_ur_code = 0;

	boost::unordered_map<string, bps_adaptor_t>& bps_adaptors = BPS->_BPS_adaptors;
	boost::unordered_map<string, bps_adaptor_t>::const_iterator iter = bps_adaptors.find(svc_key);
	if (iter == bps_adaptors.end()) {
		GPP->write_log(__FILE__, __LINE__, (_("ERROR: Unknown operation key {1}") % svc_key).str(APPLOCALE));
		return retval;
	}

	const bps_adaptor_t& bps_adaptor = iter->second;
	const vector<compiler::func_type>& funs = bps_adaptor.funs;

	if (input_serial < 0 || input_serial >= funs.size()) {
		GPP->write_log(__FILE__, __LINE__, (_("ERROR: input_serial {1} out of range") % input_serial).str(APPLOCALE));
		return retval;
	}

	const compiler::func_type& fun = funs[input_serial];
	status[output_idx] = (*fun)(BPS->_BPS_data_mgr, global, const_cast<const char **>(input), outputs[output_idx]);
	if (SGT->_SGT_ur_code > 0)
		SGT->_SGT_error_code = SGENOENT;

	retval = true;
	return retval;
}

}
}

