#if !defined(__SCHD_CONFIG_H__)
#define __SCHD_CONFIG_H__

#include "schd_ctx.h"

namespace ai
{
namespace app
{

using namespace ai::sg;
using namespace std;

int const DFLT_SCHD_MIN = 1;
int const DFLT_SCHD_MAX = 10;

class schd_config : public sg_manager
{
public:
	schd_config();
	~schd_config();

	void load(const std::string& config_file);
	void load_xml(const string& config);

private:
	void load_entries(const bpt::iptree& pt);
	void load_dependencies(const bpt::iptree& pt);
	void load_conditions(const bpt::iptree& pt);
	void load_actions(const bpt::iptree& pt);

	schd_ctx *SCHD;
};

}
}

#endif

