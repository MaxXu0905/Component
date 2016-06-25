#if !defined(__SA_CONFIG_H__)
#define __SA_CONFIG_H__

#include "sa_struct.h"

namespace ai
{
namespace app
{

using namespace ai::sg;
using namespace ai::scci;
using namespace std;

int const DFLT_PER_RECORDS = 1000;
int const DFLT_PER_REPORT = 1;
int const DFLT_MAX_ERROR_RECORDS = std::numeric_limits<int>::max();
int const DFLT_MAX_OPEN_FILES = 1000;

int const DFLT_VERSION = -1;
int const DFLT_BATCH = 1000;
int const DFLT_CONCURRENCY = 10;
int const DFLT_EXCEPTION_WAITS = 1000;

class sa_config : public sg_manager
{
public:
	sa_config();
	~sa_config();

	void load(const std::string& config_file);
	void load_xml(const string& config);

private:
	void load_resource(const bpt::iptree& pt);
	void load_global(const bpt::iptree& pt);
	void load_adaptors(const bpt::iptree& pt);
	void load_parms(const bpt::iptree& pt, sa_adaptor_t& adaptor);
	void load_source(const bpt::iptree& pt, sa_adaptor_t& adaptor);
	void load_target(const bpt::iptree& pt, sa_adaptor_t& adaptor);
	void load_invalid(const bpt::iptree& pt, sa_adaptor_t& adaptor);
	void load_error(const bpt::iptree& pt, sa_adaptor_t& adaptor);
	void load_input(const bpt::iptree& pt, sa_adaptor_t& adaptor);
	void load_output(const bpt::iptree& pt, sa_adaptor_t& adaptor);
	void load_summary(const bpt::iptree& pt, sa_adaptor_t& adaptor);
	void load_distribute(const bpt::iptree& pt, sa_adaptor_t& adaptor);
	void load_stat(const bpt::iptree& pt, sa_adaptor_t& adaptor);
	void load_args(const bpt::iptree& pt, sa_adaptor_t& adaptor);
	void replace_desc(const string& src, const string& element_name, int field_size, string& dst, int& dst_times);
	bool get_serial(const string& field_name, const sa_adaptor_t& adaptor, field_orign_enum& field_orign, int& field_serial);

	sap_ctx *SAP;
};

}
}

#endif

