#if !defined(__PARSER_CONFIG_H__)
#define __PARSER_CONFIG_H__

#include "sg_internal.h"
#include "svcp_ctx.h"
#include "parser_code.h"
#include "prsp_ctx.h"

using namespace ai::sg;

namespace ai
{
namespace app
{

class parser_config
{
public:
	parser_config();
	~parser_config();

	void run(const string& input_file, const string& output_file);

private:
	void build(string& process);
	void load(const string& config_file);
	void save(const string& config_file);

	void load_services(const bpt::iptree& pt);
	void load_parms(const bpt::iptree& pt, bps_config_t& config);
	void load_global(const bpt::iptree& pt, bps_config_t& config);
	void load_input(const bpt::iptree& pt, bps_config_t& config);
	void load_output(const bpt::iptree& pt, bps_config_t& config);
	void load_process(const bpt::iptree& pt, bps_config_t& config);
	
	void save_services(bpt::iptree& pt);
	void save_parms(bpt::iptree& pt, const bps_config_t& config);
	void save_global(bpt::iptree& pt, const bps_config_t& config);
	void save_input(bpt::iptree& pt, const bps_config_t& config);
	void save_output(bpt::iptree& pt, const bps_config_t& config);
	void save_process(bpt::iptree& pt, const bps_config_t& config);

	prsp_ctx *PRSP;
	vector<bps_config_t> configs;
};

}
}

#endif

