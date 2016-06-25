#if !defined(__BPS_CTX_H__)
#define __BPS_CTX_H__

#include "sg_internal.h"
#include "svcp_ctx.h"

using namespace ai::sg;

namespace ai
{
namespace app
{

struct bps_process_t
{
	std::vector<std::string> rules;
};

struct bps_adaptor_t
{
	// 业务处理规则
	std::vector<bps_process_t> processes;
	// 业务处理规则索引
	std::vector<int> indice;
	// 业务处理规则函数
	std::vector<compiler::func_type> funs;
};

class bps_ctx
{
public:
	static bps_ctx * instance();
	~bps_ctx();

	bool get_config();
	void set_svcname(const std::string& svc_key, std::string& svc_name) const;

	// 业务关键字
	std::vector<std::string> _BPS_svc_keys;
	// 业务关键字文件
	std::string _BPS_svc_file;
	// 业务配置版本
	int _BPS_version;
	// 业务适配器
	boost::unordered_map<std::string, bps_adaptor_t> _BPS_adaptors;
	// 编译器
	compiler *_BPS_cmpl;
	// 数据访问
	bool _BPS_use_dbc;
	void *_BPS_data_mgr;

private:
	bps_ctx();

	void load_config(const string& config);
	void load_services(const bpt::iptree& pt);
	void load_parms(const bpt::iptree& pt, svc_adaptor_t& adaptor);
	void load_global(const bpt::iptree& pt, svc_adaptor_t& adaptor);
	void load_input(const bpt::iptree& pt, svc_adaptor_t& adaptor);
	void load_output(const bpt::iptree& pt, svc_adaptor_t& adaptor);
	void load_process(const bpt::iptree& pt, bps_adaptor_t& bps_adaptor);

	static void init_once();

	static boost::once_flag once_flag;
	static bps_ctx *_instance;
};

}
}

#endif

