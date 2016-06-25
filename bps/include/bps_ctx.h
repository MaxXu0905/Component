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
	// ҵ�������
	std::vector<bps_process_t> processes;
	// ҵ�����������
	std::vector<int> indice;
	// ҵ���������
	std::vector<compiler::func_type> funs;
};

class bps_ctx
{
public:
	static bps_ctx * instance();
	~bps_ctx();

	bool get_config();
	void set_svcname(const std::string& svc_key, std::string& svc_name) const;

	// ҵ��ؼ���
	std::vector<std::string> _BPS_svc_keys;
	// ҵ��ؼ����ļ�
	std::string _BPS_svc_file;
	// ҵ�����ð汾
	int _BPS_version;
	// ҵ��������
	boost::unordered_map<std::string, bps_adaptor_t> _BPS_adaptors;
	// ������
	compiler *_BPS_cmpl;
	// ���ݷ���
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

