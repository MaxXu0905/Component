#if !defined(__SVCP_CTX_H__)
#define __SVCP_CTX_H__

#include "sa_struct.h"

namespace ai
{
namespace app
{

using namespace ai::sg;

struct svc_parms_t
{
	std::string svc_key;
	int version;
	bool disable_global;
};

struct svc_adaptor_t
{
	svc_parms_t parms;					// 通用参数

	// Global definition.
	field_map_t global_map;				// 全局变量与下标的对应关系
	std::vector<int> input_globals;			// 作为输入传递的全局变量列表
	std::vector<int> output_globals;			// 作为输出传递的全局变量列表
	std::vector<int> global_sizes;				// 全局变量长度数组

	// Input definition.
	std::vector<field_map_t> input_maps;		// 输入变量与下标的对应关系
	std::vector<int> input_sizes;				// 输入变量长度数组

	// Output definition.
	field_map_t output_map;				// 输出变量与下标的对应关系
	std::vector<int> output_sizes;			// 输出变量长度数组
};

class svcp_ctx
{
public:
	static svcp_ctx * instance();
	~svcp_ctx();

	// SVC_KEY与配置定义的映射关系
	boost::unordered_map<std::string, svc_adaptor_t> _SVCP_adaptors;

private:
	svcp_ctx();

	static void init_once();

	static boost::once_flag once_flag;
	static svcp_ctx *_instance;
};

}
}

#endif

