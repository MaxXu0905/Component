#if !defined(__SVC_BASE_H__)
#define __SVC_BASE_H__

#include "sg_svc.h"
#include "svcp_ctx.h"

namespace ai
{
namespace app
{

using namespace ai::sg;
using namespace ai::scci;

class svc_base : public sg_svc
{
public:
	svc_base();
	~svc_base();

	svc_fini_t svc(message_pointer& svcinfo);

protected:
	virtual void pre_extract_input();
	virtual void post_extract_input();
	virtual void set_user(int64_t user_id);
	virtual bool handle_meta();
	virtual bool handle_record(int output_idx) = 0;

	// 以下定义初始化参数
	svcp_ctx *SVCP;
	const svc_adaptor_t *adaptor;	// 当前处理的消息配置

	// 以下定义状态相关的变量
	char **global;					// 全局变量数组
	char **input;					// 输入变量数组
	char ***outputs;				// 输出变量数组
	int *status;					// 处理状态数组
	int input_serial;				// 输入记录类型序列号
	int array_size;					// 输出数组大小
	string svc_key;					// 业务类型

	// 以下定义消息处理相关的变量
	int rows;						// 输入记录数
	sa_rqst_t *rqst;				// 请求消息
	char *rqst_data;				// 请求消息数据区
	sa_rply_t *rply;				// 应答消息
	int *results;					// 结果状态指针
	char *rply_data;				// 应答消息数据区

private:
	void packup(message_pointer& msg);
	void packup_begin(message_pointer& msg);
	void packup_global();
	void packup_output(int output_idx);
	void packup_end(message_pointer& msg);
	int extract(message_pointer& msg);
	void extract_begin(message_pointer& msg);
	void extract_global();
	void extract_input();
	void extract_end();
	void alloc(int new_size);
	void pre_extract_global();
	void pre_handle_record(int output_idx);

	std::vector<int> global_sizes;				// 全局变量长度数组
	std::vector<int> input_sizes;				// 输入变量长度数组
	std::vector<int> output_sizes;				// 输出变量长度数组
};

}
}

#endif

