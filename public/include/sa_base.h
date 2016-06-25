#if !defined(__SA_BASE_H__)
#define __SA_BASE_H__

#include "sg_struct.h"
#include "sa_manager.h"

namespace ai
{
namespace app
{

using namespace ai::sg;

int const STATUS_PREFIX = 1001001;
int const STATUS_INVALID = 9999000;

class sa_base : public sa_manager
{
public:
	sa_base(int sa_id_);
	virtual ~sa_base();

	// Allocate resource for current instance
	void init();
	// Callback function after all SAs are initialized.
	void init2();
	// Free resource for current instance
	void destroy();
	// main routine to do things
	void run();
	// Callback function after run
	virtual void post_run();
	// routine to print preparing information
	void dump(std::ostream& os);
	// rollback to initial state
	void rollback();
	// cleanup input directory
	void iclean();
	// handle records already set by other routines
	virtual bool call();

	// get current SA's com_key
	const std::string& get_com_key() const;
	// get current SA's svc_key
	const std::string& get_svc_key() const;
	// get current SA's sa_id
	const int& get_id() const;
	// get current SA's adaptor parameter
	const sa_adaptor_t& get_adaptor() const;
	// get current SA's input class
	sa_input * get_sa_input();
	// get status
	const int& get_status() const;
	// get global address
	const char ** get_global() const;
	// get global address
	char ** get_global();
	// get inputs address
	const char *** get_inputs() const;
	// get inputs address
	char *** get_inputs();
	// get current handled input address
	const char ** get_input() const;
	const char ** get_input(int input_idx) const;
	// get current handled input address
	char ** get_input();
	char ** get_input(int input_idx);
	// get input record's serial
	const int& get_input_serial() const;
	const int& get_input_serial(int input_idx) const;
	// get input record's serial
	int& get_input_serial();
	int& get_input_serial(int input_idx);
	// get output address
	const char ** get_output() const;
	// get output address
	char ** get_output();
	// handler when break point reaches
	void on_save();
	// handler when fatal error reaches
	void on_fatal();
	// handler when end of file reaches
	void on_eof(int stage);
	// handler when exception on complete
	void on_complete();
	// handler when recovery from exception
	void on_recover();

	// Check if batch mode is supported.
	virtual bool support_batch() const = 0;
	// Check if concurrency mode is supported.
	virtual bool support_concurrency() const = 0;
	// transfer input instead of output to next SA.
	virtual bool transfer_input() const;

protected:
	// Callback function before init() is invoked.
	virtual void pre_init();
	// Callback function after init() is invoked.
	virtual void post_init();
	// Callback function before init2() is invoked.
	virtual void pre_init2();
	// Callback function after init2() is invoked.
	virtual void post_init2();
	// Callback function before destroy() is invoked.
	virtual void pre_destroy();
	// Callback function after destroy() is invoked.
	virtual void post_destroy();
	// set service name to invoke component service
	virtual void set_svcname(int input_idx);
	// Callback function if input2 is required.
	virtual void set_input2(int input_idx);
	// Transfer record to next step.
	virtual bool transfer(int input_idx);
	// Rollback SA specific job
	virtual void pre_rollback();
	// Callback function after file opened
	virtual void post_open();

	// 以下定义初始化参数
	int sa_id;						// SA ID
	sa_adaptor_t& adaptor;			// SA参数
	master_dblog_t& master;			// SA主日志
	int batch;						// 一次调用记录数
	int concurrency;				// 同时发起acall数
	int per_call;					// 最多多少条记录调用一次
	string svc_name;				// 当前SA对应的服务名

	// 以下定义输入输出对象
	sa_input *isrc_mgr;				// 输入对象
	sa_output *otgt_mgr;			// 输出对象
	sa_output *oinv_mgr;			// 无效对象
	sa_output *oerr_mgr;			// 错误对象
	sa_output *osummary_mgr;		// 汇总对象
	sa_output *odistr_mgr;			// 分发对象
	sa_output *ostat_mgr;			// 统计对象

	// 以下定义状态相关的变量
	int execute_count;				// 执行记录数
	int sndmsg_count;				// 发送消息数
	int status;					// 执行状态
	char ***inputs;				// 输入变量数组
	int *input_serials;				// 输入变量序列号数组
	char **output;					// 输出变量数组

	int input2_size;				// 转化后的输入变量长度数组
	char ***inputs2;				// 转化后的输入变量数组

	// 以下定义消息相关的变量
	int snd_len;					// 发送消息最大长度
	int rcv_len;					// 接收消息最大长度
	int *handles;					// 异步调用句柄数组
	message_pointer *snd_msgs;		// 发送消息数组
	message_pointer *rcv_msgs;		// 接收消息

	// 以下定义消息处理相关的变量
	sa_rqst_t *rqst;
	char *rqst_data;
	sa_rply_t *rply;
	int *results;
	char *rply_data;

	void packup();
	void packup_begin(sg_message& msg);
	void packup_global();
	void packup_input(int input_idx);
	void packup_end(sg_message& msg);
	bool extract(message_pointer& rcv_msg, int input_idx);
	void extract_begin(message_pointer& rcv_msg);
	void extract_global();
	void extract_output(int input_idx);
	void extract_end();
	bool check_policy();

private:
	bool handle_record(int input_idx);
	void set_fixed_input();

	bool inited;
	bool pass_validate;
};

struct sa_base_creator_t
{
	std::string com_key;
	sa_base * (*create)(int sa_id);
};

#define DECLARE_SA_BASE(_com_key, _class_name) \
namespace \
{ \
\
class sa_base_initializer \
{ \
public: \
	sa_base_initializer() { \
		sap_ctx *SAP = sap_ctx::instance(); \
		sa_base_creator_t item; \
\
		BOOST_FOREACH(const sa_base_creator_t& v, SAP->_SAP_base_creator) { \
			if (v.com_key == #_com_key) { \
				gpp_ctx *GPP = gpp_ctx::instance(); \
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: Duplicate sa_base creator for {1}") % #_com_key).str(APPLOCALE)); \
				exit(1); \
			} \
		} \
\
		item.com_key = _com_key; \
		item.create = &sa_base_initializer::create; \
		SAP->_SAP_base_creator.push_back(item); \
	} \
\
private: \
	static sa_base * create(int sa_id) { \
		return new _class_name(sa_id); \
	} \
} initializer; \
\
}

}
}

#endif

