#include "svc_base.h"

namespace ai
{
namespace app
{

using namespace ai::sg;
using namespace ai::scci;

svc_base::svc_base()
{
	SVCP = svcp_ctx::instance();

	global = NULL;
	input = NULL;
	outputs = NULL;
	status = NULL;
	array_size = 0;
}

svc_base::~svc_base()
{
}

svc_fini_t svc_base::svc(message_pointer& svcinfo)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	try {
		switch (extract(svcinfo)) {
		case -1:
			return svc_fini(SGFAIL, SGT->_SGT_ur_code, svcinfo);
		case 0:
			packup(svcinfo);
			return svc_fini(SGSUCCESS, 0, svcinfo);
		default:
			svcinfo->set_length(0);
			return svc_fini(SGSUCCESS, 0, svcinfo);
		}
	} catch (exception& ex) {
		GPP->write_log(__FILE__, __LINE__, ex.what());
		return svc_fini(SGFAIL, 0, svcinfo);
	}
}

void svc_base::pre_extract_input()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif
}

void svc_base::post_extract_input()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif
}

void svc_base::set_user(int64_t user_id)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, (_("user_id={1}") % user_id).str(APPLOCALE), NULL);
#endif
}

bool svc_base::handle_meta()
{
	bool retval = true;
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", &retval);
#endif

	return retval;
}

void svc_base::packup(message_pointer& msg)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	packup_begin(msg);
	packup_global();

	for (int i = 0; i < rows; i++)
		packup_output(i);

	packup_end(msg);
}

// 开始打包
void svc_base::packup_begin(message_pointer& msg)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	rply = reinterpret_cast<sa_rply_t *>(msg->data());
	rply->rows = 0;
	rply->global_size = adaptor->output_globals.size();
	rply->output_size = adaptor->output_sizes.size();
	results = &rply->placeholder;
	memcpy(results, status, sizeof(int) * rows);
	rply_data = reinterpret_cast<char *>(results + rows);
}

// 打包全局变量
void svc_base::packup_global()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	const vector<int>& output_globals = adaptor->output_globals;
	int *global_sizes = reinterpret_cast<int *>(rply_data);

	rply_data = reinterpret_cast<char *>(global_sizes + rply->global_size);
	for (int i = 0; i < output_globals.size(); i++) {
		const int& idx = output_globals[i];
		int data_size = strlen(global[idx]) + 1;

		global_sizes[i] = data_size ;
		memcpy(rply_data, global[idx], data_size);
		rply_data += data_size;
	}
}

// 打包输入变量
void svc_base::packup_output(int output_idx)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	// 先按INT对齐
	rply_data = reinterpret_cast<char *>(common::round_up(reinterpret_cast<long>(rply_data), static_cast<long>(sizeof(int))));

	int *output_sizes = reinterpret_cast<int *>(rply_data);
	char **output = outputs[output_idx];

	rply_data = reinterpret_cast<char *>(output_sizes + rply->output_size);
	for (int i = 0; i < rply->output_size; i++) {
		int data_size = strlen(output[i]) + 1;

		output_sizes[i] = data_size;
		memcpy(rply_data, output[i], data_size);
		rply_data += data_size;
	}

	rply->rows++;
}

// 结束打包
void svc_base::packup_end(message_pointer& msg)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	rply->datalen = rply_data - reinterpret_cast<char *>(rply);
	msg->set_length(rply->datalen);
}

// -1: 错误
// 0: 记录处理正常
// 1: 元数据处理正常
int svc_base::extract(message_pointer& msg)
{
	int retval = -1;
#if defined(DEBUG)
	scoped_debug<int> debug(500, __PRETTY_FUNCTION__, "", &retval);
#endif

	extract_begin(msg);
	if (rqst->flags & SA_METAMSG) {
		if (!handle_meta())
			return retval;

		retval = 1;
		return retval;
	}

	pre_extract_global();
	extract_global();
	pre_extract_input();

	for (int i = 0; i < rows; i++) {
		extract_input();
		pre_handle_record(i);
		if (!handle_record(i))
			return retval;
	}

	post_extract_input();
	extract_end();

	retval = 0;
	return retval;
}

// 开始提取
void svc_base::extract_begin(message_pointer& msg)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	rqst = reinterpret_cast<sa_rqst_t *>(msg->data());
	svc_key = rqst->svc_key;
	rqst_data = reinterpret_cast<char *>(&rqst->placeholder);
	rows = rqst->rows;

	// 设置用户
	set_user(rqst->user_id);

	// 根据svc_key获取配置参数
	boost::unordered_map<string, svc_adaptor_t>::const_iterator iter = SVCP->_SVCP_adaptors.find(svc_key);
	if (iter == SVCP->_SVCP_adaptors.end())
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Unknown operation key {1}") % svc_key).str(APPLOCALE));

	adaptor = &iter->second;

	// 有需要重新分配内存
	alloc(rows);
}

// 提取全局变量
void svc_base::extract_global()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	const vector<int>& input_globals = adaptor->input_globals;
	int *global_sizes = reinterpret_cast<int *>(rqst_data);

	if (rqst->global_size != input_globals.size())
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: operation contract failure({1}), global_size doesn't match.") % svc_key).str(APPLOCALE));

	rqst_data = reinterpret_cast<char *>(global_sizes + rqst->global_size);
	for (int i = 0; i < input_globals.size(); i++) {
		const int& idx = input_globals[i];
		int& data_size = global_sizes[i];
		if (data_size - 1 > adaptor->global_sizes[idx])
			throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: operation contract failure({1}), field size mismatch. index({2}), request({3}) > required({4})") % svc_key % i % (data_size - 1) % adaptor->global_sizes[idx]).str(APPLOCALE));

		memcpy(global[idx], rqst_data, data_size);
		rqst_data += data_size;
	}
}

// 提取输入变量
void svc_base::extract_input()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	// 先按INT对齐
	rqst_data = reinterpret_cast<char *>(common::round_up(reinterpret_cast<long>(rqst_data), static_cast<long>(sizeof(int))));

	int *record_serial = reinterpret_cast<int *>(rqst_data);
	input_serial = *record_serial;

	const vector<int>& inputs = adaptor->input_sizes;
	int *input_sizes = record_serial + 1;

	if (rqst->input_size > inputs.size())
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: operation contract failure({1}), input size mismatch. request({2}) > required({3})") % svc_key % rqst->input_size % inputs.size()).str(APPLOCALE));

	rqst_data = reinterpret_cast<char *>(input_sizes + rqst->input_size);
	for (int i = 0; i < rqst->input_size; i++) {
		int& data_size = input_sizes[i];
		if (data_size - 1 > inputs[i])
			throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: operation contract failure({1}), field size mismatch. index({2}), request({3}) > required({4})") % svc_key % i % (data_size - 1) % inputs[i]).str(APPLOCALE));

		memcpy(input[i], rqst_data, data_size);
		rqst_data += data_size;
	}
}

// 提取结束
void svc_base::extract_end()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif
}

void svc_base::alloc(int new_size)
{
	int i;
	int j;

	if (new_size <= array_size)
		return;

	if (array_size == 0) {
		for (boost::unordered_map<string, svc_adaptor_t>::const_iterator iter = SVCP->_SVCP_adaptors.begin(); iter != SVCP->_SVCP_adaptors.end(); ++iter) {
			const svc_adaptor_t& svc_adaptor = iter->second;
			const vector<int>& svc_global_sizes = svc_adaptor.global_sizes;
			const vector<int>& svc_input_sizes = svc_adaptor.input_sizes;
			const vector<int>& svc_output_sizes = svc_adaptor.output_sizes;

			for (int i = 0; i < svc_global_sizes.size(); i++) {
				if (i < global_sizes.size()) {
					if (global_sizes[i] < svc_global_sizes[i])
						global_sizes[i] = svc_global_sizes[i];
				} else {
					global_sizes.push_back(svc_global_sizes[i]);
				}
			}

			for (int i = 0; i < svc_input_sizes.size(); i++) {
				if (i < input_sizes.size()) {
					if (input_sizes[i] < svc_input_sizes[i])
						input_sizes[i] = svc_input_sizes[i];
				} else {
					input_sizes.push_back(svc_input_sizes[i]);
				}
			}

			for (int i = 0; i < svc_output_sizes.size(); i++) {
				if (i < output_sizes.size()) {
					if (output_sizes[i] < svc_output_sizes[i])
						output_sizes[i] = svc_output_sizes[i];
				} else {
					output_sizes.push_back(svc_output_sizes[i]);
				}
			}
		}

		const int& global_columns = global_sizes.size();
		if (global_columns > 0) {
			global = new char *[global_columns];
			for (i = 0; i < global_columns; i++) {
				const int& size = global_sizes[i];
				global[i] = new char[size + 1];
			}
		}

		const int& input_columns = input_sizes.size();
		if (input_columns > 0) {
			input = new char *[input_columns];
			for (i = 0; i < input_columns; i++) {
				const int& size = input_sizes[i];
				input[i] = new char[size + 1];
			}
		}
	}

	delete []status;
	status = new int[new_size];

	char ***old_outputs = outputs;

	outputs = new char **[new_size];
	for (i = 0; i < array_size; i++)
		outputs[i] = old_outputs[i];

	for (; i < new_size; i++) {
		const int& output_columns = output_sizes.size();
		if (output_columns > 0) {
			outputs[i] = new char *[output_columns];
			for (j = 0; j < output_columns; j++) {
				const int& size = output_sizes[j];
				outputs[i][j] = new char[size + 1];
			}
		} else {
			outputs[i] = NULL;
		}
	}

	delete []old_outputs;
	array_size = new_size;
}

void svc_base::pre_extract_global()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	const vector<int>& output_globals = adaptor->output_globals;
	for (int i = 0; i < output_globals.size(); i++) {
		const int& idx = output_globals[i];

		global[idx][0] = '\0';
	}
}

void svc_base::pre_handle_record(int output_idx)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, (_("output_idx={1}") % output_idx).str(APPLOCALE), NULL);
#endif

	char **output = outputs[output_idx];
	for (int i = 0; i < adaptor->output_sizes.size(); i++)
		output[i][0] = '\0';
}

}
}

