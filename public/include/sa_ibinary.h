#if !defined(__SA_IBINARY_H__)
#define __SA_IBINARY_H__

#include "sa_ifixed.h"

namespace ai
{
namespace app
{

using namespace ai::sg;

class sa_ibinary : public sa_ifixed
{
public:
	sa_ibinary(sa_base& _sa, int _flags);
	~sa_ibinary();

private:
	void init();
	void init2();
	int read();
	int do_record();
	int get_record_len(const char *record) const;

	// 参数列表
	int block_filler;						// 块填充符
	std::string rule_record_len;			// 获取基类长度规则

	int input_mlen;							// 记录最小长度
	std::vector<int> input_lens;			// 记录长度数组

	int record_len_idx;						// 记录长度规则ID
	compiler::func_type record_len_func;	// 记录长度规则
};

}
}

#endif

