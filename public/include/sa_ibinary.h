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

	// �����б�
	int block_filler;						// ������
	std::string rule_record_len;			// ��ȡ���೤�ȹ���

	int input_mlen;							// ��¼��С����
	std::vector<int> input_lens;			// ��¼��������

	int record_len_idx;						// ��¼���ȹ���ID
	compiler::func_type record_len_func;	// ��¼���ȹ���
};

}
}

#endif

