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

	// ���¶����ʼ������
	svcp_ctx *SVCP;
	const svc_adaptor_t *adaptor;	// ��ǰ�������Ϣ����

	// ���¶���״̬��صı���
	char **global;					// ȫ�ֱ�������
	char **input;					// �����������
	char ***outputs;				// �����������
	int *status;					// ����״̬����
	int input_serial;				// �����¼�������к�
	int array_size;					// ��������С
	string svc_key;					// ҵ������

	// ���¶�����Ϣ������صı���
	int rows;						// �����¼��
	sa_rqst_t *rqst;				// ������Ϣ
	char *rqst_data;				// ������Ϣ������
	sa_rply_t *rply;				// Ӧ����Ϣ
	int *results;					// ���״ָ̬��
	char *rply_data;				// Ӧ����Ϣ������

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

	std::vector<int> global_sizes;				// ȫ�ֱ�����������
	std::vector<int> input_sizes;				// ���������������
	std::vector<int> output_sizes;				// ���������������
};

}
}

#endif

