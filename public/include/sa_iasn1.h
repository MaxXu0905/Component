#if !defined(__SA_IASN1_H__)
#define __SA_IASN1_H__

#include "sa_istream.h"
#include "asn1.h"

namespace ai
{
namespace app
{

using namespace ai::sg;
using namespace ai::scci;

struct parent_children_t
{
	bool parent_var;
	bool children_var;
	int parent_serial;
	int parent_type;
	size_t parent_start;
	AsnLen header_len;
	AsnLen parent_len;
	AsnLen children_len;

	void clear();
};

int const MAX_BLOCK_LEN = 655360;

class sa_iasn1 : public sa_istream
{
public:
	sa_iasn1(sa_base& _sa, int _flags);
	~sa_iasn1();

private:
	int read();
	int do_record(AsnTag tag_, AsnLen len_);
	void asn2record(const input_record_t& input_record, const char *tag_str, AsnLen len_);
	int get_record_serial(const char *record) const;
	void post_open();

	// �����б�
	size_t max_block_len;			// �����С
	int file_root_tag;				// �ļ���TAG
	int body_node_tag;				// ��ڵ�TAG
	int block_data_tag;				// ������TAG
	int block_filler;				// ������
	int skip_record_len;			// �����ֽ���
	bool body_one_cdr;				// ����һ����¼
	bool block_one_cdr;				// ����һ����¼

	// �����ֶ��ڶ��󴴽�ʱ��ʼ��
	bool has_body_tag;				// �Ƿ�����TAG
	bool has_block_tag;				// �Ƿ��п�TAG

	// �����ֶ����ļ���ʱ��ʼ��
	char *block_buf;				// �黺��
	asn1 *asnfile_ptr;				// ASN1����ָ��
	AsnTag body_tag;				// ��TAG
	AsnTag block_tag;				// ��TAG
	AsnTag record_tag;				// ��¼TAG
	AsnLen body_len;				// �峤��
	AsnLen body_tag_len;			// ��TAG����
	AsnLen body_len_len;			// �峤�ȵĳ���
	size_t body_start;				// �忪ʼλ��
	AsnLen block_len;				// �鳤��
	size_t block_start;				// �鿪ʼλ��
	AsnLen record_len;				// ��¼����
	AsnLen buffer_len;				// ���泤��
	bool has_file_tag;				// �Ƿ����ļ�TAG
};

}
}

#endif

