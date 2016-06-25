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

	// 参数列表
	size_t max_block_len;			// 缓存大小
	int file_root_tag;				// 文件根TAG
	int body_node_tag;				// 体节点TAG
	int block_data_tag;				// 块数据TAG
	int block_filler;				// 块填充符
	int skip_record_len;			// 跳过字节数
	bool body_one_cdr;				// 体是一条记录
	bool block_one_cdr;				// 块是一条记录

	// 以下字段在对象创建时初始化
	bool has_body_tag;				// 是否有体TAG
	bool has_block_tag;				// 是否有块TAG

	// 以下字段在文件打开时初始化
	char *block_buf;				// 块缓存
	asn1 *asnfile_ptr;				// ASN1对象指针
	AsnTag body_tag;				// 体TAG
	AsnTag block_tag;				// 块TAG
	AsnTag record_tag;				// 记录TAG
	AsnLen body_len;				// 体长度
	AsnLen body_tag_len;			// 体TAG长度
	AsnLen body_len_len;			// 体长度的长度
	size_t body_start;				// 体开始位置
	AsnLen block_len;				// 块长度
	size_t block_start;				// 块开始位置
	AsnLen record_len;				// 记录长度
	AsnLen buffer_len;				// 缓存长度
	bool has_file_tag;				// 是否有文件TAG
};

}
}

#endif

