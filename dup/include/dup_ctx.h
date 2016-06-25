#if !defined(__DUP_CTX_H__)
#define __DUP_CTX_H__

#include "sg_internal.h"
#include <sqlite3.h>
#include "svcp_ctx.h"

using namespace ai::sg;

namespace ai
{
namespace app
{

int const DUP_TABLE_IDX = 0;
int const DUP_ID_IDX = 1;
int const DUP_KEY_IDX = 2;
int const DUP_BEGIN_IDX = 3;
int const DUP_END_IDX = 4;

const char DUP_TABLE_NAME[] = "dup_table";
const char DUP_ID_NAME[] = "dup_id";
const char DUP_KEY_NAME[] = "dup_key";
const char DUP_BEGIN_NAME[] = "begin_time";
const char DUP_END_NAME[] = "end_time";

int const DUP_TABLE_SIZE = 64;
int const DUP_ID_SIZE = 128;
int const DUP_KEY_SIZE = 256;
int const DUP_TIME_SIZE = 20;

struct dup_stmt_t
{
	sqlite3_stmt *insert_stmt;
	sqlite3_stmt *select_stmt;
};

class dup_ctx
{
public:
	static dup_ctx * instance();
	~dup_ctx();

	bool get_config();
	void set_map(field_map_t& data_map) const;
	void set_svcname(int partition_id, std::string& svc_name) const;
	bool open_sqlite();
	void close_sqlite();

	// ҵ��ؼ���
	std::string _DUP_svc_key;
	// ҵ�����ð汾
	int _DUP_version;
	// ���ݿ�����
	std::string _DUP_db_name;
	// �Ƿ����������
	bool _DUP_enable_cross;
	// ÿ̨�������ܷ�����
	int _DUP_partitions;
	// ��������
	int _DUP_mids;
	// �ܷ�����
	int _DUP_all_partitions;
	// �Զ����������ڵı�
	bool _DUP_auto_create;
	// ����ID
	int _DUP_partition_id;
	// ����ҳ��
	long _DUP_cache_size;
	// �Ƿ�ʹ������ģʽ
	bool _DUP_exclusive;
	//�Ƿ�ʹ���ڴ��
	bool _DUP_memorydb;

	// SQLite���ݿ�����
	sqlite3 *_DUP_sqlite3_db;
	// SQLite�������ӳ���
	boost::unordered_map<std::string, dup_stmt_t> _DUP_sqlite3_stmts;

private:
	dup_ctx();

	void load_config(const std::string& config);

	static void init_once();

	static boost::once_flag once_flag;
	static dup_ctx *_instance;
};

}
}

#endif

