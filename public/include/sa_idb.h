#if !defined(__SA_IDB_H__)
#define __SA_IDB_H__

#include "sa_struct.h"
#include "sa_input.h"
#include "sql_control.h"

namespace ai
{
namespace app
{

using namespace ai::sg;
using namespace ai::scci;
namespace bi = boost::interprocess;

int const OPTIONS_PER_ROWS = 0x1;
int const OPTIONS_PER_LOOP = 0x2;

class sa_idb : public sa_input
{
public:
	sa_idb(sa_base& _sa, int _flags);
	~sa_idb();

	void init();
	void init2();
	bool open();
	void flush(bool completed);
	void close();
	void clean();
	void recover();
	int read();

protected:
	struct sa_sql_struct_t
	{
		std::string sql_str;
		std::vector<int> binds;
		Generic_Statement *stmt;
		struct_dynamic *data;

		sa_sql_struct_t() {
			stmt = NULL;
			data = NULL;
		}
	};

	bool to_fields(const input_record_t& input_record, Generic_ResultSet *rset);
	bool set_sqls(std::vector<sa_sql_struct_t>& sqls, const std::string& value);
	bool set_binds(sa_sql_struct_t& sql);

	// �����б�
	int options;							// ѡ��
	time_t align_time;						// ��ʼ��������ʱ���
	int max_rows;							// һ�δ��������¼��
	int per_rows;							// ģ��һ���ļ����������¼��
	std::vector<sa_sql_struct_t> pre_sql;	// ����ǰ��ִ�����
	sa_sql_struct_t loop_sql;				// ������¼��ѭ�����
	std::vector<sa_sql_struct_t> exp_sql;	// �������
	std::vector<sa_sql_struct_t> post_sql;	// ������ִ�����
	int sleep_time;							// ����һ�εļ��ʱ��

	// ��Ա��������
	bool first_time;
	Generic_ResultSet *loop_rset;			// ������¼��ѭ�������
	Generic_ResultSet *exp_rset;			// ���������
	Generic_Statement *stmt;				// ��ǰ�������
	int exp_idx;							// ��ǰ�����������
	datetime sysdate;						// ��ǰʱ��
	int rows;								// ��ǰѭ�������¼��
	bool real_eof;							// �����Ĵ������
};

}
}

#endif

