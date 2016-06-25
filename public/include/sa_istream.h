#if !defined(__SA_ISTREAM_H__)
#define __SA_ISTREAM_H__

#include "sa_struct.h"
#include "sa_input.h"
#include "scan_file.h"

namespace ai
{
namespace app
{

using namespace ai::sg;
using namespace ai::scci;
namespace bi = boost::interprocess;

class sa_global;

enum bak_flag_enum
{
	BAK_FLAG_REMOVE = 1,	// remove file
	BAK_FLAG_SUFFIX = 2,	// add .bak suffix to file
	BAK_FLAG_RENAME = 3		// rename to another directory
};

enum deal_flag_enum
{
	DEAL_FLAG_PARTIAL = 0,	// no check with breakpoint commit
	DEAL_FLAG_WHOLE = 1,	// no check with whole file commit
	DEAL_FLAG_CHECK = 2		// check with whole file commit.
};

class sa_istream : public sa_input
{
public:
	sa_istream(sa_base& _sa, int _flags);
	~sa_istream();

	void init();
	void init2();
	bool open();
	void flush(bool completed);
	void close();
	void complete();
	void clean();
	void recover();
	bool validate();
	void rollback(const string& raw_file_name, int file_sn);
	void iclean();
	void global_rollback();

protected:
	bool get_file();
	void get_redo_files();
	void rename();

	virtual void post_open();

	// �����б�
	int max_records;					// �������ļ�¼��
	std::string rule_hcheck;				// ��¼ͷ������
	std::string src_dir;					// �����ļ�Ŀ¼
	std::string dup_dir;					// �ظ��ļ�Ŀ¼
	std::string bak_dir;					// �����ļ�Ŀ¼
	std::string tmp_dir;					// ��ʱ�ļ�Ŀ¼
	std::string ftl_dir;					// �쳣�ļ�Ŀ¼
	std::string local_dir;				// ������ʱ�ļ�Ŀ¼
	std::string pattern;					// �����ļ�������ʽ
	bak_flag_enum bak_flag;			// ���ݱ�ʶ
	std::string zip_cmd;				// ѹ������
	deal_flag_enum deal_flag;			// �ļ������ʶ
	std::string rule_check;				// �ļ�������
	int sleep_time;					// û�д����ļ�ʱ������ʱ��
	bool exit_nofile;					// û���ļ�ʱ�˳�
	string sftp_prefix;

	string file_prefix;					// ��ʱ�ļ�ǰ׺
	boost::shared_ptr<istream> is;		// �����ļ���
	scan_file<> *fscan;					// �ļ�ɨ�����
	std::string lmid;					// ��ǰ�����߼���ַ
	std::map<std::string, std::string> redo_files;	// ��Ҫ������Դ�ļ�����ʱ�ļ�ӳ���
	std::vector<std::string> files;
	std::vector<std::string>::const_iterator file_iter;
	string src_name;					// �����ļ���
	string src_full_name;				// �����ļ�ȫ��
	string target_full_name;			// Ŀ���ļ�ȫ��
	string local_full_name;				// �����ļ�ȫ��

	ios::pos_type end_pos;				// �����ļ����λ��
	bool file_correct;					// �ļ�������ȷ��ʶ

	// ������صĶ���
	int hchk_idx;						// �ļ�ͷ������ID
	std::vector<int> record_serial_idx;	// ��¼���кŹ���ID
	int chk_idx;						// �ļ�������ID

	compiler::func_type hchk_func;		// �ļ�ͷ������
	std::vector<compiler::func_type> record_serial_func;	// ��¼���кŹ���
	compiler::func_type chk_func;		// �ļ�������
};

}
}

#endif

