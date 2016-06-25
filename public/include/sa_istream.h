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

	// 参数列表
	int max_records;					// 最大允许的记录数
	std::string rule_hcheck;				// 记录头检查规则
	std::string src_dir;					// 输入文件目录
	std::string dup_dir;					// 重复文件目录
	std::string bak_dir;					// 备份文件目录
	std::string tmp_dir;					// 临时文件目录
	std::string ftl_dir;					// 异常文件目录
	std::string local_dir;				// 本地临时文件目录
	std::string pattern;					// 输入文件正则表达式
	bak_flag_enum bak_flag;			// 备份标识
	std::string zip_cmd;				// 压缩命令
	deal_flag_enum deal_flag;			// 文件处理标识
	std::string rule_check;				// 文件检查规则
	int sleep_time;					// 没有处理文件时的休眠时间
	bool exit_nofile;					// 没有文件时退出
	string sftp_prefix;

	string file_prefix;					// 临时文件前缀
	boost::shared_ptr<istream> is;		// 输入文件流
	scan_file<> *fscan;					// 文件扫描对象
	std::string lmid;					// 当前主机逻辑地址
	std::map<std::string, std::string> redo_files;	// 需要重做的源文件与临时文件映射表
	std::vector<std::string> files;
	std::vector<std::string>::const_iterator file_iter;
	string src_name;					// 输入文件名
	string src_full_name;				// 输入文件全名
	string target_full_name;			// 目标文件全名
	string local_full_name;				// 本地文件全名

	ios::pos_type end_pos;				// 输入文件最后位置
	bool file_correct;					// 文件处理正确标识

	// 规则相关的定义
	int hchk_idx;						// 文件头检查规则ID
	std::vector<int> record_serial_idx;	// 记录序列号规则ID
	int chk_idx;						// 文件检查规则ID

	compiler::func_type hchk_func;		// 文件头检查规则
	std::vector<compiler::func_type> record_serial_func;	// 记录序列号规则
	compiler::func_type chk_func;		// 文件检查规则
};

}
}

#endif

