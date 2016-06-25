#if !defined(__SAT_CTX_H__)
#define __SAT_CTX_H__

#include "sa_struct.h"
#include "sa_manager.h"

namespace ai
{
namespace app
{

using namespace ai::sg;
using namespace ai::scci;

class sat_ctx
{
public:
	static sat_ctx * instance();
	~sat_ctx();

	// initialize file related information
	void clear();
	// validate all SAs' consistency.
	bool validate();
	// handler when break point reaches
	void on_save(bool do_report);
	// handler when fatal error reaches
	void on_fatal();
	// handler when end of file reaches
	void on_eof();
	// handler when exception on complete
	void on_complete();
	// handler when recovery from exception
	void on_recover();
	void db_rollback();

	int64_t _SAT_user_id;
	std::string _SAT_hostname;
	std::string _SAT_raw_file_name;
	pid_t _SAT_last_pid;
	int _SAT_file_sn;
	time_t _SAT_login_time;
	time_t _SAT_finish_time;
	time_t _SAT_file_time;
	file_log_t _SAT_file_log;
	std::vector<dblog_item_t> _SAT_dblog;
	char **_SAT_global;		// 全局变量数组
	record_type_enum _SAT_record_type;
	std::string _SAT_rstr;
	std::vector<std::string> _SAT_rstrs;
	int _SAT_error_pos;
	bool _SAT_enable_err_info;	// 启用额外信息输出
	std::string _SAT_err_info;	// 输出时的额外信息

	Generic_Database *_SAT_db;
	std::vector<sa_base *> _SAT_sas;

	sa_manager *_SAT_mgr_array[SA_TOTAL_MANAGER];

	// 业务处理规则中使用
	string _SAT_search_str;

private:
	sat_ctx();

	void init_global();
	void report_info();

	static void on_exit(int signo);

	static boost::thread_specific_ptr<sat_ctx> instance_;
};

}
}

#endif

