#if !defined(__SA_OSOURCE_H__)
#define __SA_OSOURCE_H__

#include "sa_output.h"

namespace ai
{
namespace app
{

using namespace std;
using namespace ai::sg;

/* 说明
 * 该类负责把输入记录输出到特定文件中
 */
class sa_osource : public sa_output
{
public:
	sa_osource(sa_base& _sa, int _flags);
	~sa_osource();

	void open();
	int write(int input_idx);
	int write(std::string& input_str, std::string& err_str);
	void flush(bool completed);
	void close();
	void complete();
	void clean();
	void recover();
	void dump(ostream& os);
	void rollback(const string& raw_file_name, int file_sn);
	void global_rollback();

private:
	size_t *break_point;
	bool varied;
	char delimiter;
	std::string dir_name;
	std::string rdir_name;
	std::string tmp_name;
	std::string full_name;
	std::ofstream ofs;
	std::string sftp_prefix;
	std::string pattern;
};

}
}

#endif

