#if !defined(__SGSSH_FILE_H__)
#define __SGSSH_FILE_H__

#include "sgssh.h"
namespace ai
{
namespace sgssh_load_file
{

using namespace std;

class load_file
{
public:
	load_file(const string &file_name);
	~load_file();
	void loading(const string &file_name);
	set<string> ipaddr_set;
private:
	ifstream ifs;
	string _file_name;
};

}
}
#endif
