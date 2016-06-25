#include "sgssh_file.h"

namespace ai
{
namespace sgssh_load_file
{

load_file::load_file(const string &file_name)
	:_file_name(file_name)
{
}
load_file::~load_file()
{
}

void load_file::loading(const string &file_name)
{
	//int size,fd;
	char buff[100];
	string line;
	_file_name = file_name;

	ipaddr_set.clear();	
	
	ifs.open(_file_name.c_str());

	if(!ifs)
	{
		cout << "open file " << _file_name << " error " << std::endl;
		exit(1);
	}
	while (1)
	{
		line.clear();
		ifs.getline(buff,sizeof(buff));
		if(ifs.eof())
			break;
		else if(!ifs){
			cout << "read file " << _file_name << " error " << std::endl;
			exit (1);
		}
		
		line = buff;
		cout << "line is " << line << std::endl;
		ipaddr_set.insert(line);
			
	}
	ifs.clear();
	ifs.close();

}

	
}
}
