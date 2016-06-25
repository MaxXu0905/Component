#include <string>
#include <iostream>
#include <unistd.h>
#include <vector>
#include <libgen.h>

#include "sgssh_ssh.h"
#include "sgssh_file.h"
#include "sgssh_sgip.h"
#include <boost/thread.hpp>

using namespace std;
using namespace ai::sgssh_ssh;
using namespace ai::sgssh_load_file;
using namespace ai::sgssh_sgip;

int main(int argc,char ** argv)
{
	string remote_ip;
	string user_name;
	string password;
	string exec_command;
	string file_name;
	string read_sgconfig;
	int mode=0;
	ssh *ssh_exec;
	boost::thread_group thread_group;
	load_file *loadfile;
	sgip *sgaddr;
	string login_name;

	try
	{
	// Analyse command args.
	int ch;
	while ((ch = getopt(argc, argv, ":i:u:p:c:s:f:r")) != -1)
	{
		switch (ch)
		{
		case 'i':
			remote_ip = optarg;
			break;
		case 'u':
			user_name = optarg;
			break;
		case 'p':
			password = optarg;
			break;
		case 'c':
			exec_command = optarg;
			break;
		case 's':
			read_sgconfig = optarg;
			break;
		case 'f':
		file_name = optarg;
		break;
	case ':':
		cout << "Option -" << static_cast<char>(optopt) << " requires an argument." << std::endl;
		exit(1);
	case '?':
		cout << "Usage : " << argv[0] << " -i remote ip ..." << std::endl;
		cout << "Usage : " << argv[0] << " -u user name ..." << std::endl;
		cout << "Usage : " << argv[0] << " -p password ..." << std::endl;
		cout << "Usage : " << argv[0] << " -c command" << std::endl;
		cout << "example : " << argv[0] << " -i XX.XX.XX.XX -u user_name -p password -c \"ls\" " << std::endl;
		cout << "Usage : " << argv[0] << " -s read ip address from SGCONFIG file ..." << std::endl;
		cout << "example : " << argv[0] << " -s SGCONFIG -c \"ls\" " << std::endl;
		cout << "Usage : " << argv[0] << " -f the spacial file's name(full path) ..." << std::endl;
		cout << "example : " << argv[0] << " -f file_name -c \"ls\" " << std::endl;
		exit(1);
		}
	}
	login_name = getlogin();

	// Check arg list.
	if (remote_ip.empty() && file_name.empty() && read_sgconfig.empty())
	{
		cout << "Option -i. or -s. or -f. must be provided" << std::endl;
		exit(1);
	}
	
	if (!remote_ip.empty())
	{  
		cout << "when Option -i mode -i must be provided" << std::endl;
		cout << "example : " << argv[0] << " -i XX.XX.XX.XX -u user_name -p password -c \"ls\" " << std::endl;
		if(user_name.empty())
		{
			user_name = login_name;
		}
		else if(password.empty())
		{
			password = "";
		}
		else if(exec_command.empty())
		{
			cout << "when Option -i mode -c must be provided" << std::endl;
			cout << "example : " << argv[0] << " -i XX.XX.XX.XX -u user_name -p password -c \"ls\" " << std::endl;
			exit(1);
		}
	}
	else if(!read_sgconfig.empty())
	{
		mode = 1;
		user_name = login_name;
		password ="";
		if(exec_command.empty())
		{
			cout << "when Option -s mode -c must be provided" << std::endl;
			cout << "example : " << argv[0] << " -s SGCONFIG -c \"ls\" " << std::endl;
			exit(1);
		}
	}
	else if(!file_name.empty())
	{
		mode = 2;
		user_name = login_name;
		password ="";
		if(exec_command.empty())
		{
			cout << "when Option -f mode -c must be provided" << std::endl;
			cout << "example : " << argv[0] << " -f file_name -c \"ls\" " << std::endl;
			exit(1);
		}
	}

//	cout << "exec_command is " << exec_command  << std::endl;
	// Set application's path to the current path.
	chdir(dirname(argv[0]));
	
	// Create application.
	if(mode == 0)
	{
		ssh_exec  = new ssh(remote_ip,user_name,password,exec_command);

		ssh_exec -> exec(remote_ip,user_name,password,exec_command);
		delete ssh_exec;
	}
	else if(mode ==1)
	{
		sgaddr = new sgip();
		
		sgaddr -> load_sgconf();
		
		set<string>::const_iterator iter = sgaddr->sgip_addr.begin();
		for ( set<string>::const_iterator iter = sgaddr->sgip_addr.begin(); iter !=sgaddr->sgip_addr.end(); ++iter ) {
			remote_ip= *iter;
			
			ssh_exec = new ssh(remote_ip,user_name,password,exec_command);
			thread_group.create_thread(boost::bind(&ssh::exec,ssh_exec,remote_ip,user_name,password,exec_command));
			
			thread_group.join_all();
			
			delete ssh_exec;
		}
		delete sgaddr;
	}
	else if(mode ==2)
	{
		loadfile = new load_file(file_name);
		loadfile -> loading(file_name);
		
		for ( set<string>::const_iterator iter = loadfile->ipaddr_set.begin(); iter !=loadfile->ipaddr_set.end(); ++iter ) {
			remote_ip= *iter;
			
			ssh_exec = new ssh(remote_ip,user_name,password,exec_command);
			
			thread_group.create_thread(boost::bind(&ssh::exec,ssh_exec,remote_ip,user_name,password,exec_command));
			
			thread_group.join_all();
			delete ssh_exec;
		}
		delete loadfile;
	}

	}
	catch (exception& ex)
	{
		cout << "In file(" << __FILE__
			<< ") at line(" << __LINE__
			<< ") " << ex.what() << '\n';
		return 1;
	}

	return 0;
}

