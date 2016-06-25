#include <stdio.h>
#include <string>
#include <iostream>
#include "ssh.h"
#include "file.h"
#include "sgip.h"
#include <boost/thread.hpp>

using namespace std;
using namespace ai::ssh;
using namespace ai::load_file;
using namespace ai::sgip;
int main()
{
	string ip_addr;
	string user_name;
	string password;
	string commdline;
	string ip_addr_1;
	string user_name_1;
	string password_1;
	string commdline_1;
	string commdline_2;
	string file_name;
	int i;
	int thread_number;
	//int thread;
	boost::thread_group thread_group;

	ssh *ssh_exec;
	load_file *loadfile;
	sgip *sgaddr;
	
	ip_addr = "10.1.253.20";
	user_name = "pocrlt01";
	password = "rltpoc01";
	commdline = "ls";
	std::cout << "commdline is " << commdline << std::endl;
	ip_addr_1 = "10.1.253.20";
	user_name_1 = "pocrlt01";
	password_1 = "rltpoc01";
	commdline_1 = "ls -l ";
	commdline_2 = "ls";
	file_name ="/unibss/devusers/likan/lktest/data/ipaddr.txt";
	thread_number = 10;
	std::cout << "commdline1 is " << commdline_1 << std::endl;
	std::cout << "commdline2 is " << commdline_2 << std::endl;
/*
	ssh_exec = new ssh(ip_addr,user_name,password,commdline);
	
	ssh_exec->exec(ip_addr,user_name,password,commdline);

	ssh_exec->init();
	ssh_exec->connect(ip_addr_1);
	ssh_exec->login(user_name_1,password_1);

	std::cout << "commdline1 ==============================================" << commdline_1 << std::endl;

	ssh_exec->exec_cmd(commdline_1);


	std::cout << "commdline2 ==============================================" << commdline_2 << std::endl;
	ssh_exec->exec_cmd(commdline_2);


	ssh_exec->quit_channel();

	ssh_exec->quit();
*/
/*
	loadfile = new load_file(file_name);
	loadfile -> loading(file_name);

	set<string>::const_iterator iter = loadfile->ipaddr_set.begin();
	for (i = 0; iter !=loadfile->ipaddr_set.end(); ++iter, i++) {
		cout << "ipaddr is " << *iter << std::endl;
		ip_addr = *iter;
		
		ssh_exec = new ssh(ip_addr,user_name,password,commdline);

		//ssh_exec->exec(ip_addr,user_name,password,commdline);
	//	cmd = commdline;
		
		thread_group.create_thread(boost::bind(&ssh::exec,ssh_exec,ip_addr,user_name,password,commdline));

		thread_group.join_all();
		delete ssh_exec;
	}
*/


	sgaddr = new sgip();

	sgaddr -> load_sgconf();

        set<string>::const_iterator iter = sgaddr->sgip_addr.begin();
        for ( set<string>::const_iterator iter = sgaddr->sgip_addr.begin(); iter !=sgaddr->sgip_addr.end(); ++iter ) {
                cout << "ipaddr is " << *iter << std::endl;
                ip_addr = *iter;

                ssh_exec = new ssh(ip_addr,user_name,password,commdline);
		thread_group.create_thread(boost::bind(&ssh::exec,ssh_exec,ip_addr,user_name,password,commdline));

		thread_group.join_all();

                delete ssh_exec;

        }
	delete sgaddr;

	return 0;

}

