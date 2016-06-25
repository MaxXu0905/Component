#if !defined(_SGSSH_SSH_H__)
#define _SGSSH_SSH_H__

#include "sgssh.h"

namespace ai
{
namespace sgssh_ssh
{
using namespace std;

class ssh
{
public: 
	ssh(const string &ip_addr, const string &user_name, const string &password ,const string &cmd);
	~ssh();
	int exec(const string & ip_addr ,const string & user_name,const string & password,const string & cmd);
	void set_ip(const string &ip_addr);
	void set_user_name(const string &user_name);
	void set_password(const string &password);
	int init();
	int connect(const string &ip_addr);
	int login(const string &user_name, const string &password);
	int exec_cmd(const string &cmd);
	int quit_channel();
	int quit();
private:
	int get_home_path(string &home_path);
	void get_kownhostfile(const string &homepath , string &kownhost_file);
	void get_publickey_file(const string &homepath , string &dpublickey_file, string & rpublickey_file);
	void get_privatekey_file(const string &homepath , string &dprivatekey_file, string & rprivatekey_file);
	static int waitsocket(int socket_fd, LIBSSH2_SESSION* session);

	LIBSSH2_SESSION *session;
	LIBSSH2_CHANNEL *channel;
	LIBSSH2_KNOWNHOSTS *nh;

	int _sock_fd;

	string _ip_addr;
	string _user_name;
	string _password;
	string _cmd;
	string _full_kownhost;
	string _full_dpublicfile;
	string _full_rpublicfile;
	string _full_dprivatefile;
	string _full_rprivatefile;
	string _home_path;
	int exitcode;
	char *exitsignal;
	int bytecount;
};


}
}

#endif
