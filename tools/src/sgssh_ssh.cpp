#include "sgssh_ssh.h"
namespace ai
{
namespace sgssh_ssh
{
ssh::ssh(const string &ip_addr , const string &user_name , const string &password ,const string &cmd)
	:_ip_addr(ip_addr) , _user_name(user_name) , _password(password) , _cmd(cmd) , bytecount(0)
{
	
}
ssh::~ssh()
{
//	reset();
}
int ssh::init()
{
	int rc;

	get_home_path(_home_path);
	get_kownhostfile(_home_path , _full_kownhost);
	//the trust mode need load public key file and private key file
	if(_password.empty()){
		get_publickey_file(_home_path , _full_dpublicfile ,_full_rpublicfile);
		get_privatekey_file(_home_path , _full_dprivatefile,_full_rprivatefile);
	}

	rc = libssh2_init (0);
	if (rc != 0) {
		cout << "libssh2 initialization failed (" << rc << ")" << std::endl;
		return 1;
	}

	return 0;
}

 
int ssh::get_home_path(string &homepath)
{
	char * home_path;
	home_path = getenv("HOME");
	if(home_path[0]=='\0')
	{
		return -1;
	}
	homepath = string(home_path);
	return 0;
}

void ssh::get_kownhostfile(const string &homepath, string &kownhost_file)
{
	string host;
	host ="/.ssh/kownhost";
	kownhost_file = homepath + host;
}

void ssh::get_publickey_file(const string &homepath, string &dpublickey_file, string &rpublickey_file)
{
	string dfile;
	string rfile;
	dfile ="/.ssh/id_dsa.pub";
	rfile ="/.ssh/id_rsa.pub";
	dpublickey_file = homepath + dfile;
	rpublickey_file = homepath + rfile;
}

void ssh::get_privatekey_file(const string &homepath, string &dprivatekey_file, string &rprivatekey_file)
{
	string dfile;
	string rfile;
	dfile ="/.ssh/id_dsa";
	rfile ="/.ssh/id_rsa";
	dprivatekey_file = homepath + dfile;
	rprivatekey_file = homepath + rfile;
}

int ssh::connect(const string &ip_addr)
{
	unsigned long hostaddr;
	struct sockaddr_in sin;

	hostaddr=inet_addr(ip_addr.c_str());

	_sock_fd = socket(AF_INET, SOCK_STREAM, 0);

	sin.sin_family = AF_INET;
	sin.sin_port = htons(22);
	sin.sin_addr.s_addr = hostaddr;
    
	for(int i=0;i<3;i++){
		if (::connect(_sock_fd, (struct sockaddr*)(&sin),
			sizeof(struct sockaddr_in)) != 0) {
			cout << "failed to connect! time is " << i << std::endl;
			i++;
			if(i==2)
				 return -1;

		}
		else
			break;
	}
	return 0;
}

int ssh::login(const string & user_name, const string & password)
{
	const char *fingerprint;
	int rc;
	int type;
	size_t len;

	/* Create a session instance */
	session = libssh2_session_init();
	if (!session){
		cout << "session init error!" <<std::endl;
		return -1;
	}

	/* tell libssh2 we want it all done non-blocking */
	libssh2_session_set_blocking(session, 0);

	/* ... start it up. This will trade welcome banners, exchange keys,
	* and setup crypto, compression, and MAC layers
	*/
	while ((rc = libssh2_session_handshake(session, _sock_fd)) ==
		LIBSSH2_ERROR_EAGAIN);
	if (rc) {
		cout << "Failure establishing SSH session: "<< rc <<std::endl;
		return -1;
	}

	nh = libssh2_knownhost_init(session);
	if(!nh) {
		/* eeek, do cleanup here */
		return 2;
	}

	/* read all hosts from here */
	libssh2_knownhost_readfile(nh, _full_kownhost.c_str(),
				LIBSSH2_KNOWNHOST_FILE_OPENSSH);

	/* store all known hosts to here */
/*	libssh2_knownhost_writefile(nh, "dumpfile",
			LIBSSH2_KNOWNHOST_FILE_OPENSSH);*/

	fingerprint = libssh2_session_hostkey(session, &len, &type);
	if(fingerprint) {
		struct libssh2_knownhost *host;
#if LIBSSH2_VERSION_NUM >= 0x010206
		/* introduced in 1.2.6 */
		int check = libssh2_knownhost_checkp(nh, _ip_addr.c_str(), 22,
						fingerprint, len,
						LIBSSH2_KNOWNHOST_TYPE_PLAIN|
						LIBSSH2_KNOWNHOST_KEYENC_RAW,
						&host);
#else
		/* 1.2.5 or older */
		int check = libssh2_knownhost_check(nh, _ip_addr.c_str(),
						fingerprint, len,
						LIBSSH2_KNOWNHOST_TYPE_PLAIN|
						LIBSSH2_KNOWNHOST_KEYENC_RAW,
						&host);
#endif
		check = check;
		/*cout << "Host check: " << check << ", key: " << 
			(check <= LIBSSH2_KNOWNHOST_CHECK_MISMATCH ) ? host->key : "<none>";*/
		/*cout << "Host check: " << check << ", key: " <<  host->key;
		cout << std::endl;*/
		/*****
		* At this point, we could verify that 'check' tells us the key is
		* fine or bail out.
		*****/
	}
	else {
		/* eeek, do cleanup here */
		return 3;
	}
	
	libssh2_knownhost_free(nh);

	if ( password.size() != 0 ) {
		/* We could authenticate via password */
		while ((rc = libssh2_userauth_password(session, user_name.c_str(), password.c_str())) ==
		LIBSSH2_ERROR_EAGAIN);
		if (rc) {
			cout << "Authentication by password failed. " << std::endl;
			quit();
			return -1;
		}
	}
	else {
	/* Or by public key */
	while ((rc = libssh2_userauth_publickey_fromfile(session, user_name.c_str(), 
							_full_dpublicfile.c_str(),
							_full_dprivatefile.c_str(),
							password.c_str())) ==
							LIBSSH2_ERROR_EAGAIN);
		if (rc) {
			//cout << "\tAuthentication by public key failed " << std::endl;
			while ((rc = libssh2_userauth_publickey_fromfile(session, user_name.c_str(), 
							_full_rpublicfile.c_str(),
							_full_rprivatefile.c_str(),
							password.c_str())) ==
							LIBSSH2_ERROR_EAGAIN);
			if (rc) {
				cout << "\tAuthentication by public key failed " << std::endl;
				quit();
				return -1;
			}
		}
	}


#if 0
    libssh2_trace(session, ~0 );
#endif
	return 0;
}

int ssh::exec_cmd(const string &command)
{
	int rc;

	/* Exec non-blocking on the remove host */
	while( (channel = libssh2_channel_open_session(session)) == NULL &&
				libssh2_session_last_error(session,NULL,NULL,0) ==
				LIBSSH2_ERROR_EAGAIN )
	{
		waitsocket(_sock_fd, session);
	}

	if( channel == NULL )
	{
		cout << "Error" << std::endl;
		exit( 1 );
	}

	while( (rc = libssh2_channel_exec(channel, command.c_str())) ==
			LIBSSH2_ERROR_EAGAIN )
	{
		waitsocket(_sock_fd, session);
	}

	if( rc != 0 )
	{
		cout << "Error" << std::endl;
		exit( 1 );
	}

	for( ;; )
	{
		/* loop until we block */
		int rc;
		do
		{
			char buffer[0x4000];
			rc = libssh2_channel_read( channel, buffer, sizeof(buffer) );
			if( rc > 0 )
			{
			/*	int i;
				bytecount += rc;*/
			/*	cout <<  "We read: " << std::endl;*/
				cout << "[" << _ip_addr << "]====start====== " << std::endl;
				/*for( i=0; i < rc; ++i )
					fputc( buffer[i], stderr);*/
				cout << buffer;
				cout << "[" << _ip_addr << "]====end======== " << std::endl;
			}
			/*else {
				if( rc != LIBSSH2_ERROR_EAGAIN )
			no need to output this for the EAGAIN case 
					cout << "libssh2_channel_read returned " << rc << std::endl;
			}*/
			
		}
		while( rc > 0 );

		do
		{
			char buffer2[0x4000];
			rc = libssh2_channel_read_stderr( channel, buffer2, sizeof(buffer2) );
			if( rc > 0 )
			{
			/*	int i;
				bytecount += rc;*/
				/*cout <<  "We read: " << std::endl;*/
				cout << "[" << _ip_addr << "]====start====== " << std::endl;
				/*for( i=0; i < rc; ++i )
					fputc( buffer[i], stderr);*/
				cout << buffer2;
				cout << "[" << _ip_addr << "]====end======== " << std::endl;
			}
			/*else {
				if( rc != LIBSSH2_ERROR_EAGAIN )
				no need to output this for the EAGAIN case 
					cout << "libssh2_channel_read_stderr returned " << rc << std::endl;
			}*/
			
		}
		while( rc > 0 );

		/* this is due to blocking that would occur otherwise so we loop on
		this condition */
		if( rc == LIBSSH2_ERROR_EAGAIN )
		{
			waitsocket(_sock_fd, session);
		}
		else
			break;
	}

	quit_channel();
	
	return 0;
}

int ssh::quit_channel()
{
	int rc;

	exitcode = 127;
	while( (rc = libssh2_channel_close(channel)) == LIBSSH2_ERROR_EAGAIN )
	waitsocket(_sock_fd, session);

	if( rc == 0 )
	{
		exitcode = libssh2_channel_get_exit_status( channel );
		libssh2_channel_get_exit_signal(channel, &exitsignal,
					NULL, NULL, NULL, NULL, NULL);
	}

	if (exitsignal)
	{
		printf("\nGot signal: %s\n", exitsignal);      }
/*	else 
		printf("\nEXIT: %d bytecount: %d\n", exitcode, bytecount);*/

	libssh2_channel_free(channel);
	channel = NULL;
	return 0;
}

int ssh::quit()
{

	libssh2_session_disconnect(session,
                               "Normal Shutdown, Thank you for playing");
	libssh2_session_free(session);

	close(_sock_fd);

	//cout << "all done" << std::endl;

	libssh2_exit();
	
	return 0;

}

int ssh::waitsocket(int socket_fd, LIBSSH2_SESSION *session)
{
	struct timeval timeout;
	int rc;
	fd_set fd;
	fd_set *writefd = NULL;
	fd_set *readfd = NULL;
	int dir;

	timeout.tv_sec = 10;
	timeout.tv_usec = 0;

	FD_ZERO(&fd);

	FD_SET(socket_fd, &fd);

	/* now make sure we wait in the correct direction */
	dir = libssh2_session_block_directions(session);

	if(dir & LIBSSH2_SESSION_BLOCK_INBOUND)
		readfd = &fd;

	if(dir & LIBSSH2_SESSION_BLOCK_OUTBOUND)
		writefd = &fd;

	rc = select(socket_fd + 1, readfd, writefd, NULL, &timeout);
		return rc;
}


int ssh::exec(const string & ip_addr ,const string & user_name,const string & password,const string & cmd)
{
	exitsignal=(char *)"none";
	int ret;

	_ip_addr = ip_addr;
	_user_name = user_name; 
	_password = password;
	_cmd= cmd;

	init();

	ret = connect(_ip_addr);

	if(ret !=0 )
		return -1;


	ret = login(_user_name,_password);

	if(ret !=0 )
		return -1;


	ret = exec_cmd(_cmd);

	if(ret !=0 )
		return -1;

	
	ret = quit();

	if(ret !=0 )
		return -1;


	return 0;
}

}
}
