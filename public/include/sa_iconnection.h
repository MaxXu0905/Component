#if !defined(__SA_ICONNECTION_H__)
#define __SA_ICONNECTION_H__

#include <ctime>
#include <iostream>
#include <string>
#include "sg_public.h"

namespace ai
{
namespace app
{

namespace basio = boost::asio;

class sa_isocket;

class sa_iconnection : public boost::enable_shared_from_this<sa_iconnection>, public sa_manager
{
public:
	typedef boost::shared_ptr<sa_iconnection> pointer;

	static pointer create(basio::io_service& io_svc, int max_connections_, sa_isocket *isrc_mgr_);

	basio::ip::tcp::socket& socket();
	void start(bool tcpkeepalive, bool nodelay);
	void close();
	~sa_iconnection();

private:
	sa_iconnection(basio::io_service& io_svc, int max_connections_, sa_isocket *isrc_mgr_);

	void handle_read(const boost::system::error_code& error, size_t length);
	void do_close();

	int max_connections;
	basio::streambuf input_buffer;
	basio::ip::tcp::socket socket_;
	sa_isocket *isrc_mgr;

	static int connections;
};

}
}

#endif

