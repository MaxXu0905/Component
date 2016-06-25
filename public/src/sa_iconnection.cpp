#include "sa_internal.h"
#include "sa_iconnection.h"

namespace ai
{
namespace app
{

namespace basio = boost::asio;
namespace bi = boost::interprocess;
using namespace ai::sg;
using namespace ai::scci;

int sa_iconnection::connections = 0;

sa_iconnection::pointer sa_iconnection::create(basio::io_service& io_svc, int max_connections_, sa_isocket *isrc_mgr_)
{
	return pointer(new sa_iconnection(io_svc, max_connections_, isrc_mgr_));
}

basio::ip::tcp::socket& sa_iconnection::socket()
{
	return socket_;
}

void sa_iconnection::start(bool tcpkeepalive, bool nodelay)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(50, __PRETTY_FUNCTION__, (_("tcpkeepalive={1}, nodelay={2}") % tcpkeepalive % nodelay).str(APPLOCALE), NULL);
#endif

	if (connections == max_connections)
		return;

	++connections;

	basio::socket_base::keep_alive option1(tcpkeepalive);
	socket_.set_option(option1);

	basio::ip::tcp::no_delay option2(nodelay);
    socket_.set_option(option2);

	basio::async_read_until(socket_, input_buffer, '\n',
		boost::bind(&sa_iconnection::handle_read, shared_from_this(),
			basio::placeholders::error,
			boost::asio::placeholders::bytes_transferred));
}

void sa_iconnection::close()
{
	basio::io_service& io_svc = socket_.get_io_service();
	io_svc.post(boost::bind(&sa_iconnection::do_close, shared_from_this()));
}

sa_iconnection::sa_iconnection(basio::io_service& io_svc, int max_connections_, sa_isocket *isrc_mgr_)
	: max_connections(max_connections_),
	  socket_(io_svc),
	  isrc_mgr(isrc_mgr_)
{
}

sa_iconnection::~sa_iconnection()
{
}

void sa_iconnection::handle_read(const boost::system::error_code& error, size_t length)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(50, __PRETTY_FUNCTION__, "", NULL);
#endif

	if (!error) {
		SAT->_SAT_rstr.resize(length - 1);
		input_buffer.sgetn(const_cast<char *>(SAT->_SAT_rstr.c_str()), length - 1);
		isrc_mgr->set_socket(socket_);

		basio::async_read_until(socket_, input_buffer, '\n',
			boost::bind(&sa_iconnection::handle_read, shared_from_this(),
				basio::placeholders::error,
				boost::asio::placeholders::bytes_transferred));
	} else {
		do_close();
	}
}

void sa_iconnection::do_close()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(50, __PRETTY_FUNCTION__, "", NULL);
#endif

	boost::system::error_code ignore_error;
	socket_.close(ignore_error);
	connections--;
}

}
}

