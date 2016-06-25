#if !defined(__SA_ISOCKET_H__)
#define __SA_ISOCKET_H__

#include "sa_struct.h"
#include "sa_input.h"
#include "sa_iconnection.h"

namespace ai
{
namespace app
{

using namespace ai::sg;
using namespace ai::scci;
namespace bi = boost::interprocess;

class sa_isocket : public sa_input
{
public:
	sa_isocket(sa_base& _sa, int _flags);
	~sa_isocket();

	void init();
	void init2();
	bool open();
	void flush(bool completed);
	void close();
	void clean();
	void recover();
	int read();

	void set_socket(basio::ip::tcp::socket& _socket);
	basio::ip::tcp::socket& socket();

protected:
	void start_accept();
	void handle_accept(sa_iconnection::pointer new_connection, const boost::system::error_code& error);
	void handle_timeout();
	int do_record();
	virtual bool to_fields(const input_record_t& input_record) = 0;
	int get_record_serial(const char *record) const;

	// 参数列表
	std::string hostname;
	std::string port;
	bool tcpkeepalive;
	bool nodelay;
	int max_connections;

	// 成员操作变量
	std::vector<int> record_serial_idx;	// 记录序列号规则ID
	std::vector<compiler::func_type> record_serial_func;	// 记录序列号规则
	basio::io_service io_svc;
	basio::ip::tcp::acceptor acceptor;
	basio::deadline_timer timer;
	bool ready;
	basio::ip::tcp::socket *socket_;
};

}
}

#endif

