#include "sa_internal.h"

namespace ai
{
namespace app
{

namespace bf = boost::filesystem;
namespace bi = boost::interprocess;
namespace bp = boost::posix_time;
using namespace ai::sg;
using namespace ai::scci;

sa_isocket::sa_isocket(sa_base& _sa, int _flags)
	: sa_input(_sa, _flags),
	  acceptor(io_svc),
	  timer(io_svc)
{
	const map<string, string>& args = adaptor.source.args;
	map<string, string>::const_iterator iter;

	iter = args.find("hostname");
	if (iter == args.end())
		throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: source.args.arg.hostname missing.")).str(APPLOCALE));
	else
		hostname = iter->second;

	iter = args.find("port");
	if (iter == args.end())
		throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: source.args.arg.port missing.")).str(APPLOCALE));
	else
		port = iter->second;

	iter = args.find("tcpkeepalive");
	if (iter == args.end())
		tcpkeepalive = true;
	else if (strcasecmp(iter->second.c_str(), "Y") == 0)
		tcpkeepalive = true;
	else if (strcasecmp(iter->second.c_str(), "N") == 0)
		tcpkeepalive = false;
	else
		throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: source.args.arg.tcpkeepalive value invalid.")).str(APPLOCALE));

	iter = args.find("nodelay");
	if (iter == args.end())
		nodelay = false;
	else if (strcasecmp(iter->second.c_str(), "Y") == 0)
		nodelay = true;
	else if (strcasecmp(iter->second.c_str(), "N") == 0)
		nodelay = false;
	else
		throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: source.args.arg.nodelay value invalid.")).str(APPLOCALE));

	iter = args.find("max_connections");
	if (iter == args.end()) {
		max_connections = std::numeric_limits<int>::max();
	} else {
		try {
			max_connections = boost::lexical_cast<int>(iter->second);
			if (max_connections <= 0)
				max_connections = std::numeric_limits<int>::max();
		} catch (exception& ex) {
			throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: source.args.arg.max_connections value invalid.")).str(APPLOCALE));
		}
	}

	ready = false;
	socket_ = NULL;
}

sa_isocket::~sa_isocket()
{
}

void sa_isocket::init()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(600, __PRETTY_FUNCTION__, "", NULL);
#endif
	compiler& cmpl = SAP->_SAP_cmpl;
	const field_map_t& global_map = adaptor.global_map;

	const vector<input_record_t>& input_records = adaptor.input_records;
	for (int i = 0; i < input_records.size(); i++) {
		const input_record_t& input_record = input_records[i];
		try {
			record_serial_idx.push_back(cmpl.create_function(input_record.rule_condition, global_map, field_map_t(), field_map_t()));
		} catch (exception& ex) {
			throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Can't create function for rule_condition, {1}") % ex.what()).str(APPLOCALE));
		}
	}
}

void sa_isocket::init2()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(600, __PRETTY_FUNCTION__, "", NULL);
#endif
	compiler& cmpl = SAP->_SAP_cmpl;

	BOOST_FOREACH(const int& idx, record_serial_idx) {
		record_serial_func.push_back(cmpl.get_function(idx));
	}

	basio::ip::tcp::resolver resolver(io_svc);
	basio::ip::tcp::resolver::query query(hostname, port);
	boost::asio::ip::tcp::endpoint endpoint = *resolver.resolve(query);
	acceptor.open(endpoint.protocol());
	acceptor.set_option(basio::socket_base::reuse_address(true));
	acceptor.bind(endpoint);
	acceptor.listen();
	start_accept();
}

bool sa_isocket::open()
{
	bool retval = false;
#if defined(DEBUG)
	scoped_debug<bool> debug(600, __PRETTY_FUNCTION__, "", &retval);
#endif

	if (SGP->_SGP_shutdown)
		return retval;

	SAT->_SAT_raw_file_name = "";
	SAT->_SAT_record_type = RECORD_TYPE_BODY;
	SAT->clear();

	// insert log to database.
	dblog_manager& dblog_mgr = dblog_manager::instance(SAT);
	dblog_mgr.insert();

	SAT->_SAT_global[GLOBAL_SVC_KEY_SERIAL][0] = '\0';
	memcpy(SAT->_SAT_global[GLOBAL_FILE_NAME_SERIAL], SAT->_SAT_raw_file_name.c_str(), SAT->_SAT_raw_file_name.length() + 1);
	sprintf(SAT->_SAT_global[GLOBAL_FILE_SN_SERIAL], "%d", SAT->_SAT_file_sn);
	strcpy(SAT->_SAT_global[GLOBAL_REDO_MARK_SERIAL], "0000");

	datetime dt(SAT->_SAT_login_time);
	string dts;
	dt.iso_string(dts);
	memcpy(SAT->_SAT_global[GLOBAL_SYSDATE_SERIAL], dts.c_str(), dts.length() + 1);
	strcpy(SAT->_SAT_global[GLOBAL_FIRST_SERIAL], "0");
	strcpy(SAT->_SAT_global[GLOBAL_SECOND_SERIAL], "0");

	retval = true;
	return retval;
}

void sa_isocket::flush(bool completed)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, (_("completed={1}") % completed).str(APPLOCALE), NULL);
#endif

	master.error_type = (master.record_error == 0 ? ERROR_TYPE_SUCCESS : ERROR_TYPE_PARTIAL);
	master.completed = (completed ? 2 : 0);
}

void sa_isocket::close()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	master.completed = 1;
}

void sa_isocket::clean()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	master.error_type = ERROR_TYPE_FATAL;
	master.completed = 1;
}

void sa_isocket::recover()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(600, __PRETTY_FUNCTION__, "", NULL);
#endif
}

void sa_isocket::set_socket(basio::ip::tcp::socket& _socket)
{
	socket_ = &_socket;
	ready = true;
}

basio::ip::tcp::socket& sa_isocket::socket()
{
	return *socket_;
}

int sa_isocket::read()
{
	int retval = SA_SUCCESS;
#if defined(DEBUG)
	scoped_debug<int> debug(600, __PRETTY_FUNCTION__, "", &retval);
#endif

	timer.expires_from_now(bp::seconds(1));
	timer.async_wait(boost::bind(&sa_isocket::handle_timeout, this));
	BOOST_SCOPE_EXIT((&timer)) {
		timer.cancel();
	} BOOST_SCOPE_EXIT_END

	do {
		io_svc.run_one();
		if (SGP->_SGP_shutdown || io_svc.stopped()) {
			retval = SA_EOF;
			return retval;
		}
	} while (!ready);

	SAT->_SAT_rstrs.push_back(SAT->_SAT_rstr);
	dout << "record = [" << SAT->_SAT_rstr << "] length = " << SAT->_SAT_rstr.length() << std::endl;

	do_record();

	ready = false;
	return retval;
}

void sa_isocket::start_accept()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(600, __PRETTY_FUNCTION__, "", NULL);
#endif
	sa_iconnection::pointer new_connection = sa_iconnection::create(acceptor.get_io_service(), max_connections, this);

	acceptor.async_accept(new_connection->socket(),
		boost::bind(boost::mem_fn(&sa_isocket::handle_accept), this, new_connection,
		basio::placeholders::error));
}

void sa_isocket::handle_accept(sa_iconnection::pointer new_connection, const boost::system::error_code& error)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(10, __PRETTY_FUNCTION__, (_("error={1}") % error).str(APPLOCALE), NULL);
#endif

	if (!error)
		new_connection->start(tcpkeepalive, nodelay);

	start_accept();
}

void sa_isocket::handle_timeout()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(100, __PRETTY_FUNCTION__, "", NULL);
#endif

	timer.expires_from_now(bp::seconds(1));
	timer.async_wait(boost::bind(&sa_isocket::handle_timeout, this));
}

int sa_isocket::do_record()
{
	int retval = SA_SUCCESS;
#if defined(DEBUG)
	scoped_debug<int> debug(600, __PRETTY_FUNCTION__, "", &retval);
#endif

	int record_serial = get_record_serial(SAT->_SAT_rstr.c_str());
	if (record_serial < 0) {
		if (SAT->_SAT_record_type == RECORD_TYPE_BODY)
			retval = SA_INVALID;
		else
			retval = SA_SKIP;
		return retval;
	}

	const vector<input_record_t>& input_records = adaptor.input_records;
	const input_record_t& input_record = input_records[record_serial];

	if (!to_fields(input_record)) {
		retval = SA_ERROR;
		return retval;
	}

	return retval;
}

int sa_isocket::get_record_serial(const char *record) const
{
	int& input_serial = sa.get_input_serial();
	input_serial = -1;
#if defined(DEBUG)
	scoped_debug<int> debug(600, __PRETTY_FUNCTION__, (_("record={1}") % record).str(APPLOCALE), &input_serial);
#endif
	const vector<input_record_t>& input_records = adaptor.input_records;
	char **global = sa.get_global();

	for (int i = 0; i < record_serial_idx.size(); i++) {
		if (input_records[i].record_type != SAT->_SAT_record_type)
			continue;

		if ((*record_serial_func[i])(NULL, global, &record, 0) == 0) {
			input_serial = i;
			return input_serial;
		}
	}

	return input_serial;
}

}
}

