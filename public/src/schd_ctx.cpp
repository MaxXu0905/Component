#include "schd_ctx.h"
#include "sshp_ctx.h"
#include "scan_file.h"

namespace ai
{
namespace app
{

using namespace ai::sg;

boost::once_flag schd_ctx::once_flag = BOOST_ONCE_INIT;
schd_ctx * schd_ctx::_instance = NULL;

schd_ctx * schd_ctx::instance()
{
	if (_instance == NULL)
		boost::call_once(once_flag, schd_ctx::init_once);
	return _instance;
}

void schd_ctx::pmid2mid(sgt_ctx *SGT)
{
	sgc_ctx *SGC = SGT->SGC();

	for (map<string, schd_cfg_entry_t>::iterator iter = _SCHD_cfg_entries.begin(); iter != _SCHD_cfg_entries.end(); ++iter) {
		schd_cfg_entry_t& entry = iter->second;

		BOOST_FOREACH(const string& hostname, entry.hostnames) {
			int mid = SGC->pmid2mid(hostname.c_str());
			entry.mids.push_back(mid);

			if (mid == BADMID)
				mid = SGC->_SGC_proc.mid;

			if (std::find(_SCHD_mids.begin(), _SCHD_mids.end(), mid) == _SCHD_mids.end())
				_SCHD_mids.push_back(mid);
		}
	}
}

void schd_ctx::connect()
{
	disconnect();
	activemq::library::ActiveMQCPP::initializeLibrary();

	try {
		if (_SCHD_brokerURI.empty())
			return;

		// Create a ConnectionFactory
		auto_ptr<cms::ConnectionFactory> connectionFactory(cms::ConnectionFactory::createCMSConnectionFactory(_SCHD_brokerURI));

		// Create a Connection
		_SCHD_connection = connectionFactory->createConnection();
		_SCHD_connection->start();

		// Create a Session
		_SCHD_session = _SCHD_connection->createSession(cms::Session::AUTO_ACKNOWLEDGE);

		if (!_SCHD_producerURI.empty()) {
			// Create the producer destination (Topic)
			_SCHD_producer_destination = _SCHD_session->createTopic(_SCHD_producerURI);

			// Create a MessageProducer from the Session to the Topic
			_SCHD_producer = _SCHD_session->createProducer(_SCHD_producer_destination);
			_SCHD_producer->setDeliveryMode(cms::DeliveryMode::PERSISTENT);
		}

		if (!_SCHD_consumerURI.empty()) {
			// Create the Consumer destination (Topic)
			_SCHD_consumer_destination = _SCHD_session->createTopic(_SCHD_consumerURI);

			// Create a MessageConsumer from the Session to the Topic
			_SCHD_consumer = _SCHD_session->createConsumer(_SCHD_consumer_destination);
		}

		_SCHD_connected = true;
	} catch (cms::CMSException& ex) {
		gpp_ctx *GPP = gpp_ctx::instance();
		GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't connect to {1}, {2}") % _SCHD_brokerURI % ex.what()).str(APPLOCALE));
	}

	_SCHD_retry_time = time(0) + 60;
}

void schd_ctx::disconnect()
{
	if (!_SCHD_connected)
		return;

	try {
		_SCHD_connection->close();
	} catch (cms::CMSException& ex) {
	}

	delete _SCHD_consumer_destination;
	_SCHD_consumer_destination = NULL;
	delete _SCHD_producer_destination;
	_SCHD_producer_destination = NULL;
	delete _SCHD_producer;
	_SCHD_producer = NULL;
	delete _SCHD_consumer;
	_SCHD_consumer = NULL;
	delete _SCHD_session;
	_SCHD_session = NULL;
	delete _SCHD_connection;
	_SCHD_connection = NULL;
	_SCHD_connected = false;

	activemq::library::ActiveMQCPP::shutdownLibrary();
}

int schd_ctx::count_files(const std::string& dir, const std::string& pattern, const std::string& sftp_prefix)
{
	int retry = 0;
	while (1) {
		try {
			vector<string> files;
			scan_file<> fscan(dir, pattern);
			if (!sftp_prefix.empty())
				fscan.set_sftp(sftp_prefix);
			fscan.get_files(files);

			return files.size();
		} catch (bad_ssh& ex) {
			if (++retry < 3) {
				sshp_ctx *SSHP = sshp_ctx::instance();
				SSHP->erase_session(sftp_prefix);
				continue;
			}

			return 0;
		} catch (exception& ex) {
			return 0;
		}
	}
}

void schd_ctx::set_service()
{
	_SCHD_svc_name = SCHD_SVC;
	_SCHD_svc_name += "_";
	_SCHD_svc_name += _SCHD_suffixURI;
}

bool schd_ctx::enable_service() const
{
	return !_SCHD_svc_name.empty();
}

void schd_ctx::inc_proc(const std::string& hostname, const std::string& entry_name)
{
	_SCHD_host_procs[hostname]++;

	schd_host_entry_t key;
	key.hostname = hostname;
	key.entry_name = entry_name;

	set<schd_host_entry_t>::iterator iter = _SCHD_entry_procs.find(key);
	if (iter == _SCHD_entry_procs.end()) {
		key.processes = 1;
		key.startable = true;
		_SCHD_entry_procs.insert(key);
	} else {
		schd_host_entry_t& entry = const_cast<schd_host_entry_t&>(*iter);
		entry.processes++;
	}
}

void schd_ctx::dec_proc(const std::string& hostname, const std::string& entry_name)
{
	_SCHD_host_procs[hostname]--;

	schd_host_entry_t key;
	key.hostname = hostname;
	key.entry_name = entry_name;

	set<schd_host_entry_t>::iterator iter = _SCHD_entry_procs.find(key);
	if (iter == _SCHD_entry_procs.end()) {
		gpp_ctx *GPP = gpp_ctx::instance();
		GPP->write_log(__FILE__, __LINE__, (_("FATAL: internal error")).str(APPLOCALE));
		exit(1);
	}

	schd_host_entry_t& entry = const_cast<schd_host_entry_t&>(*iter);
	if (--entry.processes == 0)
		_SCHD_entry_procs.erase(iter);
	else
		entry.startable = false;
}

schd_ctx::schd_ctx()
{
	_SCHD_shutdown = false;

	_SCHD_sqlite3_db = NULL;
	_SCHD_select_stmt = NULL;
	_SCHD_insert_stmt = NULL;
	_SCHD_update_stmt = NULL;
	_SCHD_delete_stmt = NULL;

	_SCHD_parallel_entries = 0;
	_SCHD_parallel_procs = 0;
	_SCHD_polltime = 0;
	_SCHD_robustint = 0;
	_SCHD_resume = false;
	_SCHD_stop_timeout = 0;
	_SCHD_stop_signo = 0;

	_SCHD_connected = false;
	_SCHD_producer_destination = NULL;
	_SCHD_consumer_destination = NULL;
	_SCHD_producer = NULL;
	_SCHD_consumer = NULL;
	_SCHD_session = NULL;
	_SCHD_connection = NULL;
	_SCHD_retry_time = 0;

	_SCHD_running_entries = 0;
}

schd_ctx::~schd_ctx()
{
	if (_SCHD_select_stmt != NULL)
		sqlite3_finalize(_SCHD_select_stmt);

	if (_SCHD_insert_stmt != NULL)
		sqlite3_finalize(_SCHD_insert_stmt);

	if (_SCHD_update_stmt != NULL)
		sqlite3_finalize(_SCHD_update_stmt);

	if (_SCHD_delete_stmt != NULL)
		sqlite3_finalize(_SCHD_delete_stmt);

	if (_SCHD_sqlite3_db != NULL)
		sqlite3_close(_SCHD_sqlite3_db);

	disconnect();
}

void schd_ctx::init_once()
{
	_instance = new schd_ctx();
}

}
}

