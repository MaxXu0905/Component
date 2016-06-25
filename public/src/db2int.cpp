#include "sa_struct.h"
#include "db2int.h"

namespace bf = boost::filesystem;
namespace po = boost::program_options;
namespace bp = boost::posix_time;
namespace bc = boost::chrono;
using namespace ai::scci;
using namespace ai::sg;
using namespace std;

namespace ai
{
namespace app
{

db2int::db2int()
{
	db = NULL;
}

db2int::~db2int()
{
	delete db;
}

void db2int::run(int argc, char **argv)
{
	string user_name;
	string password;
	string connect_string;
	string prefix_file;

	po::options_description desc("Allowed options");
	desc.add_options()
		("help", (_("produce help message")).str(APPLOCALE).c_str())
		("user_name,u", po::value<string>(&user_name)->required(), (_("set database user name")).str(APPLOCALE).c_str())
		("password,p", po::value<string>(&password)->required(), (_("set database password")).str(APPLOCALE).c_str())
		("connect_string,c", po::value<string>(&connect_string)->required(), (_("set database connect string")).str(APPLOCALE).c_str())
		("prefix,o", po::value<string>(&prefix_file)->required(), (_("set prefix of output XML file")).str(APPLOCALE).c_str())
		("server,s", (_("generate server side XML file")).str(APPLOCALE).c_str())
	;

	try {
		po::variables_map vm;
		po::store(po::command_line_parser(argc, argv).options(desc).run(), vm);

		if (vm.count("help")) {
			std::cout << desc << std::endl;
			exit(0);
		}

		if (vm.count("server"))
			server_flag = true;
		else
			server_flag = false;

		po::notify(vm);
	} catch (exception& ex) {
		std::cout << ex.what() << std::endl;
		std::cout << desc << std::endl;
		exit(1);
	}

	gpenv& env_mgr = gpenv::instance();
	if (env_mgr.getenv("NLS_LANG") == NULL)
		env_mgr.putenv("NLS_LANG=SIMPLIFIED CHINESE_CHINA.AL32UTF8");

	map<string, string> conn_info;

	conn_info["user_name"] = user_name;
	conn_info["password"] = password;
	conn_info["connect_string"] = connect_string;

	database_factory& factory_mgr = database_factory::instance();
	db = factory_mgr.create("ORACLE");
	db->connect(conn_info);

	load_record_flag();

	auto_ptr<Generic_Statement> auto_stmt(db->create_statement());
	Generic_Statement *stmt = auto_stmt.get();

	auto_ptr<struct_dynamic> auto_data(db->create_data());
	struct_dynamic *data = auto_data.get();
	if (data == NULL)
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Memory allocation failure")).str(APPLOCALE));

	data->setSQL("select distinct file_type{char[4]} from bps_src_file");
	stmt->bind(data);

	auto_ptr<Generic_ResultSet> auto_rset(stmt->executeQuery());
	Generic_ResultSet *rset = auto_rset.get();

	while (rset->next()) {
		string file_type = rset->getString(1);

		try {
			string output_file = prefix_file + "_" + file_type + ".xml";
			ofs.open(output_file.c_str());
			if (!ofs)
				throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Can't open file {1}") % output_file).str(APPLOCALE));

			get_summary_global();
			get_code_distribute_map();

			if (server_flag) {
				ofs << "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
					<< "<services xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:noNamespaceSchemaLocation=\"bps.xsd\">\n"
					<< "\t<service>\n";

				prefix = "\t\t";
				load_service(file_type);

				ofs << "\t</service>\n"
					<< "</services>\n";
			} else {
				ofs << "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
					<< "<integrator xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:noNamespaceSchemaLocation=\"integrator.xsd\">\n"
					<< "\t<resource>\n"
					<< "\t\t<integrator_name>" << file_type << "</integrator_name>\n"
					<< "\t\t<openinfo>${OPENINFO}</openinfo>\n"
					<< "\t\t<libs>libsqloracle.so libbps.so libdup.so</libs>\n"
					<< "\t\t<sginfo/>\n"
					<< "\t\t<dbcinfo>${DBCINFO}</dbcinfo>\n"
					<< "\t</resource>\n";

				prefix = "\t";
				load_global(file_type);

				ofs << "\t<adaptors>\n"
					<< "\t\t<adaptor>\n";

				prefix = "\t\t\t";
				load_adaptor(file_type);

				ofs << "\t\t</adaptor>\n"
					<< "\t</adaptors>\n"
					<< "</integrator>\n";
			}
		} catch (exception& ex) {
			cout << "parse file_type " << file_type << " failed" << endl;
			cout << ex.what() << endl;
		}

		ofs.close();
	}
}

void db2int::load_service(const string& file_type)
{
	load_resource(file_type, 0);
	load_global(file_type);
	load_input(file_type);
	load_output(file_type);
	load_process(file_type);
}

void db2int::load_adaptor(const string& file_type)
{
	string rule_dup = load_args(file_type);
	if (rule_dup.empty()) {
		load_resource(file_type, 0);
		load_source(file_type);
		load_invalid(file_type);
		load_error(file_type, true);
		load_input(file_type);
		load_output(file_type);
		if((file_type=="RECE")|| (file_type=="ARRE") || (file_type=="PAID"))
		{
			load_summary(file_type);
		}
		load_distribute(file_type);
	} else {
		load_resource(file_type, 1);
		load_source(file_type);
		load_invalid(file_type);
		load_error(file_type, true);
		load_input(file_type);

		ofs << prefix << "<args>\n";
		prefix += "\t";

		ofs << prefix << "<arg>\n";
		prefix += "\t";

		ofs << prefix << "<name>rule_dup</name>\n"
			<< prefix << "<value><![CDATA[\n"
			<< "strcpy(#0, \"dup_" << file_type << "\");\n"
			<< "sprintf(#1, \"%s,%s\", file_name, $record_sn);\n"
			<< rule_dup << "\n"
			<< "]]></value>\n";

		prefix.resize(prefix.size() - 1);
		ofs << prefix << "</arg>\n" ;

		ofs << prefix << "<arg>\n";
		prefix += "\t";

		ofs << prefix << "<name>tables</name>\n"
			<< prefix << "<value>dup_" << file_type << "</value>\n";

		prefix.resize(prefix.size() - 1);
		ofs << prefix << "</arg>\n" ;

		prefix.resize(prefix.size() - 1);
		ofs << prefix << "</args>\n" ;

		ofs << "\t\t</adaptor>\n"
			<< "\t\t<adaptor>\n";

		load_resource(file_type, 2);
		load_invalid(file_type);
		load_error(file_type, true);
		load_output(file_type);
		if((file_type=="RECE")|| (file_type=="ARRE") || (file_type=="PAID"))
		{
			load_summary(file_type);
		}
		load_distribute(file_type);
	}
}

void db2int::get_summary_global()
{
	auto_ptr<Generic_Statement> auto_stmt(db->create_statement());
	Generic_Statement *stmt = auto_stmt.get();

	auto_ptr<struct_dynamic> auto_data(db->create_data());
	struct_dynamic *data = auto_data.get();
	if (data == NULL)
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Memory allocation failure")).str(APPLOCALE));

	data->setSQL("select lower(column_name), data_length from user_tab_columns where table_name = 'LOG_BPS_XXXX' order by column_id");
	stmt->bind(data);

	auto_ptr<Generic_ResultSet> auto_rset(stmt->executeQuery());
	Generic_ResultSet *rset = auto_rset.get();
	bool found = false;

	log_global_vec.clear();

	while (rset->next()) {
		log_bps_table_t item;
		item.column_name = rset->getString(1);

		if (!found) {
			if (item.column_name == "file_time")
				found = true;

			continue;
		} else {
			item.column_length = rset->getInt(2);
			log_global_vec.push_back(item);
		}
	}
}

void db2int::get_code_distribute_map()
{
	auto_ptr<Generic_Statement> auto_stmt(db->create_statement());
	Generic_Statement *stmt = auto_stmt.get();

	auto_ptr<struct_dynamic> auto_data(db->create_data());
	struct_dynamic *data = auto_data.get();
	if (data == NULL)
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Memory allocation failure")).str(APPLOCALE));

	data->setSQL("select file_type, distribute_rule from code_distribute_map order by file_type");
	stmt->bind(data);

	auto_ptr<Generic_ResultSet> auto_rset(stmt->executeQuery());
	Generic_ResultSet *rset = auto_rset.get();

	distribute_items.clear();

	while (rset->next()) {
		distribute_item_t item;
		item.file_type = rset->getString(1);
		item.distribute_rule = rset->getString(2);
		distribute_items.push_back(item);
	}
}

void db2int::load_log_global()
{
	for (vector<log_bps_table_t>::const_iterator iter = log_global_vec.begin(); iter != log_global_vec.end(); iter++) {
		ofs << prefix << "<field>\n";
		prefix += '\t';

		ofs << prefix << "<field_name>" << iter->column_name << "</field_name>\n"
			<< prefix << "<field_size>" << iter->column_length << "</field_size>\n"
			<< prefix << "<readonly>N</readonly>\n";

		prefix.resize(prefix.size() - 1);
		ofs << prefix << "</field>\n";
	}
}

void db2int::load_resource(const string& file_type, int type)
{
	ofs << prefix << "<resource>\n";
	prefix += "\t";

	if (server_flag) {
		ofs << prefix << "<svc_key>" << file_type << "</svc_key>\n"
			<< prefix << "<version>-1</version>\n"
			<< prefix << "<disable_global>N</disable_global>\n";
	} else {
		if (type == 0) {
			ofs << prefix << "<com_key>BPS</com_key>\n"
				<< prefix << "<svc_key>" << file_type << "</svc_key>\n"
				<< prefix << "<batch>1</batch>\n"
				<< prefix << "<concurrency>50</concurrency>\n"
				<< prefix << "<disable_global>N</disable_global>\n";
		} else if (type == 1) {
			ofs << prefix << "<com_key>DUP</com_key>\n"
				<< prefix << "<svc_key>ALL</svc_key>\n"
				<< prefix << "<batch>1</batch>\n"
				<< prefix << "<concurrency>50</concurrency>\n"
				<< prefix << "<disable_global>N</disable_global>\n";
		} else {
			ofs << prefix << "<com_key>BPS</com_key>\n"
				<< prefix << "<svc_key>" << file_type << "</svc_key>\n"
				<< prefix << "<batch>1</batch>\n"
				<< prefix << "<concurrency>50</concurrency>\n"
				<< prefix << "<disable_global>N</disable_global>\n"
				<< prefix << "<options>FROM_INPUT</options>\n";
		}
	}

	prefix.resize(prefix.size() - 1);
	ofs << prefix << "</resource>\n";
}

void db2int::load_thread_pattern(const string& thread_id, string& pattern)
{
	auto_ptr<Generic_Statement> auto_stmt(db->create_statement());
	Generic_Statement *stmt = auto_stmt.get();

	auto_ptr<struct_dynamic> auto_data(db->create_data());
	struct_dynamic *data = auto_data.get();
	if (data == NULL)
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Memory allocation failure")).str(APPLOCALE));

	data->setSQL("select para_contents{char[4000]} from sys_thread_para where thread_id = :thread_id{int} and para_name ='pattern'");
	stmt->bind(data);

	stmt->setString(1, thread_id);

	auto_ptr<Generic_ResultSet> auto_rset(stmt->executeQuery());
	Generic_ResultSet *rset = auto_rset.get();
	if (rset->next())
		pattern = rset->getString(1);
}
void db2int::load_pattern(const string& file_type, string &pattern)
{
	auto_ptr<Generic_Statement> auto_stmt(db->create_statement());
	Generic_Statement *stmt = auto_stmt.get();

	auto_ptr<struct_dynamic> auto_data(db->create_data());
	struct_dynamic *data = auto_data.get();
	if (data == NULL)
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Memory allocation failure")).str(APPLOCALE));

	data->setSQL("select thread_id{int} from sys_thread_para where para_contents = :file_type{char[4]}");
	stmt->bind(data);

	stmt->setString(1, file_type);

	auto_ptr<Generic_ResultSet> auto_rset(stmt->executeQuery());
	Generic_ResultSet *rset = auto_rset.get();

	if (rset->next()) {
		string thread_id = rset->getString(1);
		load_thread_pattern(thread_id,pattern);
	}
}

void db2int::load_source(const string& file_type)
{
	string pattern;
	auto_ptr<Generic_Statement> auto_stmt(db->create_statement());
	Generic_Statement *stmt = auto_stmt.get();

	auto_ptr<struct_dynamic> auto_data(db->create_data());
	struct_dynamic *data = auto_data.get();
	if (data == NULL)
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Memory allocation failure")).str(APPLOCALE));

	data->setSQL("select record_mode{int} from bps_src_file "
		"where file_type = :file_type{char[4]} and record_serial = 0");
	stmt->bind(data);

	stmt->setString(1, file_type);

	auto_ptr<Generic_ResultSet> auto_rset(stmt->executeQuery());
	Generic_ResultSet *rset = auto_rset.get();

	while (rset->next()) {
		int record_mode = rset->getInt(1);

		load_pattern(file_type , pattern);

		ofs << prefix << "<source>\n";
		prefix += '\t';

		switch (record_mode) {
		case 0: // 定长
			ofs << prefix << "<class_name>sa_ifixed</class_name>\n";
			break;
		case 1: // 变长
			ofs << prefix << "<class_name>sa_ivariable</class_name>\n";
			break;
		case 2: // ASN.1
			ofs << prefix << "<class_name>sa_iasn1</class_name>\n";
			break;
		case 3: // 二进制
			ofs << prefix << "<class_name>sa_ibinary</class_name>\n";
			break;
		default:
			throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Invalid record_mode {1}") % record_mode).str(APPLOCALE));
		}

		ofs << prefix << "<per_records>100000</per_records>\n"
			<< prefix << "<args>\n";
		prefix += '\t';

		ofs << prefix << "<arg>\n";
		prefix += '\t';

		ofs << prefix << "<name>protocol</name>\n"
			<< prefix << "<value>${SFTP_PREFIX}</value>\n";

		prefix.resize(prefix.size() - 1);
		ofs << prefix << "</arg>\n"
			<< prefix << "<arg>\n";
		prefix += '\t';

		ofs << prefix << "<name>src_dir</name>\n"
			<< prefix << "<value>${RUNICAL_DT}/bps/" << file_type << "/in</value>\n";

		prefix.resize(prefix.size() - 1);
		ofs << prefix << "</arg>\n"
			<< prefix << "<arg>\n";
		prefix += '\t';

		ofs << prefix << "<name>dup_dir</name>\n"
			<< prefix << "<value>${RUNICAL_DT}/bps/" << file_type << "/dup</value>\n";

		prefix.resize(prefix.size() - 1);
		ofs << prefix << "</arg>\n"
			<< prefix << "<arg>\n";
		prefix += '\t';

		ofs << prefix << "<name>bak_dir</name>\n"
			<< prefix << "<value>${RUNICAL_DT}/bps/" << file_type << "/bak</value>\n";

		prefix.resize(prefix.size() - 1);
		ofs << prefix << "</arg>\n"
			<< prefix << "<arg>\n";
		prefix += '\t';

		ofs << prefix << "<name>tmp_dir</name>\n"
			<< prefix << "<value>${RUNICAL_DT}/bps/" << file_type << "/tmp</value>\n";

		prefix.resize(prefix.size() - 1);
		ofs << prefix << "</arg>\n"
			<< prefix << "<arg>\n";
		prefix += '\t';

		ofs << prefix << "<name>ftl_dir</name>\n"
			<< prefix << "<value>${RUNICAL_DT}/bps/" << file_type << "/ftl</value>\n";

		prefix.resize(prefix.size() - 1);
		ofs << prefix << "</arg>\n"
			<< prefix << "<arg>\n";
		prefix += '\t';

		ofs << prefix << "<name>local_dir</name>\n"
			<< prefix << "<value>${UNICAL_DT}/bps/" << file_type << "/tmp</value>\n";

		prefix.resize(prefix.size() - 1);
		ofs << prefix << "</arg>\n"
			<< prefix << "<arg>\n";
		prefix += '\t';

		ofs << prefix << "<name>pattern</name>\n"
			<< prefix << "<value>"<< pattern << "</value>\n";

		prefix.resize(prefix.size() - 1);
		ofs << prefix << "</arg>\n"
			<< prefix << "<arg>\n";
		prefix += '\t';

		ofs << prefix << "<name>bak_flag</name>\n"
			<< prefix << "<value>RENAME</value>\n";

		prefix.resize(prefix.size() - 1);
		ofs << prefix << "</arg>\n"
			<< prefix << "<arg>\n";
		prefix += '\t';

		ofs << prefix << "<name>zip_cmd</name>\n"
			<< prefix << "<value></value>\n";

		prefix.resize(prefix.size() - 1);
		ofs << prefix << "</arg>\n"
			<< prefix << "<arg>\n";
		prefix += '\t';

		ofs << prefix << "<name>sleep_time</name>\n"
			<< prefix << "<value>60</value>\n";

		prefix.resize(prefix.size() - 1);
		ofs << prefix << "</arg>\n"
			<< prefix << "<arg>\n";
		prefix += '\t';

		ofs << prefix << "<name>exit_nofile</name>\n"
			<< prefix << "<value>Y</value>\n";

		prefix.resize(prefix.size() - 1);
		ofs << prefix << "</arg>\n"
			<< prefix << "<arg>\n";
		prefix += '\t';

		ofs << prefix << "<name>deal_flag</name>\n"
			<< prefix << "<value>WHOLE</value>\n";

		prefix.resize(prefix.size() - 1);
		ofs << prefix << "</arg>\n"
			<< prefix << "<arg>\n";
		prefix += '\t';

		ofs << prefix << "<name>rule_check</name>\n"
			<< prefix << "<value></value>\n";

		prefix.resize(prefix.size() - 1);
		ofs << prefix << "</arg>\n"
			<< prefix << "<arg>\n";
		prefix += '\t';

		ofs << prefix << "<name>record_flag</name>\n"
			<< prefix << "<value>";

		map<string, int>::const_iterator iter = record_flag_map.find(file_type);
		if (iter != record_flag_map.end()) {
			switch (iter->second) {
			case 0:
				break;
			case 1:
				ofs << "HEAD";
				break;
			case 2:
				ofs << "TAIL";
				break;
			case 3:
				ofs << "HEAD TAIL";
				break;
			default:
				break;
			}
		}

		ofs << "</value>\n";

		prefix.resize(prefix.size() - 1);
		ofs << prefix << "</arg>\n";

		prefix.resize(prefix.size() - 1);
		ofs << prefix << "</args>\n";

		prefix.resize(prefix.size() - 1);
		ofs << prefix << "</source>\n";
	}
}


void db2int::load_target(const string& file_type)
{
	ofs << prefix << "<target>\n";
	prefix += '\t';

	ofs << prefix << "<class_name>sa_otarget</class_name>\n"
		<< prefix << "<args>\n";
	prefix += '\t';

	ofs << prefix << "<arg>\n";
	prefix += '\t';

	ofs << prefix << "<name>directory</name>\n"
		<< prefix << "<value>${UNICAL_DT}/stat/" << file_type << "/tgt</value>\n";

	prefix.resize(prefix.size() - 1);
	ofs << prefix << "</arg>\n";

	ofs << prefix << "<arg>\n";
	prefix += '\t';

	ofs << prefix << "<name>rdirectory</name>\n"
		<< prefix << "<value>${SFTP_PREFIX}:${RUNICAL_DT}/stat/" << file_type << "/tgt</value>\n";

	prefix.resize(prefix.size() - 1);
	ofs << prefix << "</arg>\n";

	prefix.resize(prefix.size() - 1);
	ofs << prefix << "</args>\n";

	prefix.resize(prefix.size() - 1);
	ofs << prefix << "</target>\n";
}

void db2int::load_invalid(const string& file_type)
{
	ofs << prefix << "<invalid>\n";
	prefix += '\t';

	ofs << prefix << "<class_name>sa_osource</class_name>\n"
		<< prefix << "<args>\n";
	prefix += '\t';

	ofs << prefix << "<arg>\n";
	prefix += '\t';

	ofs << prefix << "<name>directory</name>\n"
		<< prefix << "<value>${UNICAL_DT}/bps/" << file_type << "/inv</value>\n";

	prefix.resize(prefix.size() - 1);
	ofs << prefix << "</arg>\n";

	ofs << prefix << "<arg>\n";
	prefix += '\t';

	ofs << prefix << "<name>rdirectory</name>\n"
		<< prefix << "<value>${SFTP_PREFIX}:${RUNICAL_DT}/bps/" << file_type << "/inv</value>\n";

	prefix.resize(prefix.size() - 1);
	ofs << prefix << "</arg>\n";

	prefix.resize(prefix.size() - 1);
	ofs << prefix << "</args>\n";

	prefix.resize(prefix.size() - 1);
	ofs << prefix << "</invalid>\n";
}

void db2int::load_error(const string& file_type, bool specify_table)
{
	ofs << prefix << "<error>\n";
	prefix += '\t';

	if (!specify_table) {
		ofs << prefix << "<class_name>sa_osource</class_name>\n"
			<< prefix << "<args>\n";
		prefix += '\t';

		ofs << prefix << "<arg>\n";
		prefix += '\t';

		ofs << prefix << "<name>directory</name>\n"
			<< prefix << "<value>${UNICAL_DT}/bps/" << file_type << "/err</value>\n";

		prefix.resize(prefix.size() - 1);
		ofs << prefix << "</arg>\n";

		ofs << prefix << "<arg>\n";
		prefix += '\t';

		ofs << prefix << "<name>rdirectory</name>\n"
			<< prefix << "<value>${SFTP_PREFIX}:${RUNICAL_DT}/bps/" << file_type << "/err</value>\n";
	} else {
		ofs << prefix << "<class_name>sa_odbsource</class_name>\n"
			<< prefix << "<args>\n";
		prefix += '\t';

		ofs << prefix << "<arg>\n";
		prefix += '\t';

		ofs << prefix << "<name>tables</name>\n"
			<< prefix << "<value>ERR_BPS_" << file_type << "</value>\n";

		prefix.resize(prefix.size() - 1);
		ofs << prefix << "</arg>\n";

		ofs << prefix << "<arg>\n";
		prefix += '\t';

		ofs << prefix << "<name>per_records</name>\n"
			<< prefix << "<value>1000</value>\n";

		prefix.resize(prefix.size() - 1);
		ofs << prefix << "</arg>\n";

		ofs << prefix << "<arg>\n";
		prefix += '\t';

		ofs << prefix << "<name>rollback</name>\n"
			<< prefix << "<value>DELETE</value>\n";
	}

	prefix.resize(prefix.size() - 1);
	ofs << prefix << "</arg>\n";

	prefix.resize(prefix.size() - 1);
	ofs << prefix << "</args>\n";

	prefix.resize(prefix.size() - 1);
	ofs << prefix << "</error>\n";
}

void db2int::load_global(const string& file_type)
{
	auto_ptr<Generic_Statement> auto_stmt(db->create_statement());
	Generic_Statement *stmt = auto_stmt.get();

	auto_ptr<struct_dynamic> auto_data(db->create_data());
	struct_dynamic *data = auto_data.get();
	if (data == NULL)
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Memory allocation failure")).str(APPLOCALE));

	data->setSQL("select lower(field_name){char[63]}, default_value{char[255]}, field_length{int} "
		"from bps_file_variables "
		"where file_type = :file_type{char[4]} "
		"order by field_serial");
	stmt->bind(data);

	stmt->setString(1, file_type);

	auto_ptr<Generic_ResultSet> auto_rset(stmt->executeQuery());
	Generic_ResultSet *rset = auto_rset.get();
	bool first = true;
	int field_serial = 0;

	global_map.clear();
	global_map[field_serial] = "svc_key";
	global_map[field_serial++] = "file_name";
	global_map[field_serial++] = "file_sn";
	global_map[field_serial++] = "redo_mark";
	global_map[field_serial++] = "sysdate";
	global_map[field_serial++] = "first_serial";
	global_map[field_serial++] = "second_serial";

	if (!log_global_vec.empty()) {
		ofs << prefix << "<global>\n";
		prefix += '\t';

		ofs << prefix << "<fields>\n";
		prefix += '\t';

		load_log_global();
		first = false;
	}

	while (rset->next()) {
		if (first) {
			ofs << prefix << "<global>\n";
			prefix += '\t';

			ofs << prefix << "<fields>\n";
			prefix += '\t';
			first = false;
		}

		ofs << prefix << "<field>\n";
		prefix += '\t';

		ofs << prefix << "<field_name>" << rset->getString(1) << "</field_name>\n";

		if (!server_flag)
			ofs << prefix << "<default_value>" << rset->getString(2) << "</default_value>\n";

		ofs << prefix << "<field_size>" << rset->getInt(3) << "</field_size>\n"
			<< prefix << "<readonly>N</readonly>\n";

		prefix.resize(prefix.size() - 1);
		ofs << "</field>\n";

		global_map[field_serial++] = rset->getString(1);
	}


	if (!first) {
		prefix.resize(prefix.size() - 1);
		ofs << prefix << "</fields>\n";

		prefix.resize(prefix.size() - 1);
		ofs << prefix << "</global>\n";
	}
}

void db2int::load_input(const string& file_type)
{
	auto_ptr<Generic_Statement> auto_stmt(db->create_statement());
	Generic_Statement *stmt = auto_stmt.get();

	auto_ptr<struct_dynamic> auto_data(db->create_data());
	struct_dynamic *data = auto_data.get();
	if (data == NULL)
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Memory allocation failure")).str(APPLOCALE));

	data->setSQL("select a.record_serial{int}, a.record_type{int}, a.delimit{char[1]}, a.rule_condition{char[4000]}, "
		"b.factor1{char[10]}, b.factor2{int}, lower(b.field_name){char[63]}, b.encode_type{int}, b.parent_serial{int} "
		"from bps_src_file a, bps_src_record b "
		"where a.file_type = b.file_type and a.record_serial = b.record_serial and a.file_type = :file_type{char[4]} "
		"order by a.record_serial, b.field_serial");
	stmt->bind(data);

	stmt->setString(1, file_type);

	auto_ptr<Generic_ResultSet> auto_rset(stmt->executeQuery());
	Generic_ResultSet *rset = auto_rset.get();
	int record_serial = -1;

	while (rset->next()) {
		if (record_serial != rset->getInt(1)) {
			if (record_serial == -1) {
				ofs << prefix << "<input>\n";
				prefix += '\t';
			} else {
				prefix.resize(prefix.size() - 1);
				ofs << prefix << "</fields>\n";

				prefix.resize(prefix.size() - 1);
				ofs << prefix << "</table>\n";
			}

			ofs << prefix << "<table>\n";
			prefix += '\t';
			ofs << prefix << "<name>input</name>\n";

			if (!server_flag) {
				ofs << prefix << "<record_type>";

				switch (rset->getInt(2)) {
				case 0:
					ofs << "HEAD";
					break;
				case 1:
					ofs << "BODY";
					break;
				case 2:
					ofs << "TAIL";
					break;
				default:
					throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: unknown record_type {1}") % rset->getInt(2)).str(APPLOCALE));
				}

				ofs << "</record_type>\n";

				unsigned int delimit = static_cast<unsigned int>(rset->getString(3)[0]);
				if (delimit != 0)
					ofs << prefix << "<delimiter>" << delimit << "</delimiter>\n";

				ofs << prefix << "<rule_condition><![CDATA[\n" << rset->getString(4) << "\n]]></rule_condition>\n";
			}

			ofs << prefix << "<fields>\n";
			prefix += '\t';

			record_serial = rset->getInt(1);
		}

		ofs << prefix << "<field>\n";
		prefix += '\t';

		int encode_type = rset->getInt(8);

		if (server_flag) {
			ofs << prefix << "<field_size>";

			if (encode_type == ENCODE_ASCII)
				ofs << rset->getInt(6);
			else
				ofs << (rset->getInt(6) * 2);

			ofs << "</field_size>\n"
				<< prefix << "<field_name>" << rset->getString(7) << "</field_name>\n";
		} else {
			ofs << prefix << "<factor1>" << rset->getString(5) << "</factor1>\n"
				<< prefix << "<factor2>" << rset->getInt(6) << "</factor2>\n"
				<< prefix << "<field_name>" << rset->getString(7) << "</field_name>\n";

			if (encode_type != ENCODE_NONE)
				ofs << prefix << "<encode_type>";

			switch (encode_type) {
			case ENCODE_NONE:
				break;
			case ENCODE_ASCII:
				ofs << "ASCII";
				break;
			case ENCODE_INTEGER:
				ofs << "INTEGER";
				break;
			case ENCODE_OCTETSTRING:
				ofs << "OCTETSTRING";
				break;
			case ENCODE_BCD:
				ofs << "BCD";
				break;
			case ENCODE_TBCD:
				ofs << "TBCD";
				break;
			case ENCODE_ASN:
				ofs << "ASN";
				break;
			case ENCODE_ADDRSTRING:
				ofs << "ADDRSTRING";
				break;
			case ENCODE_SEQUENCEOF:
				ofs << "SEQUENCEOF";
				break;
			case ENCODE_REALBCD:
				ofs << "REALBCD";
				break;
			case ENCODE_REALTBCD:
				ofs << "REALTBCD";
				break;
			case ENCODE_BCDD:
				ofs << "BCDD";
				break;
			case ENCODE_UNINTEGER:
				ofs << "UNINTEGER";
				break;
			default:
				throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: unknown encode_type {1}") % encode_type).str(APPLOCALE));
			}

			if (encode_type != ENCODE_NONE)
				ofs << "</encode_type>\n";

			if (rset->getInt(9) != -1)
				ofs << prefix << "<parent_serial>" << rset->getInt(9) << "</parent_serial>\n";
		}

		prefix.resize(prefix.size() - 1);
		ofs << prefix << "</field>\n";
	}

	if (record_serial != -1) {
		prefix.resize(prefix.size() - 1);
		ofs << prefix << "</fields>\n";

		prefix.resize(prefix.size() - 1);
		ofs << prefix << "</table>\n";

		prefix.resize(prefix.size() - 1);
		ofs << prefix << "</input>\n";
	}
}

string db2int::load_args(const string& file_type)
{
	auto_ptr<Generic_Statement> auto_stmt(db->create_statement());
	Generic_Statement *stmt = auto_stmt.get();

	auto_ptr<struct_dynamic> auto_data(db->create_data());
	struct_dynamic *data = auto_data.get();
	if (data == NULL)
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Memory allocation failure")).str(APPLOCALE));

	data->setSQL("select rule_dup{char[4000]} from code_rule_dup where file_type = :file_type{char[4]}");
	stmt->bind(data);

	stmt->setString(1, file_type);

	auto_ptr<Generic_ResultSet> auto_rset(stmt->executeQuery());
	Generic_ResultSet *rset = auto_rset.get();

	if (rset->next())
		return rset->getString(1);
	else
		return "";

}

void db2int::load_output(const string& file_type)
{
	auto_ptr<Generic_Statement> auto_stmt(db->create_statement());
	Generic_Statement *stmt = auto_stmt.get();

	auto_ptr<struct_dynamic> auto_data(db->create_data());
	struct_dynamic *data = auto_data.get();
	if (data == NULL)
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Memory allocation failure")).str(APPLOCALE));

	data->setSQL("select lower(field_name){char[63]}, field_length{int} "
		"from bps_medial "
		"where file_type = :file_type{char[4]} "
		"order by field_serial");
	stmt->bind(data);

	stmt->setString(1, file_type);

	auto_ptr<Generic_ResultSet> auto_rset(stmt->executeQuery());
	Generic_ResultSet *rset = auto_rset.get();
	bool first = true;
	int field_serial = 0;

	output_map.clear();

	while (rset->next()) {
		if (first) {
			ofs << prefix << "<output>\n";
			prefix += '\t';

			ofs << prefix << "<table>\n";
			prefix += '\t';
			ofs << prefix << "<name>output</name>\n";

			ofs << prefix << "<fields>\n";
			prefix += '\t';

			first = false;
		}


		ofs << prefix << "<field>\n";
		prefix += '\t';

		ofs << prefix << "<field_name>"<< rset->getString(1) << "</field_name>\n"
			<< prefix << "<field_size>" << rset->getInt(2) << "</field_size>\n";

		prefix.resize(prefix.size() - 1);
		ofs << prefix << "</field>\n";

		output_map[field_serial++] = rset->getString(1);
	}

	if (!first) {
		prefix.resize(prefix.size() - 1);
		ofs << prefix << "</fields>\n";

		prefix.resize(prefix.size() - 1);
		ofs << prefix << "</table>\n";

		prefix.resize(prefix.size() - 1);
		ofs << prefix << "</output>\n";
	}
}

void db2int::load_summary(const string& file_type)
{
	ofs << prefix << "<summary>\n";
	prefix += "\t";

	ofs << prefix << "<class_name>sa_osummary</class_name>\n";

	ofs << prefix << "<items>\n";
	prefix +="\t";

	ofs << prefix << "<item>\n";
	prefix +="\t";

	ofs << prefix << "<table_name>sum_bps</table_name>\n";

	ofs << prefix <<  "<key>\n";
	prefix +="\t";

	ofs << prefix << "<field>\n";
	prefix +="\t";

	ofs << prefix << "<field_name>file_name</field_name>\n"
		<< prefix << "<field_type>STRING</field_type>\n"
		<< prefix << "<field_size>64</field_size>\n";

	prefix.resize(prefix.size() - 1);
	ofs << prefix << "</field>\n" ;

	prefix.resize(prefix.size() - 1);
	ofs << prefix << "</key>\n" ;

	ofs << prefix << "<value>\n";
	prefix +="\t";

	for (vector<log_bps_table_t>::const_iterator iter = log_global_vec.begin(); iter != log_global_vec.end(); iter++) {
		ofs << prefix << "<field>\n";
		prefix +="\t";

		ofs << prefix << "<field_name>" << iter->column_name << "</field_name>\n"
			<< prefix << "<field_type>DOUBLE</field_type>\n"
			<< prefix << "<action>ADD</action>\n";

		prefix.resize(prefix.size() - 1);
		ofs << prefix << "</field>\n" ;
	}

	prefix.resize(prefix.size() - 1);
	ofs << prefix << "</value>\n" ;

	prefix.resize(prefix.size() - 1);
	ofs << prefix <<  "</item>\n";

	prefix.resize(prefix.size() - 1);
	ofs << prefix <<  "</items>\n";

	prefix.resize(prefix.size() - 1);
	ofs << prefix << "</summary>\n";
}


void db2int::load_distribute(const string& file_type)
{
	string rule_file_name;
	string pattern;
	auto_ptr<Generic_Statement> auto_stmt(db->create_statement());
	Generic_Statement *stmt = auto_stmt.get();

	auto_ptr<struct_dynamic> auto_data(db->create_data());
	struct_dynamic *data = auto_data.get();
	if (data == NULL)
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Memory allocation failure")).str(APPLOCALE));

	data->setSQL("select rule_condition{char[4000]}, rule_file_name{char[4000]}, dst_dir{char[4000]}, is_internal{int}, "
		"head_serial{int}, head_delimit{char[1]}, has_head_return{int}, is_head_fixed{int}, "
		"body_serial{int}, body_delimit{char[1]}, has_body_return{int}, is_body_fixed{int}, "
		"tail_serial{int}, tail_delimit{char[1]}, has_tail_return{int}, is_tail_fixed{int} "
		"from bps_dst_file "
		"where file_type = :file_type{char[4]} "
		"order by file_serial");
	stmt->bind(data);

	stmt->setString(1, file_type);

	auto_ptr<Generic_ResultSet> auto_rset(stmt->executeQuery());
	Generic_ResultSet *rset = auto_rset.get();
	bool first = true;

	while (rset->next()) {
		if (first) {
			ofs << prefix << "<distribute>\n";
			prefix += '\t';

			ofs << prefix << "<class_name>sa_ostream</class_name>\n"
				<< prefix << "<max_open_files>1000</max_open_files>\n"
				<< prefix << "<items>\n";
			prefix += '\t';
			first = false;
		}

		rule_file_name = rset->getString(2);
		ofs << prefix << "<item>\n";
		prefix += '\t';

#if defined(USE_REMOTE)
		if (in_distribute_map(file_type, rule_file_name)) {
			string dst_dir = "${DBCDATA}/dat/admin";
			string::size_type pos1 = rule_file_name.find("\"");
			string::size_type pos2 = rule_file_name.find(".");
			if (pos1 == string::npos || pos2 == string::npos)
				pattern = "^.*$";
			else
				pattern = string("^") + string(rule_file_name.begin() + pos1 + 1, rule_file_name.begin() + pos2) + "\\.seq\\.[0-9]+$";

			ofs << prefix << "<rule_condition><![CDATA[\n" << parse_rule(rset->getString(1)) << "\n]]></rule_condition>\n"
				<< prefix << "<rule_file_name><![CDATA[\n" << parse_rule(rset->getString(2)) << "\n]]></rule_file_name>\n"
				<< prefix << "<pattern>" << pattern << "</pattern>\n"
				<< prefix << "<dst_dir>" << dst_dir << "</dst_dir>\n"
				<< prefix << "<options>";

			bool append = false;
			int is_internal = rset->getInt(4);
			if (is_internal & SA_OPTION_INTERNAL) {
				append = true;
				ofs << "INTERNAL";
			}
			if (is_internal & SA_OPTION_SAME_FILE) {
				if (append)
					ofs << " ";
				else
					append = true;
				ofs << "SAME_FILE";
			}
			if (is_internal & SA_OPTION_SPLIT) {
				if (append)
					ofs << " ";
				else
					append = true;
				ofs << "SPLIT";
			}

			ofs << "</options>\n";

			for (int i = 0; i < 3; i++) {
				int offset = i * 4 + 5;
				int serial = rset->getInt(offset);
				unsigned int delimit = static_cast<unsigned int>(rset->getString(offset + 1)[0]);
				int has_return = rset->getInt(offset + 2);
				int fixed = rset->getInt(offset + 3);

				if (serial == -1)
					continue;

				switch (i) {
				case 0:
					ofs << prefix << "<head>\n";
					break;
				case 1:
					ofs << prefix << "<body>\n";
					break;
				case 2:
					ofs << prefix << "<tail>\n";
					break;
				default:
					break;
				}
				prefix += '\t';

				if (delimit != 0)
					ofs << prefix << "<delimiter>" << delimit << "</delimiter>\n";

				ofs << prefix << "<options>";

				bool item_append = false;
				if (has_return) {
					append = true;
					ofs << "RETURN";
				}
				if (fixed) {
					if (item_append)
						ofs << " ";
					else
						append = true;
					ofs << "FIXED";
				}

				ofs << "</options>\n"
					<< prefix << "<fields>\n";
				prefix += '\t';

				load_distribute_item(file_type, serial);

				prefix.resize(prefix.size() - 1);
				ofs << prefix << "</fields>\n";

				prefix.resize(prefix.size() - 1);
				switch (i) {
				case 0:
					ofs << prefix << "</head>\n";
					break;
				case 1:
					ofs << prefix << "</body>\n";
					break;
				case 2:
					ofs << prefix << "</tail>\n";
					break;
				default:
					break;
				}
			}

			prefix.resize(prefix.size() - 1);
			ofs << prefix << "</item>\n";

			ofs << prefix << "<item>\n";
			prefix += '\t';
		}
#endif

		string dst_dir = rset->getString(3);
#if defined(USE_REMOTE)
		string rdst_dir = string("${SFTP_PREFIX}:${R") + dst_dir.substr(2);
#endif
		if (dst_dir == "${UNICAL_DT}/shm/public/${ACCT_CYCLE}/dat") {
#if !defined(USE_REMOTE)
			dst_dir = "${DBCDATA}/dat/admin";
#endif

			string::size_type pos1 = rule_file_name.find("\"");
			string::size_type pos2 = rule_file_name.find(".");
			if (pos1 == string::npos || pos2 == string::npos)
				pattern = "^.*$";
			else
				pattern = string("^") + string(rule_file_name.begin() + pos1 + 1, rule_file_name.begin() + pos2) + "\\.seq\\.[0-9]+$";
		} else {
			pattern = string("^[0-9]{10}${ACCT_CYCLE}[0-9]{8}") + file_type + "[0-9]*\\.B[0-9]$";
		}

		ofs << prefix << "<rule_condition><![CDATA[\n" << parse_rule(rset->getString(1)) << "\n]]></rule_condition>\n"
			<< prefix << "<rule_file_name><![CDATA[\n" << parse_rule(rset->getString(2)) << "\n]]></rule_file_name>\n"
			<< prefix << "<pattern>" << pattern << "</pattern>\n"
			<< prefix << "<dst_dir>" << dst_dir << "</dst_dir>\n"
#if defined(USE_REMOTE)
			<< prefix << "<rdst_dir>" << rdst_dir << "</rdst_dir>\n"
#endif
			<< prefix << "<options>";

		bool append = false;
		int is_internal = rset->getInt(4);
		if (is_internal & SA_OPTION_INTERNAL) {
			append = true;
			ofs << "INTERNAL";
		}
		if (is_internal & SA_OPTION_SAME_FILE) {
			if (append)
				ofs << " ";
			else
				append = true;
			ofs << "SAME_FILE";
		}
		if (is_internal & SA_OPTION_SPLIT) {
			if (append)
				ofs << " ";
			else
				append = true;
			ofs << "SPLIT";
		}

		ofs << "</options>\n";

		for (int i = 0; i < 3; i++) {
			int offset = i * 4 + 5;
			int serial = rset->getInt(offset);
			unsigned int delimit = static_cast<unsigned int>(rset->getString(offset + 1)[0]);
			int has_return = rset->getInt(offset + 2);
			int fixed = rset->getInt(offset + 3);

			if (serial == -1)
				continue;

			switch (i) {
			case 0:
				ofs << prefix << "<head>\n";
				break;
			case 1:
				ofs << prefix << "<body>\n";
				break;
			case 2:
				ofs << prefix << "<tail>\n";
				break;
			default:
				break;
			}
			prefix += '\t';

			if (delimit != 0)
				ofs << prefix << "<delimiter>" << delimit << "</delimiter>\n";

			ofs << prefix << "<options>";

			bool item_append = false;
			if (has_return) {
				append = true;
				ofs << "RETURN";
			}
			if (fixed) {
				if (item_append)
					ofs << " ";
				else
					append = true;
				ofs << "FIXED";
			}

			ofs << "</options>\n"
				<< prefix << "<fields>\n";
			prefix += '\t';

			load_distribute_item(file_type, serial);

			prefix.resize(prefix.size() - 1);
			ofs << prefix << "</fields>\n";

			prefix.resize(prefix.size() - 1);
			switch (i) {
			case 0:
				ofs << prefix << "</head>\n";
				break;
			case 1:
				ofs << prefix << "</body>\n";
				break;
			case 2:
				ofs << prefix << "</tail>\n";
				break;
			default:
				break;
			}
		}

		prefix.resize(prefix.size() - 1);
		ofs << prefix << "</item>\n";
	}

	if (!first) {
		prefix.resize(prefix.size() - 1);
		ofs << prefix << "</items>\n";

		prefix.resize(prefix.size() - 1);
		ofs << prefix << "</distribute>\n";
	}
}

void db2int::load_distribute_item(const string& file_type, int record_serial)
{
	auto_ptr<Generic_Statement> auto_stmt(db->create_statement());
	Generic_Statement *stmt = auto_stmt.get();

	auto_ptr<struct_dynamic> auto_data(db->create_data());
	struct_dynamic *data = auto_data.get();
	if (data == NULL)
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Memory allocation failure")).str(APPLOCALE));

	data->setSQL("select field_source{int}, field_serial{int}, field_type{int}, "
		"align_mode{int}, length{int}, precision{int}, decode(fill_code, 'S', ' ', fill_code) fill{char[1]}, "
		"action{int} "
		"from bps_dst_record "
		"where file_type = :file_type{char[4]} and record_serial = :record_serial{int}"
		"order by field_order");
	stmt->bind(data);

	stmt->setString(1, file_type);
	stmt->setInt(2, record_serial);

	auto_ptr<Generic_ResultSet> auto_rset(stmt->executeQuery());
	Generic_ResultSet *rset = auto_rset.get();

	while (rset->next()) {
		ofs << prefix << "<field>\n";
		prefix += '\t';

		int field_source = rset->getInt(1);
		int field_serial = rset->getInt(2);
		int field_type = rset->getInt(3);
		int align_mode = rset->getInt(4);
		int length = rset->getInt(5);
		int precision = rset->getInt(6);
		char fill = rset->getString(7)[0];
		int action = rset->getInt(8);
		map<int, string> *field_map;

		ofs << prefix << "<field_name>";

		switch (field_source) {
		case 0:
			ofs << "output.";
			field_map = &output_map;
			break;
		case 1:
			field_serial += FIXED_GLOBAL_SIZE;
			field_map = &global_map;
			break;
		case 2:
			field_map = &global_map;
			break;
		default:
			throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: unknown field_source {1}") % field_source).str(APPLOCALE));
		}

		map<int, string>::const_iterator iter = field_map->find(field_serial);
		if (iter == field_map->end())
			throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Can't find field_serial={1}, file_type={2}, record_serial={3}") % field_serial % file_type % record_serial).str(APPLOCALE));

		ofs << iter->second << "</field_name>\n";
		ofs << prefix << "<field_type>";

		switch (field_type) {
		case FIELD_TYPE_INT:
			ofs << "INT";
			break;
		case FIELD_TYPE_LONG:
			ofs << "LONG";
			break;
		case FIELD_TYPE_FLOAT:
			ofs << "FLOAT";
			break;
		case FIELD_TYPE_DOUBLE:
			ofs << "DOUBLE";
			break;
		case FIELD_TYPE_STRING:
			ofs << "STRING";
			break;
		default:
			throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: unknown field_type {1}") % field_type).str(APPLOCALE));
		}

		ofs << "</field_type>\n"
			<< prefix << "<align_mode>";
		switch (align_mode) {
		case ALIGN_MODE_LEFT:
			ofs << "LEFT";
			break;
		case ALIGN_MODE_INTERNAL:
			ofs << "INTERNAL";
			break;
		case ALIGN_MODE_RIGHT:
			ofs << "RIGHT";
			break;
		default:
			throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: unknown align_mode {1}") % align_mode).str(APPLOCALE));
		}

		ofs << "</align_mode>\n"
			<< prefix << "<field_size>"
			<< length
			<< "</field_size>\n"
			<< prefix << "<fill>"
			<< fill
			<< "</fill>\n";

		if (precision > 0) {
			ofs << prefix << "<precision>"
				<< precision
				<< "</precision>\n";
		}

		if (action != ACTION_NONE) {
			ofs << prefix << "<action>";

			switch (action) {
			case ADD_INT:
				ofs << "ADD_INT";
				break;
			case ADD_LONG:
				ofs << "ADD_LONG";
				break;
			case ADD_FLOAT:
				ofs << "ADD_FLOAT";
				break;
			case ADD_DOUBLE:
				ofs << "ADD_DOUBLE";
				break;
			case INC_INT:
				ofs << "INC_INT";
				break;
			case MIN_INT:
				ofs << "MIN_INT";
				break;
			case MIN_LONG:
				ofs << "MIN_LONG";
				break;
			case MIN_FLOAT:
				ofs << "MIN_FLOAT";
				break;
			case MIN_DOUBLE:
				ofs << "MIN_DOUBLE";
				break;
			case MIN_STR:
				ofs << "MIN_STR";
				break;
			case MAX_INT:
				ofs << "MAX_INT";
				break;
			case MAX_LONG:
				ofs << "MAX_LONG";
				break;
			case MAX_FLOAT:
				ofs << "MAX_FLOAT";
				break;
			case MAX_DOUBLE:
				ofs << "MAX_DOUBLE";
				break;
			case MAX_STR:
				ofs << "MAX_STR";
				break;
			case SPLIT_FIELD:
				ofs << "SPLIT_FIELD";
				break;
			default:
				throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: unknown action {1}") % action).str(APPLOCALE));
			}

			ofs << "</action>\n";
		}

		prefix.resize(prefix.size() - 1);
		ofs << prefix << "</field>\n";
	}
}

bool db2int::in_distribute_map(const string& file_type, const string& rule_file_name)
{
	for (vector<distribute_item_t>::const_iterator iter = distribute_items.begin(); iter!= distribute_items.end(); iter++) {
		if (file_type == iter->file_type
			&& rule_file_name.find(iter->distribute_rule) != string::npos) {
			return true;
		}
	}

	return false;
}

void db2int::load_process(const string& file_type)
{
	auto_ptr<Generic_Statement> auto_stmt(db->create_statement());
	Generic_Statement *stmt = auto_stmt.get();

	auto_ptr<struct_dynamic> auto_data(db->create_data());
	struct_dynamic *data = auto_data.get();
	if (data == NULL)
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Memory allocation failure")).str(APPLOCALE));

	data->setSQL("select record_serial{int}, rule_process{char[4000]} "
		"from bps_process "
		"where file_type = :file_type{char[4]} and rule_process is not null "
		"order by record_serial, prior_level, field_serial");
	stmt->bind(data);

	stmt->setString(1, file_type);

	auto_ptr<Generic_ResultSet> auto_rset(stmt->executeQuery());
	Generic_ResultSet *rset = auto_rset.get();
	int record_serial = -1;
	string rule_process;

	while (rset->next()) {
		if (record_serial != rset->getInt(1)) {
			if (record_serial == -1) {
				ofs << prefix << "<processes>\n";
				prefix += '\t';
			} else {
				ofs << prefix << "<process>\n";
				prefix += '\t';

				ofs << prefix << "<rule><![CDATA[\n" << parse_rule(rule_process) << "\n]]></rule>\n";

				prefix.resize(prefix.size() - 1);
				ofs << prefix << "</process>\n";

				rule_process = "";
			}

			record_serial = rset->getInt(1);
		}

		if (!rule_process.empty())
			rule_process += "\n";
		rule_process += rset->getString(2);
	}

	if (record_serial != -1) {
		ofs << prefix << "<process>\n";
		prefix += '\t';

		ofs << prefix << "<rule><![CDATA[\n" << parse_rule(rule_process) << "\n]]></rule>\n";

		prefix.resize(prefix.size() - 1);
		ofs << prefix << "</process>\n";

		prefix.resize(prefix.size() - 1);
		ofs << prefix << "</processes>\n";
	}
}

void db2int::load_record_flag()
{
	auto_ptr<Generic_Statement> auto_stmt(db->create_statement());
	Generic_Statement *stmt = auto_stmt.get();

	auto_ptr<struct_dynamic> auto_data(db->create_data());
	struct_dynamic *data = auto_data.get();
	if (data == NULL)
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Memory allocation failure")).str(APPLOCALE));

	data->setSQL("select thread_id{char[10]}, para_name{char[32]}, para_contents{char[1000]} "
		"from sys_thread_para where para_name in ('record_flag', 'file_type') "
		"order by thread_id, para_name");
	stmt->bind(data);

	auto_ptr<Generic_ResultSet> auto_rset(stmt->executeQuery());
	Generic_ResultSet *rset = auto_rset.get();
	string thread_id;
	int record_flag = 0;
	string file_type;

	while (rset->next()) {
		if (thread_id != rset->getString(1)) {
			if (!thread_id.empty()) {
				record_flag_map[file_type] = record_flag;
				record_flag = 0;
				file_type = "";
			}
			thread_id = rset->getString(1);
		}

		string para_name = rset->getString(2);
		string para_value = rset->getString(3);

		if (para_name == "record_flag")
			record_flag = atoi(para_value.c_str());
		else
			file_type = para_value;
	}

	if (!thread_id.empty())
		record_flag_map[file_type] = record_flag;
}

string db2int::parse_rule(const string& src_code)
{
	string dst_code = "";

	for (string::const_iterator iter = src_code.begin(); iter != src_code.end();) {
		if (*iter == '\\') {
			// This charactor and next are a whole.
			dst_code += *iter;
			++iter;
			dst_code += *iter;
			++iter;
		} else if (*iter == '\"') {
			// Const string.
			dst_code += *iter;
			++iter;
			while (iter != src_code.end()) {
				// Skip const string.
				dst_code += *iter;
				++iter;
				if (*(iter-1) == '\"' && *(iter-2) != '\\')
					break;
			}
		} else if (*iter == '/' && (iter  + 1) != src_code.end() && *(iter + 1) == '*') {
			// Multi Line Comments
			dst_code += *iter;
			++iter;
			dst_code += *iter;
			++iter;
			while (iter != src_code.end()) {
				// Skip comment string.
				dst_code += *iter;
				++iter;
				if (*(iter - 1) == '/' && *(iter - 2) == '*')
					break;
			}
		} else if (*iter == '$') {
			// Input variables
			iter++;
			if (*iter >= '0' && *iter <= '9') {
				dst_code += "$output.";
				int field_serial = 0;
				for (; iter != src_code.end(); ++iter) {
					if (*iter >= '0' && *iter <= '9')
						field_serial = field_serial * 10 + *iter - '0';
					else
						break;
				}

				map<int, string>::const_iterator oiter = output_map.find(field_serial);
				if (oiter == output_map.end())
					throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Can't find field_serial={1}") % field_serial).str(APPLOCALE));

				dst_code += oiter->second;
			} else if (isalnum(*iter) || *iter == '_') {
				dst_code += "$input.";
				for (; iter != src_code.end(); ++iter) {
					if (isalnum(*iter) || *iter == '_')
						dst_code += tolower(*iter);
					else
						break;
				}
			}
		} else if (*iter == '#') {
			// Output variables
			iter++;
			if (*iter >= '0' && *iter <= '9') {
				dst_code += "#";
				for (; iter != src_code.end(); ++iter) {
					if (*iter >= '0' && *iter <= '9')
						dst_code += *iter;
					else
						break;
				}
			} else if (isalnum(*iter) || *iter == '_') {
				dst_code += "#output.";
				for (; iter != src_code.end(); ++iter) {
					if (isalnum(*iter) || *iter == '_')
						dst_code += tolower(*iter);
					else
						break;
				}
			}
		} else if (isalpha(*iter) || *iter == '_') {
			// Key word.
			string token = "";
			token += *iter;
			for (++iter; iter != src_code.end(); ++iter) {
				if (isalnum(*iter) || *iter == '_')
					token += *iter;
				else
					break;
			}

			if (token == "record_sn")
				dst_code += "$record_sn";
			else
				dst_code += token;
		} else {
			// Other case
			dst_code += *iter;
			++iter;
		}
	}

	return dst_code;
}

}
}

using namespace ai::app;

int main(int argc, char **argv)
{
	try {
		db2int instance;

		instance.run(argc, argv);
		return 0;
	} catch (exception& ex) {
		cout << ex.what() << std::endl;
		return 1;
	}
}

