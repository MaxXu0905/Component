#include "sa_struct.h"
#include "db2cal.h"
#include "calp_ctx.h"
#include "cal_struct.h"

namespace bf = boost::filesystem;
namespace po = boost::program_options;
namespace bp = boost::posix_time;
namespace bc = boost::chrono;
using namespace ai::scci;
using namespace ai::sg;
using namespace std;

typedef boost::tokenizer<boost::char_separator<char> > tokenizer;

namespace ai
{
namespace app
{

db2cal::db2cal()
{
	db = NULL;
}

db2cal::~db2cal()
{
	delete db;
}

void db2cal::run(int argc, char **argv)
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
		("stage,S", po::value<int>(&stage)->required(), (_("specify stage to generate")).str(APPLOCALE).c_str())
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

	load_para();
	load_para_default();

	auto_ptr<Generic_Statement> auto_stmt(db->create_statement());
	Generic_Statement *stmt = auto_stmt.get();

	auto_ptr<struct_dynamic> auto_data(db->create_data());
	struct_dynamic *data = auto_data.get();
	if (data == NULL)
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Memory allocation failure")).str(APPLOCALE));

	data->setSQL("select distinct file_type{char[4]} from unical_src_file");
	stmt->bind(data);

	auto_ptr<Generic_ResultSet> auto_rset(stmt->executeQuery());
	Generic_ResultSet *rset = auto_rset.get();

	while (rset->next()) {
		string file_type = rset->getString(1);
		string thread_id = get_thread_id(file_type);
		if (thread_id.empty())
			continue;

		try {
			string output_file = prefix_file + "_" + file_type + boost::lexical_cast<string>(stage) + ".xml";
			ofs.open(output_file.c_str());
			if (!ofs)
				throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Can't open file {1}") % output_file).str(APPLOCALE));

			if (server_flag) {
				ofs << "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
					<< "<cal xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:noNamespaceSchemaLocation=\"cal.xsd\">\n";

				prefix = "\t";
				load_service(file_type, thread_id);

				ofs << "</cal>\n";
			} else {
				ofs << "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
					<< "<integrator xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:noNamespaceSchemaLocation=\"integrator.xsd\">\n"
					<< "\t<resource>\n"
					<< "\t\t<integrator_name>" << file_type << stage << "</integrator_name>\n"
					<< "\t\t<openinfo>${OPENINFO}</openinfo>\n"
					<< "\t\t<libs>libsqloracle.so libcal.so</libs>\n"
					<< "\t\t<sginfo/>\n"
					<< "\t\t<dbcinfo>${DBCINFO}</dbcinfo>\n"
					<< "\t</resource>\n";

				prefix = "\t";
				load_global(file_type);

				ofs << "\t<adaptors>\n"
					<< "\t\t<adaptor>\n";

				prefix = "\t\t\t";
				load_adaptor(file_type, thread_id);

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

void db2cal::load_service(const string& file_type, const string& thread_id)
{
	load_shm_tables();
	load_resource(file_type, thread_id);
	load_tables(thread_id);
	load_aliases();
	load_input(file_type);
	load_output(file_type);
}

void db2cal::load_adaptor(const string& file_type, const string& thread_id)
{
	load_resource(file_type, thread_id);
	load_source(file_type, thread_id);
	load_invalid(file_type, thread_id);
	load_error(file_type, thread_id);
	load_input(file_type);
	load_output(file_type);
	load_distribute(file_type, thread_id);
}

void db2cal::load_resource(const string& file_type, const string& thread_id)
{
	ofs << prefix << "<resource>\n";
	prefix += "\t";

	if (server_flag) {
		ofs << prefix << "<openinfo>${CALOPENINFO}</openinfo>\n"
			<< prefix << "<province_code>" << get_value(thread_id, "province_code") << "</province_code>\n"
			<< prefix << "<driver_data>" << get_value(thread_id, "driver_data") << "</driver_data>\n"
			<< prefix << "<stage>" << stage << "</stage>\n";

		string collision_level_str = get_value(thread_id, "collision_level");
		int collision_level = atoi(collision_level_str.c_str());
		if (collision_level == COLLISION_LEVEL_SOURCE || collision_level == COLLISION_LEVEL_ALL) {
			ofs << prefix << "<collision_level>";
			if (collision_level == COLLISION_LEVEL_SOURCE)
				ofs << "SOURCE";
			else
				ofs << "ALL";
			ofs << "</collision_level>\n";
		}

		string channel_level = get_value(thread_id, "channel_level");
		if (channel_level == "1")
			ofs << prefix << "<channel_level>Y</channel_level>\n";

		string cycle_type = get_value(thread_id, "cycle_type");
		if (cycle_type == "D" || cycle_type == "M") {
			ofs << prefix << "<cycle_type>";
			if (cycle_type == "D")
				ofs << "DAY";
			else
				ofs << "MONTH";
			ofs << "</cycle_type>\n";
		}

		ofs << prefix << "<busi_limit_month>" << get_value(thread_id, "busi_limit_month") << "</busi_limit_month>\n";

		if (stage == 1) {
			ofs << prefix << "<rule_dup><![CDATA[\n"
				<< "strcpy(#0, \"dup_UNICAL\");\n"
				<< "sprintf(#1, \"%s,%s\", file_name, $record_sn);\n"
				<< "sprintf(#2, \"%s,%s,%s,%s\", $output.acct_no, $output.channel_id, $output.mod_id, $output.rule_id);\n"
				<< "]]></rule_dup>\n";
		}
	} else {
		ofs << prefix << "<com_key>CAL</com_key>\n"
			<< prefix << "<svc_key>" << file_type << stage << "</svc_key>\n"
			<< prefix << "<batch>1</batch>\n"
			<< prefix << "<concurrency>1</concurrency>\n"
			<< prefix << "<disable_global>N</disable_global>\n";
	}

	prefix.resize(prefix.size() - 1);
	ofs << prefix << "</resource>\n";
}

void db2cal::load_source(const string& file_type, const string& thread_id)
{
	ofs << prefix << "<source>\n";
	prefix += '\t';

	ofs << prefix << "<class_name>sa_ivariable</class_name>\n"
		<< prefix << "<per_records>100000</per_records>\n"
		<< prefix << "<args>\n";
	prefix += '\t';

	ofs << prefix << "<arg>\n";
	prefix += '\t';

	ofs << prefix << "<name>src_dir</name>\n"
		<< prefix << "<value>" << get_value(thread_id, "src_dir") << "</value>\n";

	prefix.resize(prefix.size() - 1);
	ofs << prefix << "</arg>\n"
		<< prefix << "<arg>\n";
	prefix += '\t';

	ofs << prefix << "<name>dup_dir</name>\n"
		<< prefix << "<value>" << get_value(thread_id, "dup_dir") << "</value>\n";

	prefix.resize(prefix.size() - 1);
	ofs << prefix << "</arg>\n"
		<< prefix << "<arg>\n";
	prefix += '\t';

	ofs << prefix << "<name>bak_dir</name>\n"
		<< prefix << "<value>" << get_value(thread_id, "bak_dir") << "</value>\n";

	prefix.resize(prefix.size() - 1);
	ofs << prefix << "</arg>\n"
		<< prefix << "<arg>\n";
	prefix += '\t';

	ofs << prefix << "<name>tmp_dir</name>\n"
		<< prefix << "<value>" << get_value(thread_id, "tmp_dir") << "</value>\n";

	prefix.resize(prefix.size() - 1);
	ofs << prefix << "</arg>\n"
		<< prefix << "<arg>\n";
	prefix += '\t';

	string ftl_dir = get_value(thread_id, "tmp_dir");
	ftl_dir.resize(ftl_dir.size() - 3);
	ftl_dir += "ftl";

	ofs << prefix << "<name>ftl_dir</name>\n"
		<< prefix << "<value>" << ftl_dir << "</value>\n";

	prefix.resize(prefix.size() - 1);
	ofs << prefix << "</arg>\n"
		<< prefix << "<arg>\n";
	prefix += '\t';

	ofs << prefix << "<name>pattern</name>\n"
		<< prefix << "<value>"<< get_value(thread_id, "pattern") << "</value>\n";

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
	ofs << prefix << "</arg>\n";

	prefix.resize(prefix.size() - 1);
	ofs << prefix << "</args>\n";

	prefix.resize(prefix.size() - 1);
	ofs << prefix << "</source>\n";
}

void db2cal::load_invalid(const string& file_type, const string& thread_id)
{
	ofs << prefix << "<invalid>\n";
	prefix += '\t';

	ofs << prefix << "<class_name>sa_osource</class_name>\n"
		<< prefix << "<args>\n";
	prefix += '\t';

	ofs << prefix << "<arg>\n";
	prefix += '\t';

	ofs << prefix << "<name>directory</name>\n"
		<< prefix << "<value>" << get_value(thread_id, "inv_dir") << "</value>\n";

	prefix.resize(prefix.size() - 1);
	ofs << prefix << "</arg>\n";

	prefix.resize(prefix.size() - 1);
	ofs << prefix << "</args>\n";

	prefix.resize(prefix.size() - 1);
	ofs << prefix << "</invalid>\n";
}

void db2cal::load_error(const string& file_type, const string& thread_id)
{
	ofs << prefix << "<error>\n";
	prefix += '\t';

	ofs << prefix << "<class_name>sa_osource</class_name>\n"
		<< prefix << "<args>\n";
	prefix += '\t';

	ofs << prefix << "<arg>\n";
	prefix += '\t';

	ofs << prefix << "<name>directory</name>\n"
		<< prefix << "<value>" << get_value(thread_id, "err_dir") << "</value>\n";

	prefix.resize(prefix.size() - 1);
	ofs << prefix << "</arg>\n";

	prefix.resize(prefix.size() - 1);
	ofs << prefix << "</args>\n";

	prefix.resize(prefix.size() - 1);
	ofs << prefix << "</error>\n";
}

void db2cal::load_global(const string& file_type)
{
	ofs << prefix << "<global>\n";
	prefix += '\t';

	ofs << prefix << "<fields>\n";
	prefix += '\t';

	ofs << prefix << "<field>\n";
	prefix += '\t';

	if (stage == 1) {
		ofs << prefix << "<field_name>is_accumulate</field_name>\n";
		ofs << prefix << "<field_size>1</field_size>\n";
		ofs << prefix << "<default_value>0</default_value>\n";

		prefix.resize(prefix.size() - 1);
		ofs << prefix << "</field>\n";

		ofs << prefix << "<field>\n";
		prefix += '\t';

		ofs << prefix << "<field_name>" << BUSI_CODE << "</field_name>\n";
		ofs << prefix << "<field_size>" << BUSI_CODE_LEN << "</field_size>\n";

		prefix.resize(prefix.size() - 1);
		ofs << prefix << "</field>\n";

		ofs << prefix << "<field>\n";
		prefix += '\t';

		ofs << prefix << "<field_name>" << BUSI_VALUE << "</field_name>\n";
		ofs << prefix << "<field_size>" << BUSI_VALUE_LEN << "</field_size>\n";

		prefix.resize(prefix.size() - 1);
		ofs << prefix << "</field>\n";

		ofs << prefix << "<field>\n";
		prefix += '\t';

		ofs << prefix << "<field_name>busi_count</field_name>\n";
		ofs << prefix << "<field_size>1</field_size>\n";
		ofs << prefix << "<default_value>1</default_value>\n";
	} else {
		ofs << prefix << "<field_name>pay_return</field_name>\n";
		ofs << prefix << "<field_size>1</field_size>\n";
		ofs << prefix << "</field>\n";

		ofs << prefix << "<field>\n";
		ofs << prefix << "<field_name>is_limit_flag</field_name>\n";
		ofs << prefix << "<field_size>1</field_size>\n";
	}

	prefix.resize(prefix.size() - 1);
	ofs << prefix << "</field>\n";

	prefix.resize(prefix.size() - 1);
	ofs << prefix << "</fields>\n";

	prefix.resize(prefix.size() - 1);
	ofs << prefix << "</global>\n";
}

void db2cal::load_input(const string& file_type)
{
	auto_ptr<Generic_Statement> auto_stmt(db->create_statement());
	Generic_Statement *stmt = auto_stmt.get();

	auto_ptr<struct_dynamic> auto_data(db->create_data());
	struct_dynamic *data = auto_data.get();
	if (data == NULL)
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Memory allocation failure")).str(APPLOCALE));

	string sql_str;
	if (stage == 1) {
		sql_str = "select factor1{char[10]}, factor2{int}, lower(field_name){char[63]}, field_type{int} "
			"from unical_src_record "
			"where file_type = :file_type{char[4]} "
			"order by field_serial";
	} else {
		sql_str = "select to_char(length){char[10]}, length{int}, lower(field_name){char[63]}, field_type{int} "
			"from unical_dst_record "
			"where file_type = :file_type{char[4]} and use_default = 0 "
			"order by field_order";
	}

	data->setSQL(sql_str);
	stmt->bind(data);

	stmt->setString(1, file_type);

	auto_ptr<Generic_ResultSet> auto_rset(stmt->executeQuery());
	Generic_ResultSet *rset = auto_rset.get();

	ofs << prefix << "<input>\n";
	prefix += '\t';

	if (!server_flag) {
		ofs << prefix << "<table>\n";
		prefix += '\t';
		ofs << prefix << "<name>input</name>\n"
			<< prefix << "<delimiter>1</delimiter>\n";
	}

	ofs << prefix << "<fields>\n";
	prefix += '\t';

	while (rset->next()) {
		string field_name = rset->getString(3);
		if (stage == 3 && (field_name == FREEZE_EXPIRE || field_name == CALC_TRACK))
			continue;

		ofs << prefix << "<field>\n";
		prefix += '\t';

		if (server_flag) {
			int field_type = rset->getInt(4);
			ofs << prefix << "<field_type>";

			switch (field_type) {
			case 0:
				ofs << "CHAR";
				break;
			case 1:
				ofs << "SHORT";
				break;
			case 2:
				ofs << "INT";
				break;
			case 3:
				ofs << "LONG";
				break;
			case 4:
				ofs << "FLOAT";
				break;
			case 5:
				ofs << "DOUBLE";
				break;
			case 6:
				ofs << "STRING";
				break;
			default:
				throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: unknown field_type {1}") % field_type).str(APPLOCALE));
			}

			ofs << "</field_type>\n"
				<< prefix << "<field_name>" << field_name << "</field_name>\n";
		} else {
			ofs << prefix << "<factor1>" << rset->getString(1) << "</factor1>\n"
				<< prefix << "<factor2>" << rset->getInt(2) << "</factor2>\n"
				<< prefix << "<field_name>" << field_name << "</field_name>\n";
		}

		prefix.resize(prefix.size() - 1);
		ofs << prefix << "</field>\n";
	}

	prefix.resize(prefix.size() - 1);
	ofs << prefix << "</fields>\n";

	if (!server_flag) {
		prefix.resize(prefix.size() - 1);
		ofs << prefix << "</table>\n";
	}

	prefix.resize(prefix.size() - 1);
	ofs << prefix << "</input>\n";
}

void db2cal::load_output(const string& file_type)
{
	auto_ptr<Generic_Statement> auto_stmt(db->create_statement());
	Generic_Statement *stmt = auto_stmt.get();

	auto_ptr<struct_dynamic> auto_data(db->create_data());
	struct_dynamic *data = auto_data.get();
	if (data == NULL)
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Memory allocation failure")).str(APPLOCALE));

	string sql_str = "select lower(field_name){char[63]}, length{int}, use_default{int}, default_value{char[4000]} "
		"from unical_dst_record "
		"where file_type = :file_type{char[4]} ";
	if (stage == 3)
		sql_str += "and out_flag = 1 ";
	else if (!server_flag)
		sql_str += "and use_default = 0 ";
	sql_str += "order by field_order";
	data->setSQL(sql_str);
	stmt->bind(data);

	stmt->setString(1, file_type);

	auto_ptr<Generic_ResultSet> auto_rset(stmt->executeQuery());
	Generic_ResultSet *rset = auto_rset.get();

	ofs << prefix << "<output>\n";
	prefix += '\t';

	if (!server_flag) {
		ofs << prefix << "<table>\n";
		prefix += '\t';
		ofs << prefix << "<name>output</name>\n";
	}

	ofs << prefix << "<fields>\n";
	prefix += '\t';

	bool has_channel_id = false;
	bool has_acct_no = false;

	while (rset->next()) {
		string field_name = rset->getString(1);

		if (stage != 3) {
			if (field_name == FREEZE_EXPIRE || field_name == CALC_TRACK)
				continue;

			if (field_name == CHANNEL_ID)
				has_channel_id = true;
			else if (field_name == ACCT_NO)
				has_acct_no = true;
		}

		if (server_flag) {
			int use_default = rset->getInt(3);
			if (use_default && (field_name == CHANNEL_ID || field_name == ACCT_NO || stage == 3)) {
				ofs << prefix << "<field>\n";
				prefix += '\t';

				ofs << prefix << "<field_name>"<< field_name << "</field_name>\n"
					<< prefix << "<default_value><![CDATA[" << rset->getString(4) << "]]></default_value>\n";

				prefix.resize(prefix.size() - 1);
				ofs << prefix << "</field>\n";
			}
		} else {
			ofs << prefix << "<field>\n";
			prefix += '\t';

			ofs << prefix << "<field_name>"<< field_name << "</field_name>\n"
				<< prefix << "<field_size>" << rset->getInt(2) << "</field_size>\n";

			prefix.resize(prefix.size() - 1);
			ofs << prefix << "</field>\n";
		}
	}

	if (!server_flag && stage != 3) {
		if (!has_channel_id) {
			ofs << prefix << "<field>\n";
			prefix += '\t';

			ofs << prefix << "<field_name>" << CHANNEL_ID << "</field_name>\n"
				<< prefix << "<field_size>" << CHANNEL_ID_LEN << "</field_size>\n";

			prefix.resize(prefix.size() - 1);
			ofs << prefix << "</field>\n";
		}

		if (!has_acct_no) {
			ofs << prefix << "<field>\n";
			prefix += '\t';

			ofs << prefix << "<field_name>" << ACCT_NO << "</field_name>\n"
				<< prefix << "<field_size>" << ACCT_NO_LEN << "</field_size>\n";

			prefix.resize(prefix.size() - 1);
			ofs << prefix << "</field>\n";
		}
	}

	prefix.resize(prefix.size() - 1);
	ofs << prefix << "</fields>\n";

	if (!server_flag) {
		prefix.resize(prefix.size() - 1);
		ofs << prefix << "</table>\n";
	}

	prefix.resize(prefix.size() - 1);
	ofs << prefix << "</output>\n";
}

void db2cal::load_distribute(const string& file_type, const string& thread_id)
{
	ofs << prefix << "<distribute>\n";
	prefix += '\t';

	ofs << prefix << "<class_name>sa_ostream</class_name>\n"
		<< prefix << "<max_open_files>1000</max_open_files>\n"
		<< prefix << "<items>\n";
	prefix += '\t';

	if (stage == 1)
		load_distribute1(file_type, thread_id);
	else
		load_distribute3(file_type, thread_id);

	prefix.resize(prefix.size() - 1);
	ofs << prefix << "</items>\n";

	prefix.resize(prefix.size() - 1);
	ofs << prefix << "</distribute>\n";
}

void db2cal::load_distribute1(const string& file_type, const string& thread_id)
{
	string pattern = "^.*$";

	ofs << prefix << "<item>\n";
	prefix += '\t';

	ofs << prefix << "<rule_condition><![CDATA[\nif (is_accumulate[0] == '0')\n\treturn 0;\nelse\n\treturn -1;\n]]></rule_condition>\n"
		<< prefix << "<rule_file_name><![CDATA[\n"
		<< "sprintf(#0, \"%.32sC%s.%09d.%09d\", file_name, file_name + 33, atoi($output.mod_id), atoi($output.rule_id));\n";

	ofs << "]]></rule_file_name>\n"
		<< prefix << "<pattern>" << pattern << "</pattern>\n"
		<< prefix << "<dst_dir>" << get_value(thread_id, "dst_dir") << "</dst_dir>\n";

	ofs << prefix << "<body>\n";
	prefix += '\t';

	ofs << prefix << "<delimiter>1</delimiter>\n";

	ofs << prefix << "<fields>\n";
	prefix += '\t';

	load_distribute_item(file_type);

	prefix.resize(prefix.size() - 1);
	ofs << prefix << "</fields>\n";

	prefix.resize(prefix.size() - 1);
	ofs << prefix << "</body>\n";

	prefix.resize(prefix.size() - 1);
	ofs << prefix << "</item>\n";

	for (int i = 0; i < 2; i++) {
		ofs << prefix << "<item>\n";
		prefix += '\t';

		string pattern;
		if (i == 0) {
			pattern = string("^[0-9]{10}${ACCT_CYCLE}") + file_type + ".F0$";
			ofs << prefix << "<rule_condition><![CDATA[\nif (is_accumulate[0] == '1')\n\treturn 0;\nelse\n\treturn -1;\n]]></rule_condition>\n";
		} else {
			pattern = string("^[0-9]{10}${ACCT_CYCLE}") + file_type + ".F1$";
			ofs << prefix << "<rule_condition><![CDATA[\nif (is_accumulate[0] == '2')\n\treturn 0;\nelse\n\treturn -1;\n]]></rule_condition>\n";
		}

		ofs << prefix << "<rule_file_name><![CDATA[\n";

		if (i == 0)
			ofs << "sprintf(#0, \"%010d${ACCT_CYCLE}" << file_type << ".F0\", atoi(file_sn));\n";
		else
			ofs << "sprintf(#0, \"%010d${ACCT_CYCLE}" << file_type << ".F1\", atoi(file_sn));\n";

		ofs << "]]></rule_file_name>\n"
			<< prefix << "<pattern>" << pattern << "</pattern>\n"
			<< prefix << "<dst_dir>" << get_value(thread_id, "busi_dir") << "</dst_dir>\n";

		ofs << prefix << "<body>\n";
		prefix += '\t';

		ofs << prefix << "<delimiter>44</delimiter>\n";

		ofs << prefix << "<fields>\n";
		prefix += '\t';

		if (i == 1) {
			ofs << prefix << "<field>\n";
			prefix += '\t';

			ofs << prefix << "<field_name>output.acct_no</field_name>\n";
			ofs << prefix << "<field_type>STRING</field_type>\n";
			ofs << prefix << "<align_mode>LEFT</align_mode>\n";
			ofs << prefix << "<field_size>" << ACCT_NO_LEN << "</field_size>\n";

			prefix.resize(prefix.size() - 1);
			ofs << prefix << "</field>\n";
		}

		ofs << prefix << "<field>\n";
		prefix += '\t';

		ofs << prefix << "<field_name>output.channel_id</field_name>\n";
		ofs << prefix << "<field_type>STRING</field_type>\n";
		ofs << prefix << "<align_mode>LEFT</align_mode>\n";
		ofs << prefix << "<field_size>" << CHANNEL_ID_LEN << "</field_size>\n";

		prefix.resize(prefix.size() - 1);
		ofs << prefix << "</field>\n";

		ofs << prefix << "<field>\n";
		prefix += '\t';

		ofs << prefix << "<field_name>output.mod_id</field_name>\n";
		ofs << prefix << "<field_type>STRING</field_type>\n";
		ofs << prefix << "<align_mode>LEFT</align_mode>\n";
		ofs << prefix << "<field_size>" << MOD_ID_LEN<< "</field_size>\n";

		prefix.resize(prefix.size() - 1);
		ofs << prefix << "</field>\n";

		ofs << prefix << "<field>\n";
		prefix += '\t';

		ofs << prefix << "<field_name>output.rule_id</field_name>\n";
		ofs << prefix << "<field_type>STRING</field_type>\n";
		ofs << prefix << "<align_mode>LEFT</align_mode>\n";
		ofs << prefix << "<field_size>" << RULE_ID_LEN << "</field_size>\n";

		prefix.resize(prefix.size() - 1);
		ofs << prefix << "</field>\n";

		ofs << prefix << "<field>\n";
		prefix += '\t';

		ofs << prefix << "<field_name>busi_code</field_name>\n";
		ofs << prefix << "<field_type>STRING</field_type>\n";
		ofs << prefix << "<align_mode>LEFT</align_mode>\n";
		ofs << prefix << "<field_size>" << BUSI_CODE_LEN << "</field_size>\n";

		prefix.resize(prefix.size() - 1);
		ofs << prefix << "</field>\n";

		ofs << prefix << "<field>\n";
		prefix += '\t';

		ofs << prefix << "<field_name>busi_value</field_name>\n";
		ofs << prefix << "<field_type>STRING</field_type>\n";
		ofs << prefix << "<align_mode>LEFT</align_mode>\n";
		ofs << prefix << "<field_size>" << BUSI_VALUE_LEN << "</field_size>\n";

		prefix.resize(prefix.size() - 1);
		ofs << prefix << "</field>\n";

		ofs << prefix << "<field>\n";
		prefix += '\t';

		ofs << prefix << "<field_name>busi_count</field_name>\n";
		ofs << prefix << "<field_type>STRING</field_type>\n";
		ofs << prefix << "<align_mode>LEFT</align_mode>\n";
		ofs << prefix << "<field_size>1</field_size>\n";

		prefix.resize(prefix.size() - 1);
		ofs << prefix << "</field>\n";

		prefix.resize(prefix.size() - 1);
		ofs << prefix << "</fields>\n";

		prefix.resize(prefix.size() - 1);
		ofs << prefix << "</body>\n";

		prefix.resize(prefix.size() - 1);
		ofs << prefix << "</item>\n";
	}
}

void db2cal::load_distribute3(const string& file_type, const string& thread_id)
{
	string pattern = "^.*$";

	for (int i = 0; i < 3; i++) {
		ofs << prefix << "<item>\n";
		prefix += '\t';

		ofs << prefix << "<rule_condition><![CDATA[\n";

		switch (i) {
		case 0:
			ofs << "if (pay_return[0] == '0' || pay_return[0] == '2')\n\treturn 0;\nelse\n\treturn -1;";
			break;
		case 1:
			ofs << "if (pay_return[0] == '1' || pay_return[0] == '2')\n\treturn 0;\nelse\n\treturn -1;";
			break;
		default:
			ofs << "if (is_limit_flag[0] == '1' && strcmp($output.calc_cycle, $output.sett_cycle) == 0)\n\treturn 0;\nelse\n\treturn -1;";
			break;
		}

		ofs << "\n]]></rule_condition>\n";
		ofs << prefix << "<rule_file_name><![CDATA[\n";
		if (i<2) 
			ofs << "sprintf(#0, \"%.32s%c%s\", file_name, static_cast<char>('D' + " << i << "), file_name + 33);\n";
		else
			ofs << "sprintf(#0, \"%010d${ACCT_CYCLE}" << file_type << ".F2\", atoi(file_sn));\n";

		ofs << "]]></rule_file_name>\n"
			<< prefix << "<pattern>" << pattern << "</pattern>\n";

		switch (i) {
		case 0:
			ofs << prefix << "<dst_dir>" << get_value(thread_id, "dst_dir") << "</dst_dir>\n";
			break;
		case 1:
			ofs << prefix << "<dst_dir>" << get_value(thread_id, "freeze_dir") << "</dst_dir>\n";
			break;
		default:
			ofs << prefix << "<dst_dir>" << get_value(thread_id, "busi_dir") << "</dst_dir>\n";
			break;
		}

		ofs << prefix << "<body>\n";
		prefix += '\t';

		if (i<2)
			ofs << prefix << "<delimiter>1</delimiter>\n";
		else
			ofs << prefix << "<delimiter>44</delimiter>\n";

		ofs << prefix << "<fields>\n";
		prefix += '\t';

		if (i < 2) {
			load_distribute_item(file_type);
		} else {
			ofs << prefix << "<field>\n";
			prefix += '\t';

			ofs << prefix << "<field_name>input.accu_chnl_id</field_name>\n";
			ofs << prefix << "<field_type>STRING</field_type>\n";
			ofs << prefix << "<align_mode>LEFT</align_mode>\n";
			ofs << prefix << "<field_size>" << CHANNEL_ID_LEN << "</field_size>\n";

			prefix.resize(prefix.size() - 1);
			ofs << prefix << "</field>\n";

			ofs << prefix << "<field>\n";
			prefix += '\t';

			ofs << prefix << "<field_name>output.pay_chnl_id</field_name>\n";
			ofs << prefix << "<field_type>STRING</field_type>\n";
			ofs << prefix << "<align_mode>LEFT</align_mode>\n";
			ofs << prefix << "<field_size>" << CHANNEL_ID_LEN << "</field_size>\n";

			prefix.resize(prefix.size() - 1);
			ofs << prefix << "</field>\n";

			ofs << prefix << "<field>\n";
			prefix += '\t';

			ofs << prefix << "<field_name>output.comm_type</field_name>\n";
			ofs << prefix << "<field_type>STRING</field_type>\n";
			ofs << prefix << "<align_mode>LEFT</align_mode>\n";
			ofs << prefix << "<field_size>" << COMM_TYPE_LEN<< "</field_size>\n";

			prefix.resize(prefix.size() - 1);
			ofs << prefix << "</field>\n";

			ofs << prefix << "<field>\n";
			prefix += '\t';

			ofs << prefix << "<field_name>output.calc_cycle</field_name>\n";
			ofs << prefix << "<field_type>STRING</field_type>\n";
			ofs << prefix << "<align_mode>LEFT</align_mode>\n";
			ofs << prefix << "<field_size>" << CALC_CYCLE_LEN<< "</field_size>\n";

			prefix.resize(prefix.size() - 1);
			ofs << prefix << "</field>\n";

			ofs << prefix << "<field>\n";
			prefix += '\t';

			ofs << prefix << "<field_name>output.mod_id</field_name>\n";
			ofs << prefix << "<field_type>STRING</field_type>\n";
			ofs << prefix << "<align_mode>LEFT</align_mode>\n";
			ofs << prefix << "<field_size>" << MOD_ID_LEN<< "</field_size>\n";

			prefix.resize(prefix.size() - 1);
			ofs << prefix << "</field>\n";

			ofs << prefix << "<field>\n";
			prefix += '\t';

			ofs << prefix << "<field_name>output.comm_fee</field_name>\n";
			ofs << prefix << "<field_type>DOUBLE</field_type>\n";
			ofs << prefix << "<align_mode>LEFT</align_mode>\n";
			ofs << prefix << "<field_size>15</field_size>\n";
            ofs << prefix << "<precision>6</precision>\n";

			prefix.resize(prefix.size() - 1);
			ofs << prefix << "</field>\n";

			ofs << prefix << "<field>\n";
			prefix += '\t';

			ofs << prefix << "<field_name>output.mod_type</field_name>\n";
			ofs << prefix << "<field_type>STRING</field_type>\n";
			ofs << prefix << "<align_mode>LEFT</align_mode>\n";
			ofs << prefix << "<field_size>" << MOD_TYPE_LEN<< "</field_size>\n";

			prefix.resize(prefix.size() - 1);
			ofs << prefix << "</field>\n";
		}

		prefix.resize(prefix.size() - 1);
		ofs << prefix << "</fields>\n";

		prefix.resize(prefix.size() - 1);
		ofs << prefix << "</body>\n";

		prefix.resize(prefix.size() - 1);
		ofs << prefix << "</item>\n";
	}
}

void db2cal::load_distribute_item(const string& file_type)
{
	auto_ptr<Generic_Statement> auto_stmt(db->create_statement());
	Generic_Statement *stmt = auto_stmt.get();

	auto_ptr<struct_dynamic> auto_data(db->create_data());
	struct_dynamic *data = auto_data.get();
	if (data == NULL)
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Memory allocation failure")).str(APPLOCALE));

	string sql_str = "select field_name{char[63]}, field_type{int}, "
		"align_mode{int}, length{int}, precision{int}, decode(fill_code, 'S', ' ', fill_code) fill{char[1]} "
		"from unical_dst_record "
		"where file_type = :file_type{char[4]} ";
	if (stage == 3)
		sql_str += "and out_flag = 1 ";
	else
		sql_str += "and use_default = 0 ";
	sql_str += "order by field_order";
	data->setSQL(sql_str);
	stmt->bind(data);

	stmt->setString(1, file_type);

	auto_ptr<Generic_ResultSet> auto_rset(stmt->executeQuery());
	Generic_ResultSet *rset = auto_rset.get();

	while (rset->next()) {
		string field_name = rset->getString(1);
		if (stage != 3 && (field_name == FREEZE_EXPIRE || field_name == CALC_TRACK))
			continue;

		int field_type = rset->getInt(2);
		int align_mode = rset->getInt(3);
		int length = rset->getInt(4);
		int precision = rset->getInt(5);
		char fill = rset->getString(6)[0];

		ofs << prefix << "<field>\n";
		prefix += '\t';

		ofs << prefix << "<field_name>output." << field_name << "</field_name>\n"
			<< prefix << "<field_type>";

		switch (field_type) {
		case 0:
			ofs << "CHAR";
			break;
		case 1:
			ofs << "SHORT";
			break;
		case 2:
			ofs << "INT";
			break;
		case 3:
			ofs << "LONG";
			break;
		case 4:
			ofs << "FLOAT";
			break;
		case 5:
			ofs << "DOUBLE";
			break;
		case 6:
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
			<< "</field_size>\n";

		if (fill != '\0' && fill != ' ')
			ofs << prefix << "<fill>" << fill << "</fill>\n";

		if (precision > 0) {
			ofs << prefix << "<precision>"
				<< precision
				<< "</precision>\n";
		}

		prefix.resize(prefix.size() - 1);
		ofs << prefix << "</field>\n";
	}
}

void db2cal::load_shm_tables()
{
	auto_ptr<Generic_Statement> auto_stmt(db->create_statement());
	Generic_Statement *stmt = auto_stmt.get();

	auto_ptr<struct_dynamic> auto_data(db->create_data());
	struct_dynamic *data = auto_data.get();
	if (data == NULL)
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Memory allocation failure")).str(APPLOCALE));

	data->setSQL("select distinct table_id{int}, lower(table_name){char[63]} from shm_table_info");
	stmt->bind(data);

	auto_ptr<Generic_ResultSet> auto_rset(stmt->executeQuery());
	Generic_ResultSet *rset = auto_rset.get();

	table_map.clear();

	while (rset->next())
		table_map[rset->getInt(1)] = rset->getString(2);
}

void db2cal::load_tables(const string& thread_id)
{
	string shm_table_ids = get_value(thread_id, "shm_table_ids");
	boost::char_separator<char> sep(",");
	tokenizer tokens(shm_table_ids, sep);
	tokenizer::iterator iter;

	ofs << prefix << "<tables>\n";
	prefix += '\t';

	for (iter = tokens.begin(); iter != tokens.end(); ++iter) {
		string token = *iter;
		int table_id = atoi(token.c_str());

		if (table_id == 9 || table_id == 10) // tf_chl_chnl_developer/tf_f_u_c
			continue;

		if (stage == 1 && table_id == 27) // ai_complex_busi_deposit
			continue;

		map<int, string>::const_iterator iter = table_map.find(table_id);
		if (iter == table_map.end())
			continue;

		ofs << prefix << "<table>\n";
		prefix += '\t';

		ofs << prefix << "<table_name>" << iter->second << "</table_name>\n";
		ofs << prefix << "<index_id>0</index_id>\n";

		prefix.resize(prefix.size() - 1);
		ofs << prefix << "</table>\n";
	}

	prefix.resize(prefix.size() - 1);
	ofs << prefix << "</tables>\n";
}

void db2cal::load_aliases()
{
	auto_ptr<Generic_Statement> auto_stmt(db->create_statement());
	Generic_Statement *stmt = auto_stmt.get();

	auto_ptr<struct_dynamic> auto_data(db->create_data());
	struct_dynamic *data = auto_data.get();
	if (data == NULL)
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Memory allocation failure")).str(APPLOCALE));

	data->setSQL("select table_id{int}, table_name{char[63]}, orig_table_id{int} from unical_table_alias");
	stmt->bind(data);

	auto_ptr<Generic_ResultSet> auto_rset(stmt->executeQuery());
	Generic_ResultSet *rset = auto_rset.get();

	ofs << prefix << "<aliases>\n";
	prefix += '\t';

	while (rset->next()) {
		int table_id = rset->getInt(1);
		string table_name = rset->getString(2);
		int orig_table_id = rset->getInt(3);

		ofs << prefix << "<alias>\n";
		prefix += '\t';

		ofs << prefix << "<table_name>" << table_name << "</table_name>\n";

		map<int, string>::const_iterator iter = table_map.find(orig_table_id);
		if (iter == table_map.end())
			throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Can't find table with table id {1}") % orig_table_id).str(APPLOCALE));

		ofs << prefix << "<orig_table_name>" << iter->second << "</orig_table_name>\n";
		ofs << prefix << "<orig_index_id>0</orig_index_id>\n";

		load_alias(table_id);

		prefix.resize(prefix.size() - 1);
		ofs << prefix << "</alias>\n";
	}

	prefix.resize(prefix.size() - 1);
	ofs << prefix << "</aliases>\n";
}

void db2cal::load_alias(int table_id)
{
	auto_ptr<Generic_Statement> auto_stmt(db->create_statement());
	Generic_Statement *stmt = auto_stmt.get();

	auto_ptr<struct_dynamic> auto_data(db->create_data());
	struct_dynamic *data = auto_data.get();
	if (data == NULL)
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Memory allocation failure")).str(APPLOCALE));

	data->setSQL("select field_name{char[63]}, orig_field_name{char[63]} from unical_field_alias "
		"where table_id = :table_id{int}");
	stmt->bind(data);

	stmt->setInt(1, table_id);

	auto_ptr<Generic_ResultSet> auto_rset(stmt->executeQuery());
	Generic_ResultSet *rset = auto_rset.get();

	ofs << prefix << "<fields>\n";
	prefix += '\t';

	while (rset->next()) {
		ofs << prefix << "<field>\n";
		prefix += '\t';

		ofs << prefix << "<field_name>" << rset->getString(1) << "</field_name>\n";
		ofs << prefix << "<orig_field_name>" << rset->getString(2) << "</orig_field_name>\n";

		prefix.resize(prefix.size() - 1);
		ofs << prefix << "</field>\n";
	}

	prefix.resize(prefix.size() - 1);
	ofs << prefix << "</fields>\n";
}

void db2cal::load_para()
{
	auto_ptr<Generic_Statement> auto_stmt(db->create_statement());
	Generic_Statement *stmt = auto_stmt.get();

	auto_ptr<struct_dynamic> auto_data(db->create_data());
	struct_dynamic *data = auto_data.get();
	if (data == NULL)
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Memory allocation failure")).str(APPLOCALE));

	data->setSQL("select thread_id{char[8]}, para_name{char[63]}, para_contents{char[4000]} "
		"from sys_thread_para where substr(thread_id, 0, 2) = '19'");
	stmt->bind(data);

	auto_ptr<Generic_ResultSet> auto_rset(stmt->executeQuery());
	Generic_ResultSet *rset = auto_rset.get();

	while (rset->next()) {
		thread_para_t para;

		para.thread_id = rset->getString(1);
		para.para_name = rset->getString(2);

		para_map[para] = rset->getString(3);
	}
}

void db2cal::load_para_default()
{
	auto_ptr<Generic_Statement> auto_stmt(db->create_statement());
	Generic_Statement *stmt = auto_stmt.get();

	auto_ptr<struct_dynamic> auto_data(db->create_data());
	struct_dynamic *data = auto_data.get();
	if (data == NULL)
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Memory allocation failure")).str(APPLOCALE));

	data->setSQL("select para_name{char[63]}, default_value{char[4000]} "
		"from sys_thread_para_default where module_id = '19'");
	stmt->bind(data);

	auto_ptr<Generic_ResultSet> auto_rset(stmt->executeQuery());
	Generic_ResultSet *rset = auto_rset.get();

	while (rset->next())
		para_default_map[rset->getString(1)] = rset->getString(2);
}

string db2cal::get_value(const string& thread_id, const string& para_name)
{
	string value;
	thread_para_t para;
	para.thread_id = thread_id;
	para.para_name = para_name;

	map<thread_para_t, string>::const_iterator iter = para_map.find(para);
	if (iter != para_map.end())
		return iter->second;

	map<string, string>::const_iterator def_iter = para_default_map.find(para_name);
	if (def_iter != para_default_map.end())
		return def_iter->second;

	return "";
}

string db2cal::get_thread_id(const string& file_type)
{
	set<string> threads;

	for (map<thread_para_t, string>::const_iterator iter = para_map.begin(); iter != para_map.end(); ++iter) {
		if (iter->first.para_name == "stage" && atoi(iter->second.c_str()) == stage)
			threads.insert(iter->first.thread_id);
	}

	for (map<thread_para_t, string>::const_iterator iter = para_map.begin(); iter != para_map.end(); ++iter) {
		if (iter->first.para_name == "file_type" && iter->second == file_type) {
			string thread_id = iter->first.thread_id;
			if (threads.find(thread_id) != threads.end())
				return thread_id;
		}
	}

	return "";
}

}
}

using namespace ai::app;

int main(int argc, char **argv)
{
	try {
		db2cal instance;

		instance.run(argc, argv);
		return 0;
	} catch (exception& ex) {
		cout << ex.what() << std::endl;
		return 1;
	}
}

