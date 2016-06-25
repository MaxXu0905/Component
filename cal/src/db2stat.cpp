#include "sa_struct.h"
#include "db2stat.h"

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

db2stat::db2stat()
{
	db = NULL;
}

db2stat::~db2stat()
{
	delete db;
}

void db2stat::run(int argc, char **argv)
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
	;

	try {
		po::variables_map vm;
		po::store(po::command_line_parser(argc, argv).options(desc).run(), vm);

		if (vm.count("help")) {
			std::cout << desc << std::endl;
			exit(0);
		}

		po::notify(vm);
	} catch (exception& ex) {
		std::cout << ex.what() << std::endl;
		std::cout << desc << std::endl;
		exit(1);
	}

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

	auto_ptr<struct_dynamic> auto_data(db->create_data(true));
	struct_dynamic *data = auto_data.get();
	if (data == NULL)
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Memory allocation failure")).str(APPLOCALE));

	data->setSQL("select distinct thread_id{char[8]} from sys_thread_para where substr(thread_id, 0, 2) = '14' order by thread_id");
	stmt->bind(data);

	auto_ptr<Generic_ResultSet> auto_rset(stmt->executeQuery());
	Generic_ResultSet *rset = auto_rset.get();
	map<string, int> stat_keys;
	map<string, string> thread_keys;

	while (rset->next()) {
		string thread_id = rset->getString(1);
		string stat_key = get_value(thread_id, "stat_key");
		if (stat_key.empty())
			continue;

		int stat_id;
		map<string, int>::iterator key_iter = stat_keys.find(stat_key);
		if (key_iter == stat_keys.end()) {
			stat_keys[stat_key] = 1;
			stat_id = 1;
		} else {
			stat_id = ++key_iter->second;
		}

		try {
			string integrator_name = string("S") + stat_key;
			if (stat_id > 1)
				integrator_name += boost::lexical_cast<string>(stat_id);

			thread_keys[thread_id] = integrator_name;

			string output_file = prefix_file + "_" + integrator_name +".xml";

			ofs.open(output_file.c_str());
			if (!ofs)
				throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Can't open file {1}") % output_file).str(APPLOCALE));

			ofs << "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
				<< "<integrator xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:noNamespaceSchemaLocation=\"integrator.xsd\">\n"
				<< "\t<resource>\n"
				<< "\t\t<integrator_name>" << integrator_name << "</integrator_name>\n"
				<< "\t\t<openinfo>${OPENINFO}</openinfo>\n"
				<< "\t\t<libs>libsqloracle.so libbps.so</libs>\n"
				<< "\t\t<sginfo/>\n"
				<< "\t\t<dbcinfo>${DBCINFO}</dbcinfo>\n"
				<< "\t</resource>\n";

			prefix = "\t";
			load_global(stat_key);

			ofs << "\t<adaptors>\n"
				<< "\t\t<adaptor>\n";

			prefix = "\t\t\t";
			load_adaptor(stat_key, thread_id);

			ofs << "\t\t</adaptor>\n"
				<< "\t</adaptors>\n"
				<< "</integrator>\n";
		} catch (exception& ex) {
			cout << "parse stat_key " << stat_key << " failed" << endl;
			cout << ex.what() << endl;
		}

		ofs.close();
	}

	for (map<string, string>::const_iterator iter = thread_keys.begin(); iter != thread_keys.end(); ++iter)
		cout << iter->first << "\t" << iter->second << endl;
}

void db2stat::load_adaptor(const string& stat_key, const string& thread_id)
{
	load_resource(stat_key, thread_id);
	load_source(stat_key, thread_id);
	load_invalid(stat_key, thread_id);
	load_error(stat_key, thread_id);
	load_input(stat_key);
	load_stat(stat_key, thread_id);
}

void db2stat::load_resource(const string& stat_key, const string& thread_id)
{
	ofs << prefix << "<resource>\n";
	prefix += "\t";

	ofs << prefix << "<com_key>BPS</com_key>\n"
		<< prefix << "<svc_key></svc_key>\n"
		<< prefix << "<batch>1</batch>\n"
		<< prefix << "<concurrency>1</concurrency>\n"
		<< prefix << "<disable_global>N</disable_global>\n";

	prefix.resize(prefix.size() - 1);
	ofs << prefix << "</resource>\n";
}

void db2stat::load_source(const string& stat_key, const string& thread_id)
{
	ofs << prefix << "<source>\n";
	prefix += '\t';

	ofs << prefix << "<class_name>sa_ivariable</class_name>\n"
		<< prefix << "<per_records>1000</per_records>\n"
		<< prefix << "<args>\n";
	prefix += '\t';

	ofs << prefix << "<arg>\n";
	prefix += '\t';

	string src_dir = get_value(thread_id, "src_dir");
	if (*src_dir.rbegin() == '/')
		src_dir.resize(src_dir.size() - 1);
	ofs << prefix << "<name>src_dir</name>\n"
		<< prefix << "<value>" << src_dir << "</value>\n";

	prefix.resize(prefix.size() - 1);
	ofs << prefix << "</arg>\n"
		<< prefix << "<arg>\n";
	prefix += '\t';

	string dup_dir = get_value(thread_id, "dup_dir");
	if (*dup_dir.rbegin() == '/')
		dup_dir.resize(dup_dir.size() - 1);
	ofs << prefix << "<name>dup_dir</name>\n"
		<< prefix << "<value>" << dup_dir << "</value>\n";

	prefix.resize(prefix.size() - 1);
	ofs << prefix << "</arg>\n"
		<< prefix << "<arg>\n";
	prefix += '\t';

	string bak_dir = get_value(thread_id, "bak_dir");
	if (*bak_dir.rbegin() == '/')
		bak_dir.resize(bak_dir.size() - 1);
	ofs << prefix << "<name>bak_dir</name>\n"
		<< prefix << "<value>" << bak_dir << "</value>\n";

	prefix.resize(prefix.size() - 1);
	ofs << prefix << "</arg>\n"
		<< prefix << "<arg>\n";
	prefix += '\t';

	string tmp_dir = get_value(thread_id, "dup_dir");
	if (*tmp_dir.rbegin() == '/')
		tmp_dir.resize(tmp_dir.size() - 1);
	while (*tmp_dir.rbegin() != '/')
		tmp_dir.resize(tmp_dir.size() - 1);
	tmp_dir += "tmp";

	ofs << prefix << "<name>tmp_dir</name>\n"
		<< prefix << "<value>" << tmp_dir << "</value>\n";

	prefix.resize(prefix.size() - 1);
	ofs << prefix << "</arg>\n"
		<< prefix << "<arg>\n";
	prefix += '\t';

	string ftl_dir = get_value(thread_id, "dup_dir");
	if (*ftl_dir.rbegin() == '/')
		ftl_dir.resize(ftl_dir.size() - 1);
	while (*ftl_dir.rbegin() != '/')
		ftl_dir.resize(ftl_dir.size() - 1);
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

void db2stat::load_invalid(const string& stat_key, const string& thread_id)
{
	ofs << prefix << "<invalid>\n";
	prefix += '\t';

	ofs << prefix << "<class_name>sa_osource</class_name>\n"
		<< prefix << "<args>\n";
	prefix += '\t';

	ofs << prefix << "<arg>\n";
	prefix += '\t';

	string inv_dir = get_value(thread_id, "dup_dir");
	if (*inv_dir.rbegin() == '/')
		inv_dir.resize(inv_dir.size() - 1);
	while (*inv_dir.rbegin() != '/')
		inv_dir.resize(inv_dir.size() - 1);
	inv_dir += "inv";

	ofs << prefix << "<name>directory</name>\n"
		<< prefix << "<value>" << inv_dir << "</value>\n";

	prefix.resize(prefix.size() - 1);
	ofs << prefix << "</arg>\n";

	prefix.resize(prefix.size() - 1);
	ofs << prefix << "</args>\n";

	prefix.resize(prefix.size() - 1);
	ofs << prefix << "</invalid>\n";
}

void db2stat::load_error(const string& stat_key, const string& thread_id)
{
	ofs << prefix << "<error>\n";
	prefix += '\t';

	ofs << prefix << "<class_name>sa_osource</class_name>\n"
		<< prefix << "<args>\n";
	prefix += '\t';

	ofs << prefix << "<arg>\n";
	prefix += '\t';

	string err_dir = get_value(thread_id, "dup_dir");
	if (*err_dir.rbegin() == '/')
		err_dir.resize(err_dir.size() - 1);
	while (*err_dir.rbegin() != '/')
		err_dir.resize(err_dir.size() - 1);
	err_dir += "err";

	ofs << prefix << "<name>directory</name>\n"
		<< prefix << "<value>" << err_dir << "</value>\n";

	prefix.resize(prefix.size() - 1);
	ofs << prefix << "</arg>\n";

	prefix.resize(prefix.size() - 1);
	ofs << prefix << "</args>\n";

	prefix.resize(prefix.size() - 1);
	ofs << prefix << "</error>\n";
}

void db2stat::load_global(const string& stat_key)
{
	ofs << prefix << "<global/>\n";
}

void db2stat::load_input(const string& stat_key)
{
	auto_ptr<Generic_Statement> auto_stmt(db->create_statement());
	Generic_Statement *stmt = auto_stmt.get();

	auto_ptr<struct_dynamic> auto_data(db->create_data());
	struct_dynamic *data = auto_data.get();
	if (data == NULL)
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Memory allocation failure")).str(APPLOCALE));

	string sql_str = "select factor1{int}, factor2{int}, lower(field_name){char[63]},field_serial{int} "
		"from stat_src_record "
		"where stat_key = :stat_key{char[8]} and field_type = 0 "
		"order by field_serial";

	data->setSQL(sql_str);
	stmt->bind(data);

	stmt->setString(1, stat_key);

	auto_ptr<Generic_ResultSet> auto_rset(stmt->executeQuery());
	Generic_ResultSet *rset = auto_rset.get();

	ofs << prefix << "<input>\n";
	prefix += '\t';

	ofs << prefix << "<table>\n";
	prefix += '\t';
	ofs << prefix << "<name>input</name>\n"
		<< prefix << "<delimiter>1</delimiter>\n";

	ofs << prefix << "<fields>\n";
	prefix += '\t';

	fields.clear();
	while (rset->next()) {
		string field_name = rset->getString(3);
		int field_serial = rset->getInt(4);

		ofs << prefix << "<field>\n";
		prefix += '\t';

		ofs << prefix << "<factor1>" << rset->getString(1) << "</factor1>\n"
			<< prefix << "<factor2>" << rset->getInt(2) << "</factor2>\n"
			<< prefix << "<field_name>" << field_name << "</field_name>\n";

		prefix.resize(prefix.size() - 1);
		ofs << prefix << "</field>\n";

		fields[field_serial] = field_name;
	}

	prefix.resize(prefix.size() - 1);
	ofs << prefix << "</fields>\n";

	prefix.resize(prefix.size() - 1);
	ofs << prefix << "</table>\n";

	prefix.resize(prefix.size() - 1);
	ofs << prefix << "</input>\n";
}

void db2stat::load_stat(const string& stat_key, const string& thread_id)
{
	ofs << prefix << "<stat>\n";
	prefix += '\t';

	ofs << prefix << "<class_name>sa_ostat</class_name>\n"
		<< prefix << "<items>\n";
	prefix += '\t';

	string cdr_flag = get_value(thread_id, "cdr_flag");
	if (cdr_flag == "1")
		load_stat_src_record(stat_key, thread_id);

	string stat_flag = get_value(thread_id, "stat_flag");
	if (stat_flag == "1")
		load_stat_element(stat_key, thread_id);

	prefix.resize(prefix.size() - 1);
	ofs << prefix << "</items>\n";

	prefix.resize(prefix.size() - 1);
	ofs << prefix << "</stat>\n";
}

void db2stat::load_stat_src_record(const string& stat_key, const string& thread_id)
{
	ofs << prefix << "<item>\n";
	prefix += '\t';

	ofs << prefix << "<options>INSERT</options>\n"
		<< prefix << "<sort>Y</sort>\n"
		<< prefix << "<rule_condition><![CDATA[\n\n]]></rule_condition>\n"
		<< prefix << "<rule_table_name><![CDATA[\n" << get_value(thread_id, "rule_table_name") << "\n]]></rule_table_name>\n";

	auto_ptr<Generic_Statement> auto_stmt(db->create_statement());
	Generic_Statement *stmt = auto_stmt.get();

	auto_ptr<struct_dynamic> auto_data(db->create_data());
	struct_dynamic *data = auto_data.get();
	if (data == NULL)
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Memory allocation failure")).str(APPLOCALE));

	string sql_str = "select lower(field_name){char[63]}, field_type{int}, func_desc{char[128]} "
		"from stat_src_record "
		"where stat_key = :stat_key{char[8]} and db_flag = 1 "
		"order by field_serial";

	data->setSQL(sql_str);
	stmt->bind(data);

	stmt->setString(1, stat_key);

	auto_ptr<Generic_ResultSet> auto_rset(stmt->executeQuery());
	Generic_ResultSet *rset = auto_rset.get();

	ofs << prefix << "<fields>\n";
	prefix += '\t';

	while (rset->next()) {
		string field_name = rset->getString(1);
		int field_type = rset->getInt(2);

		ofs << prefix << "<field>\n";
		prefix += '\t';

		ofs << prefix << "<field_name>";
		switch (field_type) {
		case 0:	// 输入中字段
			ofs << "input." << field_name;
			break;
		case 1:	// 文件名
			ofs << "file_name";
			break;
		case 2:	// 系统时间
			ofs << "sysdate";
			break;
		case 3:	// 记录序列号
			ofs << "record_sn";
			break;
		default:
			throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Unknown field_type {1}") % field_type).str(APPLOCALE));
		}
		ofs << "</field_name>\n";

		ofs << prefix << "<element_name>" << field_name << "</element_name>\n";

		string insert_desc = rset->getString(3);
		if (!insert_desc.empty())
			ofs << prefix << "<insert_desc>" << rset->getString(3) << "</insert_desc>\n";

		prefix.resize(prefix.size() - 1);
		ofs << prefix << "</field>\n";
	}

	prefix.resize(prefix.size() - 1);
	ofs << prefix << "</fields>\n";

	prefix.resize(prefix.size() - 1);
	ofs << prefix << "</item>\n";
}

void db2stat::load_stat_element(const string& stat_key, const string& thread_id)
{
	auto_ptr<Generic_Statement> auto_stmt(db->create_statement());
	Generic_Statement *stmt = auto_stmt.get();

	auto_ptr<struct_dynamic> auto_data(db->create_data());
	struct_dynamic *data = auto_data.get();
	if (data == NULL)
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Memory allocation failure")).str(APPLOCALE));

	string sql_str = "select stat_type{char[63]}, rule_condition{char[4000]}, rule_table_name{char[4000]} "
		"from stat_type "
		"where thread_id = :thread_id{char[8]} and stat_key = :stat_key{char[8]}";

	data->setSQL(sql_str);
	stmt->bind(data);

	stmt->setString(1, thread_id);
	stmt->setString(2, stat_key);

	auto_ptr<Generic_ResultSet> auto_rset(stmt->executeQuery());
	Generic_ResultSet *rset = auto_rset.get();

	while (rset->next()) {
		ofs << prefix << "<item>\n";
		prefix += '\t';

		string rule_condition = rset->getString(2);
		string::size_type pos = rule_condition.find("$45");
		if (pos != string::npos)
			rule_condition = rule_condition.substr(0, pos) + "$input.pay_comm_flag" + rule_condition.substr(pos + 3);

		ofs << prefix << "<options>INSERT UPDATE</options>\n"
			<< prefix << "<sort>Y</sort>\n"
			<< prefix << "<rule_condition><![CDATA[\n" << rule_condition << "\n]]></rule_condition>\n"
			<< prefix << "<rule_table_name><![CDATA[\n" << rset->getString(3) << "\n]]></rule_table_name>\n";

		load_stat_element_item(stat_key, rset->getString(1));

		prefix.resize(prefix.size() - 1);
		ofs << prefix << "</item>\n";
	}
}

void db2stat::load_stat_element_item(const string& stat_key, const string& stat_type)
{
	auto_ptr<Generic_Statement> auto_stmt(db->create_statement());
	Generic_Statement *stmt = auto_stmt.get();

	auto_ptr<struct_dynamic> auto_data(db->create_data());
	struct_dynamic *data = auto_data.get();
	if (data == NULL)
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Memory allocation failure")).str(APPLOCALE));

	string sql_str = "select element_type{int}, lower(element_name){char[63]}, field_serial{int}, field_type{int}, "
		"insert_func_desc{char[128]}, update_func_desc{char[128]}, operation{char[32]} "
		"from stat_element "
		"where stat_key = :stat_key{char[8]} and stat_type = :stat_type{char[8]} "
		"order by element_type, field_type, field_serial";

	data->setSQL(sql_str);
	stmt->bind(data);

	stmt->setString(1, stat_key);
	stmt->setString(2, stat_type);

	auto_ptr<Generic_ResultSet> auto_rset(stmt->executeQuery());
	Generic_ResultSet *rset = auto_rset.get();

	ofs << prefix << "<fields>\n";
	prefix += '\t';

	while (rset->next()) {
		int element_type = rset->getInt(1);
		string element_name = rset->getString(2);
		int field_serial = rset->getInt(3);
		int field_type = rset->getInt(4);

		ofs << prefix << "<field>\n";
		prefix += '\t';

		ofs << prefix << "<field_name>";
		switch (field_type) {
		case 0:	// 输入中字段
			if (field_serial >= 0 )
				ofs << "input." << fields[field_serial];
			break;
		case 1:	// 文件名
			ofs << "file_name";
			break;
		case 2:	// 系统时间
			ofs << "sysdate";
			break;
		case 3:	// 记录序列号
			ofs << "record_sn";
			break;
		default:
			throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Unknown field_type {1}") % field_type).str(APPLOCALE));
		}
		ofs << "</field_name>\n";

		ofs << prefix << "<element_name>" << element_name << "</element_name>\n";
		ofs << prefix << "<operation>" << rset->getString(7) << "</operation>\n";

		string insert_desc = rset->getString(5);
		if (!insert_desc.empty())
			ofs << prefix << "<insert_desc>" << insert_desc << "</insert_desc>\n";

		string update_desc = rset->getString(6);
		if (!update_desc.empty())
			ofs << prefix << "<update_desc>" << update_desc << "</update_desc>\n";

		if (element_type == 0)
			ofs << prefix << "<is_key>Y</is_key>\n";

		prefix.resize(prefix.size() - 1);
		ofs << prefix << "</field>\n";
	}

	prefix.resize(prefix.size() - 1);
	ofs << prefix << "</fields>\n";
}

void db2stat::load_para()
{
	auto_ptr<Generic_Statement> auto_stmt(db->create_statement());
	Generic_Statement *stmt = auto_stmt.get();

	auto_ptr<struct_dynamic> auto_data(db->create_data(true));
	struct_dynamic *data = auto_data.get();
	if (data == NULL)
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Memory allocation failure")).str(APPLOCALE));

	data->setSQL("select thread_id{char[8]}, para_name{char[63]}, para_contents{char[4000]} "
		"from sys_thread_para where substr(thread_id, 0, 2) = '14'");
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

void db2stat::load_para_default()
{
	auto_ptr<Generic_Statement> auto_stmt(db->create_statement());
	Generic_Statement *stmt = auto_stmt.get();

	auto_ptr<struct_dynamic> auto_data(db->create_data(true));
	struct_dynamic *data = auto_data.get();
	if (data == NULL)
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Memory allocation failure")).str(APPLOCALE));

	data->setSQL("select para_name{char[63]}, default_value{char[4000]} "
		"from sys_thread_para_default where module_id = '14'");
	stmt->bind(data);

	auto_ptr<Generic_ResultSet> auto_rset(stmt->executeQuery());
	Generic_ResultSet *rset = auto_rset.get();

	while (rset->next())
		para_default_map[rset->getString(1)] = rset->getString(2);
}

string db2stat::get_value(const string& thread_id, const string& para_name)
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

}
}

using namespace ai::app;

int main(int argc, char **argv)
{
	try {
		db2stat instance;

		instance.run(argc, argv);
		return 0;
	} catch (exception& ex) {
		cout << ex.what() << std::endl;
		return 1;
	}
}

