#include "sa_internal.h"

namespace ai
{
namespace app
{

using namespace std;
using namespace ai::sg;
using namespace ai::scci;
typedef boost::tokenizer<boost::char_separator<char> > tokenizer;

sa_config::sa_config()
{
	SAP = sap_ctx::instance();
}

sa_config::~sa_config()
{
}

void sa_config::load(const string& config_file)
{
	bpt::iptree pt;
#if defined(DEBUG)
	scoped_debug<bool> debug(100, __PRETTY_FUNCTION__, "", NULL);
#endif

	bpt::read_xml(config_file, pt);

	load_resource(pt);
	load_global(pt);
	load_adaptors(pt);
}

void sa_config::load_xml(const string& config)
{
	bpt::iptree pt;
#if defined(DEBUG)
	scoped_debug<bool> debug(100, __PRETTY_FUNCTION__, "", NULL);
#endif

	istringstream is(config);

	bpt::read_xml(is, pt);

	load_resource(pt);
	load_global(pt);
	load_adaptors(pt);
}

void sa_config::load_resource(const bpt::iptree& pt)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(100, __PRETTY_FUNCTION__, "", NULL);
#endif
	sa_resource_t& resource = SAP->_SAP_resource;
	vector<string> the_vector;
	gpenv& env_mgr = gpenv::instance();

	try {
		BOOST_FOREACH(const bpt::iptree::value_type& v, pt.get_child("integrator")) {
			if (v.first != "resource")
				continue;

			const bpt::iptree& v_pt = v.second;

			resource.integrator_name = v_pt.get<string>("integrator_name");
			resource.dbname = get_value(v_pt, "dbname", string("ORACLE"));
			env_mgr.expand_var(resource.openinfo, get_value(v_pt, "openinfo", string("")));
			resource.libs = get_value(v_pt, "libs", string(""));
			env_mgr.expand_var(resource.sginfo, get_value(v_pt, "sginfo", string("")));
			env_mgr.expand_var(resource.dbcinfo, get_value(v_pt, "dbcinfo", string("")));
		}
	} catch (bpt::ptree_bad_data& ex) {
		throw sg_exception(__FILE__, __LINE__, SGESYSTEM, 0, (_("ERROR: Parse section resource failure, {1} ({2})") % ex.what() % ex.data<string>()).str(APPLOCALE));
	} catch (bpt::ptree_error& ex) {
		throw sg_exception(__FILE__, __LINE__, SGESYSTEM, 0, (_("ERROR: Parse section resource failure, {1}") % ex.what()).str(APPLOCALE));
	}
}

void sa_config::load_global(const bpt::iptree& pt)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(100, __PRETTY_FUNCTION__, "", NULL);
#endif
	sa_global_t& global = SAP->_SAP_global;
	vector<global_field_t>& global_fields = global.global_fields;
	field_map_t& global_map = global.global_map;
	vector<int>& input_globals = global.input_globals;
	vector<int>& output_globals = global.output_globals;
	global_field_t field;
	int field_serial = 0;

	global_fields.clear();
	global_map.clear();
	input_globals.clear();
	output_globals.clear();

	field.field_name = "svc_key";
	field.default_value = "";
	field.field_size = SVC_KEY_LEN;
	field.readonly = true;
	global_fields.push_back(field);
	global_map[field.field_name] = field_serial;
	input_globals.push_back(field_serial);
	field_serial++;

	field.field_name = "file_name";
	field.default_value = "";
	field.field_size = FILE_NAME_LEN;
	field.readonly = true;
	global_fields.push_back(field);
	global_map[field.field_name] = field_serial;
	input_globals.push_back(field_serial);
	field_serial++;

	field.field_name = "file_sn";
	field.default_value = "";
	field.field_size = FILE_SN_LEN;
	field.readonly = true;
	global_fields.push_back(field);
	global_map[field.field_name] = field_serial;
	input_globals.push_back(field_serial);
	field_serial++;

	field.field_name = "redo_mark";
	field.default_value = "";
	field.field_size = REDO_MARK_LEN;
	field.readonly = true;
	global_fields.push_back(field);
	global_map[field.field_name] = field_serial;
	input_globals.push_back(field_serial);
	field_serial++;

	field.field_name = "sysdate";
	field.default_value = "";
	field.field_size = SYSDATE_LEN;
	global_fields.push_back(field);
	global_map[field.field_name] = field_serial;
	input_globals.push_back(field_serial);
	field_serial++;

	field.field_name = "first_serial";
	field.default_value = "";
	field.field_size = SERIAL_LEN;
	field.readonly = true;
	global_fields.push_back(field);
	global_map[field.field_name] = field_serial;
	input_globals.push_back(field_serial);
	field_serial++;

	field.field_name = "second_serial";
	field.default_value = "";
	field.field_size = SERIAL_LEN;
	field.readonly = true;
	global_fields.push_back(field);
	global_map[field.field_name] = field_serial;
	input_globals.push_back(field_serial);
	field_serial++;

	try {
		boost::optional<const bpt::iptree&> node = pt.get_child_optional("integrator.global.fields");
		if (!node)
			return;

		BOOST_FOREACH(const bpt::iptree::value_type& v, *node) {
			if (v.first != "field")
				continue;

			const bpt::iptree& v_pt = v.second;

			field.field_name = boost::to_lower_copy(v_pt.get<string>("field_name"));
			if (field.field_name.empty())
				throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: global.field_name is empty")).str(APPLOCALE));

			field.default_value = get_value(v_pt, "default_value", string(""));

			field.field_size = v_pt.get<int>("field_size");
			if (field.field_size <= 0)
				throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: global.field_size must be positive")).str(APPLOCALE));

			string readonly = get_value(v_pt, "readonly", string("N"));
			if (strcasecmp(readonly.c_str(), "Y") == 0)
				field.readonly = true;
			else if (strcasecmp(readonly.c_str(), "N") == 0)
				field.readonly = false;
			else
				throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: global.readonly invalid")).str(APPLOCALE));

			field_map_t::const_iterator iter = global_map.find(field.field_name);
			if (iter != global_map.end())
				throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: duplicate global.field_name {1}") % field.field_name).str(APPLOCALE));

			global_fields.push_back(field);
			global_map[field.field_name] = field_serial;
			input_globals.push_back(field_serial);
			if (!field.readonly)
				output_globals.push_back(field_serial);
			field_serial++;
		}
	} catch (bpt::ptree_bad_data& ex) {
		throw sg_exception(__FILE__, __LINE__, SGESYSTEM, 0, (_("ERROR: Parse section global failure, {1} ({2})") % ex.what() % ex.data<string>()).str(APPLOCALE));
	} catch (bpt::ptree_error& ex) {
		throw sg_exception(__FILE__, __LINE__, SGESYSTEM, 0, (_("ERROR: Parse section global failure, {1}") % ex.what()).str(APPLOCALE));
	}
}

void sa_config::load_adaptors(const bpt::iptree& pt)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(100, __PRETTY_FUNCTION__, "", NULL);
#endif

	try {
		BOOST_FOREACH(const bpt::iptree::value_type& v, pt.get_child("integrator.adaptors")) {
			if (v.first != "adaptor")
				continue;

			const bpt::iptree& v_pt = v.second;
			SAP->_SAP_adaptors.push_back(sa_adaptor_t());
			sa_adaptor_t& adaptor = *SAP->_SAP_adaptors.rbegin();

			load_parms(v_pt, adaptor);

			if (!adaptor.parms.disable_global) {
				adaptor.global_map = SAP->_SAP_global.global_map;
				adaptor.input_globals = SAP->_SAP_global.input_globals;
				adaptor.output_globals = SAP->_SAP_global.output_globals;
			} else {
				adaptor.global_map.clear();
				adaptor.input_globals.clear();
				adaptor.output_globals.clear();
			}

			load_source(v_pt, adaptor);
			load_target(v_pt, adaptor);
			load_invalid(v_pt, adaptor);
			load_error(v_pt, adaptor);
			load_input(v_pt, adaptor);
			load_output(v_pt, adaptor);
			load_summary(v_pt, adaptor);
			load_distribute(v_pt, adaptor);
			load_stat(v_pt, adaptor);
			load_args(v_pt, adaptor);
		}
	} catch (bpt::ptree_bad_data& ex) {
		throw sg_exception(__FILE__, __LINE__, SGESYSTEM, 0, (_("ERROR: Parse section adaptors failure, {1} ({2})") % ex.what() % ex.data<string>()).str(APPLOCALE));
	} catch (bpt::ptree_error& ex) {
		throw sg_exception(__FILE__, __LINE__, SGESYSTEM, 0, (_("ERROR: Parse section adaptors failure, {1}") % ex.what()).str(APPLOCALE));
	}

	if (SAP->_SAP_adaptors.empty())
		throw sg_exception(__FILE__, __LINE__, SGESYSTEM, 0, (_("ERROR: integrator.adaptors.adaptor missing.")).str(APPLOCALE));
}


void sa_config::load_parms(const bpt::iptree& pt, sa_adaptor_t& adaptor)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(100, __PRETTY_FUNCTION__, "", NULL);
#endif
	sa_parms_t& parms = adaptor.parms;
	boost::char_separator<char> sep(" \t\b");

	try {
		const bpt::iptree& v_pt = pt.get_child("resource");

		parms.com_key = v_pt.get<string>("com_key");
		parms.svc_key = get_value(v_pt, "svc_key", string(""));
		parms.version = get_value(v_pt, "version", DFLT_VERSION);
		parms.batch = get_value(v_pt, "batch", DFLT_BATCH);
		if (parms.batch <= 0)
			throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: resource.batch must be positive")).str(APPLOCALE));

		parms.concurrency = get_value(v_pt, "concurrency", DFLT_CONCURRENCY);
		if (parms.concurrency <= 0 || parms.concurrency > 64)
			throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: resource.concurrency must be between 1 and 64")).str(APPLOCALE));

		string disable_global = get_value(v_pt, "disable_global", string("N"));
		if (strcasecmp(disable_global.c_str(), "Y") == 0)
			parms.disable_global = true;
		else if (strcasecmp(disable_global.c_str(), "N") == 0)
			parms.disable_global = false;
		else
			throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: resource.disable_global invalid")).str(APPLOCALE));

		parms.options = 0;
		string options = get_value(v_pt, "options", string(""));
		tokenizer tokens(options, sep);
		for (tokenizer::iterator iter = tokens.begin(); iter != tokens.end(); ++iter) {
			string str = *iter;

			if (strcasecmp(str.c_str(), "FROM_INPUT") == 0)
				parms.options |= SA_OPTION_FROM_INPUT;
			else if (strcasecmp(str.c_str(), "FROM_OUTPUT") == 0)
				parms.options |= SA_OPTION_FROM_OUTPUT;
		}

		string exception_policy = get_value(v_pt, "exception_policy", string("RETRY"));
		if (strcasecmp(exception_policy.c_str(), "RETRY") == 0)
			parms.exception_policy = EXCEPTION_POLICY_RETRY;
		else if (strcasecmp(exception_policy.c_str(), "DONE") == 0)
			parms.exception_policy = EXCEPTION_POLICY_DONE;
		else if (strcasecmp(exception_policy.c_str(), "EXIT") == 0)
			parms.exception_policy = EXCEPTION_POLICY_EXIT;
		else
			throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: resource.exception_policy should be RETRY, DONE or EXIT")).str(APPLOCALE));

		parms.exception_waits = get_value(v_pt, "exception_waits", DFLT_EXCEPTION_WAITS);
		if (parms.exception_waits < 0)
			throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: resource.exception_waits must not be negative")).str(APPLOCALE));

		if ((parms.options & (SA_OPTION_FROM_INPUT | SA_OPTION_FROM_OUTPUT)) && SAP->_SAP_adaptors.size() == 1)
			throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: resource.options can't be set to FROM_INPUT or FROM_OUTPUT for first adaptor")).str(APPLOCALE));
	} catch (bpt::ptree_bad_data& ex) {
		throw sg_exception(__FILE__, __LINE__, SGESYSTEM, 0, (_("ERROR: Parse section resource failure, {1} ({2})") % ex.what() % ex.data<string>()).str(APPLOCALE));
	} catch (bpt::ptree_error& ex) {
		throw sg_exception(__FILE__, __LINE__, SGESYSTEM, 0, (_("ERROR: Parse section resource failure, {1}") % ex.what()).str(APPLOCALE));
	}
}

void sa_config::load_source(const bpt::iptree& pt, sa_adaptor_t& adaptor)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(100, __PRETTY_FUNCTION__, "", NULL);
#endif
	sa_source_t& source = adaptor.source;
	map<string, string>& args = source.args;

	try {
		boost::optional<const bpt::iptree&> node = pt.get_child_optional("source");
		if (!node) {
			source.class_name = "sa_input";
			return;
		}

		const bpt::iptree& v_pt = *node;

		source.class_name = v_pt.get<string>("class_name");
		source.per_records = get_value(v_pt, "per_records", DFLT_PER_RECORDS);
		source.per_report = get_value(v_pt, "per_report", DFLT_PER_REPORT);
		source.max_error_records = get_value(v_pt, "max_error_records", DFLT_MAX_ERROR_RECORDS);

		BOOST_FOREACH(const bpt::iptree::value_type& v, v_pt.get_child("args")) {
			if (v.first != "arg")
				continue;

			const bpt::iptree& v_pf = v.second;

			string name = v_pf.get<string>("name");
			name = boost::to_lower_copy(name);
			string value = v_pf.get<string>("value");

			if (args.find(name) != args.end())
				throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: Duplicate {1} for source.args.arg") % name).str(APPLOCALE));

			args[name] = value;
		}
	} catch (bpt::ptree_bad_data& ex) {
		throw sg_exception(__FILE__, __LINE__, SGESYSTEM, 0, (_("ERROR: Parse section source failure, {1} ({2})") % ex.what() % ex.data<string>()).str(APPLOCALE));
	} catch (bpt::ptree_error& ex) {
		throw sg_exception(__FILE__, __LINE__, SGESYSTEM, 0, (_("ERROR: Parse section source failure, {1}") % ex.what()).str(APPLOCALE));
	}
}

void sa_config::load_target(const bpt::iptree& pt, sa_adaptor_t& adaptor)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(100, __PRETTY_FUNCTION__, "", NULL);
#endif
	sa_target_t& target = adaptor.target;
	map<string, string>& args = target.args;

	try {
		boost::optional<const bpt::iptree&> node = pt.get_child_optional("target");
		if (!node) {
			target.class_name = "sa_output";
			return;
		}

		const bpt::iptree& v_pt = *node;

		target.class_name = v_pt.get<string>("class_name");

		node = v_pt.get_child_optional("args");
		if (!node)
			return;

		BOOST_FOREACH(const bpt::iptree::value_type& v, *node) {
			if (v.first != "arg")
				continue;

			const bpt::iptree& v_pf = v.second;

			string name = v_pf.get<string>("name");
			name = boost::to_lower_copy(name);
			string value = v_pf.get<string>("value");

			if (args.find(name) != args.end())
				throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: Duplicate {1} for target.args.arg") % name).str(APPLOCALE));

			args[name] = value;
		}
	} catch (bpt::ptree_bad_data& ex) {
		throw sg_exception(__FILE__, __LINE__, SGESYSTEM, 0, (_("ERROR: Parse section target failure, {1} ({2})") % ex.what() % ex.data<string>()).str(APPLOCALE));
	} catch (bpt::ptree_error& ex) {
		throw sg_exception(__FILE__, __LINE__, SGESYSTEM, 0, (_("ERROR: Parse section target failure, {1}") % ex.what()).str(APPLOCALE));
	}
}

void sa_config::load_invalid(const bpt::iptree& pt, sa_adaptor_t& adaptor)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(100, __PRETTY_FUNCTION__, "", NULL);
#endif
	sa_invalid_t& invalid = adaptor.invalid;
	map<string, string>& args = invalid.args;

	try {
		boost::optional<const bpt::iptree&> node = pt.get_child_optional("invalid");
		if (!node) {
			invalid.class_name = "sa_output";
			return;
		}

		const bpt::iptree& v_pt = *node;

		invalid.class_name = v_pt.get<string>("class_name");

		node = v_pt.get_child_optional("args");
		if (!node)
			return;

		BOOST_FOREACH(const bpt::iptree::value_type& v, *node) {
			if (v.first != "arg")
				continue;

			const bpt::iptree& v_pf = v.second;

			string name = v_pf.get<string>("name");
			name = boost::to_lower_copy(name);
			string value = v_pf.get<string>("value");

			if (args.find(name) != args.end())
				throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: Duplicate {1} for invalid.args.arg") % name).str(APPLOCALE));

			args[name] = value;
		}
	} catch (bpt::ptree_bad_data& ex) {
		throw sg_exception(__FILE__, __LINE__, SGESYSTEM, 0, (_("ERROR: Parse section invalid failure, {1} ({2})") % ex.what() % ex.data<string>()).str(APPLOCALE));
	} catch (bpt::ptree_error& ex) {
		throw sg_exception(__FILE__, __LINE__, SGESYSTEM, 0, (_("ERROR: Parse section invalid failure, {1}") % ex.what()).str(APPLOCALE));
	}
}

void sa_config::load_error(const bpt::iptree& pt, sa_adaptor_t& adaptor)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(100, __PRETTY_FUNCTION__, "", NULL);
#endif
	sa_error_t& error = adaptor.error;
	map<string, string>& args = error.args;

	try {
		boost::optional<const bpt::iptree&> node = pt.get_child_optional("error");
		if (!node) {
			error.class_name = "sa_output";
			return;
		}

		const bpt::iptree& v_pt = *node;

		error.class_name = v_pt.get<string>("class_name");

		node = v_pt.get_child_optional("args");
		if (!node)
			return;

		BOOST_FOREACH(const bpt::iptree::value_type& v, *node) {
			if (v.first != "arg")
				continue;

			const bpt::iptree& v_pf = v.second;

			string name = v_pf.get<string>("name");
			name = boost::to_lower_copy(name);
			string value = v_pf.get<string>("value");

			if (args.find(name) != args.end())
				throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: Duplicate {1} for error.args.arg") % name).str(APPLOCALE));

			args[name] = value;
		}
	} catch (bpt::ptree_bad_data& ex) {
		throw sg_exception(__FILE__, __LINE__, SGESYSTEM, 0, (_("ERROR: Parse section error failure, {1} ({2})") % ex.what() % ex.data<string>()).str(APPLOCALE));
	} catch (bpt::ptree_error& ex) {
		throw sg_exception(__FILE__, __LINE__, SGESYSTEM, 0, (_("ERROR: Parse section error failure, {1}") % ex.what()).str(APPLOCALE));
	}

}

void sa_config::load_input(const bpt::iptree& pt, sa_adaptor_t& adaptor)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(100, __PRETTY_FUNCTION__, "", NULL);
#endif
	vector<input_record_t>& input_records = adaptor.input_records;
	vector<field_map_t>& input_maps = adaptor.input_maps;
	vector<int>& input_sizes = adaptor.input_sizes;

	input_records.clear();
	input_maps.clear();
	input_sizes.clear();

	if (adaptor.parms.options & SA_OPTION_FROM_INPUT) {
		sa_adaptor_t& prev_adaptor = *(SAP->_SAP_adaptors.rbegin() + 1);

		if (prev_adaptor.input_records.size() != 1)
			throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: previous adaptor has more inputs, so current adaptor should define input specifically")).str(APPLOCALE));

		input_records = prev_adaptor.input_records;
		input_maps = prev_adaptor.input_maps;
		input_sizes = prev_adaptor.input_sizes;

		return;
	} else if (adaptor.parms.options & SA_OPTION_FROM_OUTPUT) {
		sa_adaptor_t& prev_adaptor = *(SAP->_SAP_adaptors.rbegin() + 1);
		input_records.push_back(input_record_t());
		input_record_t& record = *input_records.rbegin();
		input_maps.push_back(field_map_t());
		field_map_t& input_map = *input_maps.rbegin();

		record.record_type = RECORD_TYPE_BODY;
		record.delimiter = '\0';
		record.rule_condition = "";

		int field_serial = 0;
		BOOST_FOREACH(const output_field_t& output_field, prev_adaptor.output_fields) {
			input_field_t field;

			field.field_serial = field_serial++;
			field.factor1 = -1;
			field.factor2 = output_field.field_size;
			field.encode_type = ENCODE_ASCII;
			field.parent_serial = -1;
			field.field_name = output_field.field_name;
			field.column_name = output_field.column_name;

			record.field_vector.push_back(field);
			record.field_set.insert(field);
			input_map[field.field_name] = field.field_serial;
			input_sizes.push_back(field.factor2);
		}

		return;
	}

	try {
		BOOST_FOREACH(const bpt::iptree::value_type& v, pt.get_child("input")) {
			if (v.first != "table")
				continue;

			const bpt::iptree& v_pt = v.second;

			input_records.push_back(input_record_t());
			input_record_t& record = *input_records.rbegin();
			field_map_t input_map;

			string table_name = boost::to_lower_copy(v_pt.get<string>("name"));

			string record_type = get_value(v_pt, "record_type", string("BODY"));
			if (strcasecmp(record_type.c_str(), "HEAD") == 0)
				record.record_type = RECORD_TYPE_HEAD;
			else if (strcasecmp(record_type.c_str(), "BODY") == 0)
				record.record_type = RECORD_TYPE_BODY;
			else if (strcasecmp(record_type.c_str(), "TAIL") == 0)
				record.record_type = RECORD_TYPE_TAIL;
			else
				throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: inputs.input.record_type invalid")).str(APPLOCALE));

			record.delimiter = static_cast<char>(get_value(v_pt, "delimiter", 0));
			record.rule_condition = get_value(v_pt, "rule_condition", string(""));

			int field_serial = 0;
			input_field_t field;

			// 增加默认字段record_sn
			field.field_serial = field_serial++;
			field.factor1 = 0;
			field.factor2 = RECORD_SN_LEN;
			field.encode_type = ENCODE_ASCII;
			field.parent_serial = -1;
			field.field_name = "record_sn";
			field.column_name = field.field_name;
			field.inv_flag = false;

			record.field_vector.push_back(field);
			record.field_set.insert(field);
			input_map[field.field_name] = field.field_serial;

			BOOST_FOREACH(const bpt::iptree::value_type& vf, v_pt.get_child("fields")) {
				if (vf.first != "field")
					continue;

				const bpt::iptree& v_pf = vf.second;

				field.field_serial = field_serial++;

				string factor1 = get_value(v_pf, "factor1", string(""));
				if (strncasecmp(factor1.c_str(), "0x", 2) == 0)
					field.factor1 = ::strtol(factor1.c_str(), NULL, 16);
				else
					field.factor1 = ::atoi(factor1.c_str());

				if (field.factor1 < 0)
					throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: inputs.input.fields.field.factor1 must be zero or positive.")).str(APPLOCALE));

				field.factor2 = get_value(v_pf, "factor2", field.factor1);
				if (field.factor2 <= 0)
					throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: inputs.input.fields.field.factor2 must be positive.")).str(APPLOCALE));

				string encode_type = get_value(v_pf, "encode_type", string("ASCII"));
				if (strcasecmp(encode_type.c_str(), "ASCII") == 0)
					field.encode_type = ENCODE_ASCII;
				else if (strcasecmp(encode_type.c_str(), "INTEGER") == 0)
					field.encode_type = ENCODE_INTEGER;
				else if (strcasecmp(encode_type.c_str(), "OCTETSTRING") == 0)
					field.encode_type = ENCODE_OCTETSTRING;
				else if (strcasecmp(encode_type.c_str(), "BCD") == 0)
					field.encode_type = ENCODE_BCD;
				else if (strcasecmp(encode_type.c_str(), "TBCD") == 0)
					field.encode_type = ENCODE_TBCD;
				else if (strcasecmp(encode_type.c_str(), "ASN") == 0)
					field.encode_type = ENCODE_ASN;
				else if (strcasecmp(encode_type.c_str(), "ADDRSTRING") == 0)
					field.encode_type = ENCODE_ADDRSTRING;
				else if (strcasecmp(encode_type.c_str(), "SEQUENCEOF") == 0)
					field.encode_type = ENCODE_SEQUENCEOF;
				else if (strcasecmp(encode_type.c_str(), "REALBCD") == 0)
					field.encode_type = ENCODE_REALBCD;
				else if (strcasecmp(encode_type.c_str(), "REALTBCD") == 0)
					field.encode_type = ENCODE_REALTBCD;
				else if (strcasecmp(encode_type.c_str(), "BCDD") == 0)
					field.encode_type = ENCODE_BCDD;
				else if (strcasecmp(encode_type.c_str(), "UNINTEGER") == 0)
					field.encode_type = ENCODE_UNINTEGER;
				else
					throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: inputs.input.fields.field.encode_type invalid.")).str(APPLOCALE));

				field.parent_serial = get_value(v_pf, "parent_serial", -1);
				field.column_name = boost::to_lower_copy(v_pf.get<string>("field_name"));

				string inv_flag = get_value(v_pf, "inv_flag", string("N"));
				if (inv_flag == "Y")
					field.inv_flag = true;
				else
					field.inv_flag = false;

				field.field_name = table_name;
				field.field_name += ".";
				field.field_name += field.column_name;

				record.field_vector.push_back(field);
				record.field_set.insert(field);
				input_map[field.field_name] = field.field_serial;
			}

			if (record.field_vector.size() <= 1)
				throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: inputs.input.fields.field missing.")).str(APPLOCALE));

			input_maps.push_back(input_map);
		}
	} catch (bpt::ptree_bad_data& ex) {
		throw sg_exception(__FILE__, __LINE__, SGESYSTEM, 0, (_("ERROR: Parse section inputs failure, {1} ({2})") % ex.what() % ex.data<string>()).str(APPLOCALE));
	} catch (bpt::ptree_error& ex) {
		throw sg_exception(__FILE__, __LINE__, SGESYSTEM, 0, (_("ERROR: Parse section inputs failure, {1}") % ex.what()).str(APPLOCALE));
	}

	if (input_records.empty())
		throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: inputs.input missing.")).str(APPLOCALE));

	BOOST_FOREACH(const input_record_t& input_record, input_records) {
		const vector<input_field_t>& field_vector = input_record.field_vector;

		for (int i = 0; i < field_vector.size(); i++) {
			int len = field_vector[i].factor2;

			// 对于定长二进制格式，BCD编码的字段解析出来后需要* 2
			if (field_vector[i].encode_type != ENCODE_ASCII)
				len <<= 1;

			if (i < input_sizes.size()) {
				if (input_sizes[i] < len)
					input_sizes[i] = len;
			} else {
				input_sizes.push_back(len);
			}
		}
	}
}

void sa_config::load_output(const bpt::iptree& pt, sa_adaptor_t& adaptor)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(100, __PRETTY_FUNCTION__, "", NULL);
#endif
	vector<output_field_t>& output_fields = adaptor.output_fields;
	field_map_t& output_map = adaptor.output_map;
	int field_serial = 0;
	output_field_t field;

	output_fields.clear();
	output_map.clear();

	try {
		// 增加默认字段record_sn
		field.field_name = "record_sn";
		field.field_size = RECORD_SN_LEN;
		field.column_name = "record_sn";

		output_fields.push_back(field);
		output_map[field.field_name] = field_serial++;

		boost::optional<const bpt::iptree&> node = pt.get_child_optional("output");
		if (!node)
			return;

		BOOST_FOREACH(const bpt::iptree::value_type& v, *node) {
			if (v.first != "table")
				continue;

			const bpt::iptree& v_pt = v.second;

			string table_name = boost::to_lower_copy(v_pt.get<string>("name"));

			BOOST_FOREACH(const bpt::iptree::value_type& vf, v_pt.get_child("fields")) {
				if (vf.first != "field")
					continue;

				const bpt::iptree& v_pf = vf.second;

				field.column_name = boost::to_lower_copy(v_pf.get<string>("field_name"));

				field.field_name = table_name;
				field.field_name += ".";
				field.field_name += field.column_name;

				field.field_size = v_pf.get<int>("field_size");
				if (field.field_size <= 0)
					throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: output.fields.field.field_size must be positive.")).str(APPLOCALE));

				output_fields.push_back(field);
				output_map[field.field_name] = field_serial++;
			}
		}
	} catch (bpt::ptree_bad_data& ex) {
		throw sg_exception(__FILE__, __LINE__, SGESYSTEM, 0, (_("ERROR: Parse section output failure, {1} ({2})") % ex.what() % ex.data<string>()).str(APPLOCALE));
	} catch (bpt::ptree_error& ex) {
		throw sg_exception(__FILE__, __LINE__, SGESYSTEM, 0, (_("ERROR: Parse section output failure, {1}") % ex.what()).str(APPLOCALE));
	}
}

void sa_config::load_summary(const bpt::iptree& pt, sa_adaptor_t& adaptor)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(100, __PRETTY_FUNCTION__, "", NULL);
#endif
	gpenv& env_mgr = gpenv::instance();
	sa_summary_t& summary = adaptor.summary;
	vector<sa_summary_record_t>& records = summary.records;

	records.clear();

	try {
		boost::optional<const bpt::iptree&> node = pt.get_child_optional("summary");
		if (!node) {
			summary.class_name = "sa_output";
			return;
		}

		const bpt::iptree& v1 = *node;
		summary.class_name = v1.get<string>("class_name");

		BOOST_FOREACH(const bpt::iptree::value_type& v2, v1.get_child("items")) {
			if (v2.first != "item")
				continue;

			sa_summary_record_t record;
			record.rollback_type = ROLLBACK_BY_DELETE;
			record.del_flag = 0;

			BOOST_FOREACH(const bpt::iptree::value_type& v3, v2.second) {
				const string& node_name = v3.first;
				const bpt::iptree& v4 = v3.second;
				vector<sa_summary_field_t> *fields = NULL;
				bool in_key = false;

				if (node_name == "table_name") {
					env_mgr.expand_var(record.table_name, v4.get_value<string>());
					continue;
				} else if (node_name == "rollback") {
					string rollback_str = v4.get_value<string>();
					if (strcasecmp(rollback_str.c_str(), "NONE") == 0)
						record.rollback_type = ROLLBACK_BY_NONE;
					else if (strcasecmp(rollback_str.c_str(), "TRUNCATE") == 0)
						record.rollback_type = ROLLBACK_BY_TRUNCATE;
					else if (strcasecmp(rollback_str.c_str(), "DELETE") == 0)
						record.rollback_type = ROLLBACK_BY_DELETE;
					else if (strcasecmp(rollback_str.c_str(), "SCRIPT") == 0)
						record.rollback_type = ROLLBACK_BY_SCRIPT;
					else
						throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: summary.items.item.rollback invalid")).str(APPLOCALE));
				} else if (node_name == "rollback_script") {
					env_mgr.expand_var(record.rollback_script, v4.get_value<string>());
				} else if (node_name == "key") {
					fields = &record.keys;
					in_key = true;
				} else if (node_name == "value") {
					fields = &record.values;
				} else {
					continue;
				}

				BOOST_FOREACH(const bpt::iptree::value_type& v5, v4) {
					if (v5.first != "field")
						continue;

					sa_summary_field_t field;
					const bpt::iptree& v_pt = v5.second;

					field.field_name = boost::to_lower_copy(v_pt.get<string>("field_name"));

					if (!get_serial(field.field_name, adaptor, field.field_orign, field.field_serial))
						throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: summary.items.item.{1}.field.field_name {2} invalid") % node_name % field.field_name).str(APPLOCALE));

					if (in_key && field.field_orign == FIELD_ORIGN_GLOBAL) {
						if (field.field_serial == GLOBAL_FILE_NAME_SERIAL)
							record.del_flag |= DEL_BY_FILE_NAME;
						else if (field.field_serial == GLOBAL_FILE_SN_SERIAL)
							record.del_flag |= DEL_BY_FILE_SN;
					}

					field.column_name = boost::to_lower_copy(get_value(v_pt, "column_name", string("")));
					if (field.column_name.empty()) {
						string::size_type pos = field.field_name.rfind('.');
						if (pos == string::npos)
							field.column_name = field.field_name;
						else
							field.column_name = string(field.field_name.begin() + pos + 1, field.field_name.end());
					}

					string field_type = v_pt.get<string>("field_type");
					if (strcasecmp(field_type.c_str(), "INT") == 0) {
						field.field_type = FIELD_TYPE_INT;
						field.field_size = sizeof(int);
					} else if (strcasecmp(field_type.c_str(), "LONG") == 0) {
						field.field_type = FIELD_TYPE_LONG;
						field.field_size = sizeof(long);
					} else if (strcasecmp(field_type.c_str(), "FLOAT") == 0) {
						field.field_type = FIELD_TYPE_FLOAT;
						field.field_size = sizeof(float);
					} else if (strcasecmp(field_type.c_str(), "DOUBLE") == 0) {
						field.field_type = FIELD_TYPE_DOUBLE;
						field.field_size = sizeof(double);
					} else if (strcasecmp(field_type.c_str(), "STRING") == 0) {
						field.field_type = FIELD_TYPE_STRING;
						field.field_size = v_pt.get<int>("field_size");
						if (field.field_size <= 0)
							throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: summary.items.item.{1}.field.field_size must be positive") % node_name).str(APPLOCALE));
					} else {
						throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: summary.items.item.{1}.field.field_type invalid") % node_name).str(APPLOCALE));
					}

					string action = get_value(v_pt, "action", string("ADD"));
					if (strcasecmp(action.c_str(), "ASSIGN") == 0)
						field.action = SUMMARY_ACTION_ASSIGN;
					else if (strcasecmp(action.c_str(), "MIN") == 0)
						field.action = SUMMARY_ACTION_MIN;
					else if (strcasecmp(action.c_str(), "MAX") == 0)
						field.action = SUMMARY_ACTION_MAX;
					else if (strcasecmp(action.c_str(), "ADD") == 0)
						field.action = SUMMARY_ACTION_ADD;
					else
						throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: summary.items.item.{1}.field.field_size must be positive") % node_name).str(APPLOCALE));

					fields->push_back(field);
				}
			}

			if (record.del_flag & DEL_BY_FILE_SN)
				record.del_flag &= ~DEL_BY_FILE_NAME;

			records.push_back(record);
		}
	} catch (bpt::ptree_bad_data& ex) {
		throw sg_exception(__FILE__, __LINE__, SGESYSTEM, 0, (_("ERROR: Parse section summary failure, {1} ({2})") % ex.what() % ex.data<string>()).str(APPLOCALE));
	} catch (bpt::ptree_error& ex) {
		throw sg_exception(__FILE__, __LINE__, SGESYSTEM, 0, (_("ERROR: Parse section summary failure, {1}") % ex.what()).str(APPLOCALE));
	}
}

void sa_config::load_distribute(const bpt::iptree& pt, sa_adaptor_t& adaptor)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(100, __PRETTY_FUNCTION__, "", NULL);
#endif
	sa_distribute_t& distr = adaptor.distr;
	vector<dst_file_t>& dst_files = distr.dst_files;
	gpenv& env_mgr = gpenv::instance();
	boost::char_separator<char> sep(" \t\b");

	dst_files.clear();

	try {
		boost::optional<const bpt::iptree&> node = pt.get_child_optional("distribute");
		if (!node) {
			distr.class_name = "sa_output";
			return;
		}

		const bpt::iptree& v1 = *node;

		distr.class_name = v1.get<string>("class_name");
		distr.max_open_files = get_value(v1, "max_open_files", DFLT_MAX_OPEN_FILES);
		if (distr.max_open_files <= 0)
			throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: distribute.max_open_files must be positive.")).str(APPLOCALE));

		BOOST_FOREACH(const bpt::iptree::value_type& v2, v1.get_child("items")) {
			if (v2.first != "item")
				continue;

			const bpt::iptree& v_pt = v2.second;
			dst_files.push_back(dst_file_t());
			dst_file_t& item = *dst_files.rbegin();

			env_mgr.expand_var(item.rule_condition, get_value(v_pt, "rule_condition", string("")));
			env_mgr.expand_var(item.rule_file_name, v_pt.get<string>("rule_file_name"));

			string dst_dir = v_pt.get<string>("dst_dir");
			env_mgr.expand_var(item.dst_dir, dst_dir);
			if (*item.dst_dir.rbegin() != '/')
				item.dst_dir += '/';

			string rdst_dir = get_value(v_pt, "rdst_dir", string(""));
			env_mgr.expand_var(item.rdst_dir, rdst_dir);
			if (!item.rdst_dir.empty()) {
				if (*item.rdst_dir.rbegin() != '/')
					item.rdst_dir += '/';

				if (!common::parse(item.rdst_dir, item.sftp_prefix))
					throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: distribute.items.item.rdst_dir invalid")).str(APPLOCALE));

				// 对于本地协议，直接忽略
				if (common::is_local(item.sftp_prefix)) {
					item.sftp_prefix.clear();
					item.dst_dir = item.rdst_dir;
				}
			}

			env_mgr.expand_var(item.pattern, get_value(v_pt, "pattern", (boost::format("^.*%1%.*$") % adaptor.parms.svc_key).str()));

			item.options = 0;
			string options = get_value(v_pt, "options", string(""));
			tokenizer tokens(options, sep);
			for (tokenizer::iterator iter = tokens.begin(); iter != tokens.end(); ++iter) {
				string str = *iter;

				if (strcasecmp(str.c_str(), "INTERNAL") == 0)
					item.options |= SA_OPTION_INTERNAL;
				else if (strcasecmp(str.c_str(), "SAME_FILE") == 0)
					item.options |= SA_OPTION_SAME_FILE;
				else if (strcasecmp(str.c_str(), "SPLIT") == 0)
					item.options |= SA_OPTION_SPLIT;
			}

			for (int i = 0; i < 3; i++) {
				string node_name;
				dst_record_t *dst_record = NULL;

				switch (i) {
				case 0:
					node_name = "head";
					dst_record = &item.head;
					break;
				case 1:
					node_name = "body";
					dst_record = &item.body;
					break;
				case 2:
					node_name = "tail";
					dst_record = &item.tail;
					break;
				default:
					break;
				}

				boost::optional<const bpt::iptree&> node = v_pt.get_child_optional(node_name);
				if (!node)
					continue;

				const bpt::iptree& vf = *node;

				dst_record->delimiter = static_cast<char>(get_value(vf, "delimiter", 0));
				dst_record->has_return = false;
				dst_record->is_fixed = false;

				options = get_value(vf, "options", string(""));
				tokenizer tokens2(options, sep);
				for (tokenizer::iterator iter = tokens2.begin(); iter != tokens2.end(); ++iter) {
					string str = *iter;

					if (strcasecmp(str.c_str(), "RETURN") == 0)
						dst_record->has_return = true;
					else if (strcasecmp(str.c_str(), "FIXED") == 0)
						dst_record->is_fixed = true;
				}

				BOOST_FOREACH(const bpt::iptree::value_type& v, vf.get_child("fields")) {
					if (v.first != "field")
						continue;

					const bpt::iptree& v_pf = v.second;
					dst_field_t field;
					string field_name = boost::to_lower_copy(v_pf.get<string>("field_name"));

					if (!get_serial(field_name, adaptor, field.field_orign, field.field_serial))
						throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: distribute.items.item.{1}.fields.field.field_name {2} invalid") % node_name % field_name).str(APPLOCALE));

					string field_type = get_value(v_pf, "field_type", string("STRING"));
					if (strcasecmp(field_type.c_str(), "INT") == 0)
						field.field_type = FIELD_TYPE_INT;
					else if (strcasecmp(field_type.c_str(), "LONG") == 0)
						field.field_type = FIELD_TYPE_LONG;
					else if (strcasecmp(field_type.c_str(), "FLOAT") == 0)
						field.field_type = FIELD_TYPE_FLOAT;
					else if (strcasecmp(field_type.c_str(), "DOUBLE") == 0)
						field.field_type = FIELD_TYPE_DOUBLE;
					else if (strcasecmp(field_type.c_str(), "STRING") == 0)
						field.field_type = FIELD_TYPE_STRING;
					else
						throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: distribute.items.item.{1}.fields.field.field_type invalid") % node_name).str(APPLOCALE));

					string align_mode = get_value(v_pf, "align_mode", string("INTERNAL"));
					if (strcasecmp(align_mode.c_str(), "LEFT") == 0)
						field.align_mode = ALIGN_MODE_LEFT;
					else if (strcasecmp(align_mode.c_str(), "INTERNAL") == 0)
						field.align_mode = ALIGN_MODE_INTERNAL;
					else if (strcasecmp(align_mode.c_str(), "RIGHT") == 0)
						field.align_mode = ALIGN_MODE_RIGHT;
					else
						throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: distribute.items.item.{1}.fields.field.align_mode invalid") % node_name).str(APPLOCALE));

					field.field_size = get_value(v_pf, "field_size", 0);
					if (field.field_size < 0)
						throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: distribute.items.item.{1}.fields.field.field_size must be zero or positive") % node_name).str(APPLOCALE));

					field.fill = get_value(v_pf, "fill", ' ');
					field.precision = get_value(v_pf, "precision", 0);
					if (field.precision < 0)
						throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: distribute.items.item.{1}.fields.field.precision must be zero or positive") % node_name).str(APPLOCALE));

					string action = get_value(v_pf, "action", string(""));
					if (action.empty())
						field.action = ACTION_NONE;
					else if (strcasecmp(action.c_str(), "ADD_INT") == 0)
						field.action = ADD_INT;
					else if (strcasecmp(action.c_str(), "ADD_LONG") == 0)
						field.action = ADD_LONG;
					else if (strcasecmp(action.c_str(), "ADD_FLOAT") == 0)
						field.action = ADD_FLOAT;
					else if (strcasecmp(action.c_str(), "ADD_DOUBLE") == 0)
						field.action = ADD_DOUBLE;
					else if (strcasecmp(action.c_str(), "INC_INT") == 0)
						field.action = INC_INT;
					else if (strcasecmp(action.c_str(), "MIN_INT") == 0)
						field.action = MIN_INT;
					else if (strcasecmp(action.c_str(), "MIN_LONG") == 0)
						field.action = MIN_LONG;
					else if (strcasecmp(action.c_str(), "MIN_FLOAT") == 0)
						field.action = MIN_FLOAT;
					else if (strcasecmp(action.c_str(), "MIN_DOUBLE") == 0)
						field.action = MIN_DOUBLE;
					else if (strcasecmp(action.c_str(), "MIN_STR") == 0)
						field.action = MIN_STR;
					else if (strcasecmp(action.c_str(), "MAX_INT") == 0)
						field.action = MAX_INT;
					else if (strcasecmp(action.c_str(), "MAX_LONG") == 0)
						field.action = MAX_LONG;
					else if (strcasecmp(action.c_str(), "MAX_FLOAT") == 0)
						field.action = MAX_FLOAT;
					else if (strcasecmp(action.c_str(), "MAX_DOUBLE") == 0)
						field.action = MAX_DOUBLE;
					else if (strcasecmp(action.c_str(), "MAX_STR") == 0)
						field.action = MAX_STR;
					else if (strcasecmp(action.c_str(), "SPLIT_FIELD") == 0)
						field.action = SPLIT_FIELD;
					else
						throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: distribute.items.item.{1}.fields.field.action invalid") % node_name).str(APPLOCALE));

					dst_record->fields.push_back(field);
				}
			}
		}
	} catch (bpt::ptree_bad_data& ex) {
		throw sg_exception(__FILE__, __LINE__, SGESYSTEM, 0, (_("ERROR: Parse section distribute failure, {1} ({2})") % ex.what() % ex.data<string>()).str(APPLOCALE));
	} catch (bpt::ptree_error& ex) {
		throw sg_exception(__FILE__, __LINE__, SGESYSTEM, 0, (_("ERROR: Parse section distribute failure, {1}") % ex.what()).str(APPLOCALE));
	}
}

void sa_config::load_stat(const bpt::iptree& pt, sa_adaptor_t& adaptor)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(100, __PRETTY_FUNCTION__, "", NULL);
#endif
	sa_stat_t& stat = adaptor.stat;
	vector<sa_stat_record_t>& records = stat.records;
	boost::char_separator<char> sep(" \t\b");
	gpenv& env_mgr = gpenv::instance();

	records.clear();

	try {
		boost::optional<const bpt::iptree&> node = pt.get_child_optional("stat");
		if (!node) {
			stat.class_name = "sa_output";
			return;
		}

		const bpt::iptree& v1 = *node;

		stat.class_name = v1.get<string>("class_name");

		BOOST_FOREACH(const bpt::iptree::value_type& v2, v1.get_child("items")) {
			if (v2.first != "item")
				continue;

			const bpt::iptree& v_pt = v2.second;
			records.push_back(sa_stat_record_t());
			sa_stat_record_t& item = *records.rbegin();

			item.stat_action = 0;
			string options = get_value(v_pt, "options", string(""));
			tokenizer tokens(options, sep);
			for (tokenizer::iterator iter = tokens.begin(); iter != tokens.end(); ++iter) {
				string str = *iter;

				if (strcasecmp(str.c_str(), "INSERT") == 0)
					item.stat_action |= STAT_ACTION_INSERT;
				else if (strcasecmp(str.c_str(), "UPDATE") == 0)
					item.stat_action |= STAT_ACTION_UPDATE;
			}

			string sort_str = get_value(v_pt, "sort", string("Y"));
			if (strcasecmp(sort_str.c_str(), "Y") == 0)
				item.sort = true;
			else if (strcasecmp(sort_str.c_str(), "N") == 0)
				item.sort = false;
			else
				throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: stat.sort invalid")).str(APPLOCALE));

			env_mgr.expand_var(item.rule_condition, get_value(v_pt, "rule_condition", string("")));
			env_mgr.expand_var(item.rule_table_name, get_value(v_pt, "rule_table_name", string("")));

			item.rollback_type = ROLLBACK_BY_DELETE;
			item.del_flag = 0;

			env_mgr.expand_var(item.table_name, get_value(v_pt, "table_name", string("")));
			env_mgr.expand_var(item.rollback_script, get_value(v_pt, "rollback_script", string("")));

			string rollback_str = get_value(v_pt, "rollback", string("NONE"));
			if (strcasecmp(rollback_str.c_str(), "NONE") == 0)
				item.rollback_type = ROLLBACK_BY_NONE;
			else if (strcasecmp(rollback_str.c_str(), "TRUNCATE") == 0)
				item.rollback_type = ROLLBACK_BY_TRUNCATE;
			else if (strcasecmp(rollback_str.c_str(), "DELETE") == 0)
				item.rollback_type = ROLLBACK_BY_DELETE;
			else if (strcasecmp(rollback_str.c_str(), "SCRIPT") == 0)
				item.rollback_type = ROLLBACK_BY_SCRIPT;
			else
				throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: stat.items.item.rollback invalid")).str(APPLOCALE));

			item.same_struct = true;
			BOOST_FOREACH(const bpt::iptree::value_type& v, v_pt.get_child("fields")) {
				if (v.first != "field")
					continue;

				const bpt::iptree& v_pf = v.second;
				sa_stat_field_t field;
				string field_name = boost::to_lower_copy(get_value(v_pf, "field_name", string("")));

				if (field_name.empty()) {
					field.field_serial = -1;
					field.field_size = 0;
				} else {
					if (!get_serial(field_name, adaptor, field.field_orign, field.field_serial))
						throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: stat.fields.field.field_name {1} invalid") % field_name).str(APPLOCALE));

					switch (field.field_orign) {
					case FIELD_ORIGN_INPUT:
						field.field_size = adaptor.input_sizes[field.field_serial];
						break;
					case FIELD_ORIGN_GLOBAL:
						field.field_size = SAP->_SAP_global.global_fields[field.field_serial].field_size;
						break;
					case FIELD_ORIGN_OUTPUT:
						field.field_size = adaptor.output_fields[field.field_serial].field_size;
						break;
					default:
						throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: unknown field orign {1} for field_serial {2}") % field.field_orign % field.field_serial).str(APPLOCALE));
					}
				}

				field.element_name = get_value(v_pf, "element_name", field_name);
				field.operation = get_value(v_pf, "operation", string("="));
				if (field.operation.empty())
					field.operation = "=";

				string desc = ":";
				desc += field.element_name;

				string insert_desc;
				env_mgr.expand_var(insert_desc, get_value(v_pf, "insert_desc", desc));
				replace_desc(insert_desc, field.element_name, field.field_size, field.insert_desc, field.insert_times);

				string update_desc;
				env_mgr.expand_var(update_desc, get_value(v_pf, "update_desc", desc));
				replace_desc(update_desc, field.element_name, field.field_size, field.update_desc, field.update_times);

				if (field.insert_times != field.update_times)
					item.same_struct = false;

				string is_key = get_value(v_pf, "is_key", string("N"));
				if (strcasecmp(is_key.c_str(), "Y") == 0) {
					item.keys.push_back(field);

					if (field.field_orign == FIELD_ORIGN_GLOBAL) {
						if (field.field_serial == GLOBAL_FILE_NAME_SERIAL)
							item.del_flag |= DEL_BY_FILE_NAME;
						else if (field.field_serial == GLOBAL_FILE_SN_SERIAL)
							item.del_flag |= DEL_BY_FILE_SN;
					}
				} else if (strcasecmp(is_key.c_str(), "N") == 0) {
					item.values.push_back(field);
				} else {
					throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: stat.fields.field.is_key invalid")).str(APPLOCALE));
				}
			}

			if (item.keys.empty()) {
				if (item.stat_action & STAT_ACTION_UPDATE)
					throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: key not defined for stat.fields")).str(APPLOCALE));

				if (item.sort) {
					GPP->write_log((_("WARN: sort option is on, but no keys provided, turn it off")).str(APPLOCALE));
					item.sort = false;
				}
			}
		}
	} catch (bpt::ptree_bad_data& ex) {
		throw sg_exception(__FILE__, __LINE__, SGESYSTEM, 0, (_("ERROR: Parse section distribute failure, {1} ({2})") % ex.what() % ex.data<string>()).str(APPLOCALE));
	} catch (bpt::ptree_error& ex) {
		throw sg_exception(__FILE__, __LINE__, SGESYSTEM, 0, (_("ERROR: Parse section distribute failure, {1}") % ex.what()).str(APPLOCALE));
	}
}

void sa_config::load_args(const bpt::iptree& pt, sa_adaptor_t& adaptor)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(100, __PRETTY_FUNCTION__, "", NULL);
#endif
	map<string, string>& args = adaptor.args;

	try {
		boost::optional<const bpt::iptree&> node = pt.get_child_optional("args");
		if (!node)
			return;

		BOOST_FOREACH(const bpt::iptree::value_type& v, *node) {
			if (v.first != "arg")
				continue;

			const bpt::iptree& v_pt = v.second;

			string name = v_pt.get<string>("name");
			name = boost::to_lower_copy(name);
			string value = v_pt.get<string>("value");

			if (args.find(name) != args.end())
				throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: Duplicate {1} for args.arg") % name).str(APPLOCALE));

			args[name] = value;
		}
	} catch (bpt::ptree_bad_data& ex) {
		throw sg_exception(__FILE__, __LINE__, SGESYSTEM, 0, (_("ERROR: Parse section args failure, {1} ({2})") % ex.what() % ex.data<string>()).str(APPLOCALE));
	} catch (bpt::ptree_error& ex) {
		throw sg_exception(__FILE__, __LINE__, SGESYSTEM, 0, (_("ERROR: Parse section args failure, {1}") % ex.what()).str(APPLOCALE));
	}
}

void sa_config::replace_desc(const string& src, const string& element_name, int field_size, string& dst, int& dst_times)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(100, __PRETTY_FUNCTION__, "", NULL);
#endif

	dst_times = 0;

	for (int i = 0; i < src.size();) {
		char ch = src[i];

		if (ch == '\'') {
			dst += ch;
			for (i++; i < src.size(); i++) {
				ch = src[i];
				dst += ch;
				if (ch == '\'')
					break;
			}
		} else if (ch == '\"') {
			dst += ch;
			for (i++; i < src.size(); i++) {
				ch = src[i];
				dst += ch;
				if (ch == '\"')
					break;
			}
		} else if (ch == ':') {
			dst += ch;
			for (i++; i < src.size(); i++) {
				ch = src[i];
				if (!isalnum(ch) && ch != '_') {
					i--;
					break;
				}
			}

			dst += element_name;
			dst += "{char[";
			dst += boost::lexical_cast<string>(field_size);
			dst += "]}";
			dst_times++;
		} else {
			dst += ch;
		}

		i++;
	}
}

bool sa_config::get_serial(const string& field_name, const sa_adaptor_t& adaptor, field_orign_enum& field_orign, int& field_serial)
{
	bool retval = false;
#if defined(DEBUG)
	scoped_debug<bool> debug(100, __PRETTY_FUNCTION__, (_("field_name={1}") % field_name).str(APPLOCALE), &retval);
#endif

	field_map_t::const_iterator iter;

	const field_map_t& global_fields = adaptor.global_map;
	iter = global_fields.find(field_name);
	if (iter != global_fields.end()) {
		field_orign = FIELD_ORIGN_GLOBAL;
		field_serial = iter->second;
		retval = true;
		return retval;
	}

	if (adaptor.input_maps.size() == 1) {
		const field_map_t& input_fields = adaptor.input_maps[0];
		iter = input_fields.find(field_name);
		if (iter != input_fields.end()) {
			field_orign = FIELD_ORIGN_INPUT;
			field_serial = iter->second;
			retval = true;
			return retval;
		}
	}

	const field_map_t& output_fields = adaptor.output_map;
	iter = output_fields.find(field_name);
	if (iter != output_fields.end()) {
		field_orign = FIELD_ORIGN_OUTPUT;
		field_serial = iter->second;
		retval = true;
		return retval;
	}

	return retval;
}

}
}

