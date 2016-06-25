#include "bps_ctx.h"

typedef boost::tokenizer<boost::char_separator<char> > tokenizer;

namespace ai
{
namespace app
{

int const DFLT_VERSION = -1;

using namespace ai::sg;

boost::once_flag bps_ctx::once_flag = BOOST_ONCE_INIT;
bps_ctx * bps_ctx::_instance = NULL;

bps_ctx * bps_ctx::instance()
{
	if (_instance == NULL)
		boost::call_once(once_flag, bps_ctx::init_once);
	return _instance;
}

bool bps_ctx::get_config()
{
	string config;
	sg_api& api_mgr = sg_api::instance();
	gpp_ctx *GPP = gpp_ctx::instance();
	sgt_ctx *SGT = sgt_ctx::instance();
	bool retval = false;
#if defined(DEBUG)
	scoped_debug<bool> debug(1000, __PRETTY_FUNCTION__, "", &retval);
#endif

	// 加载svc_key列表
	if (!_BPS_svc_file.empty()) {
		// 获取svc_key列表文件
		if (!api_mgr.get_config(config, _BPS_svc_file, _BPS_version)) {
			SGT->_SGT_error_code = SGESYSTEM;
			GPP->write_log(__FILE__, __LINE__, (_("ERROR: Failure to load BPS configuration, svc_key={1}") % _BPS_svc_file).str(APPLOCALE));
			return retval;
		}

		boost::char_separator<char> sep(" \t\b\n");
		tokenizer tokens(config, sep);
		for (tokenizer::iterator iter = tokens.begin(); iter != tokens.end(); ++iter) {
			string svc_key = *iter;
			if (svc_key.empty())
				continue;

			_BPS_svc_keys.push_back(svc_key);
		}
	}

	// 需要进行排序，后面引用时需要依赖顺序
	std::sort(_BPS_svc_keys.begin(), _BPS_svc_keys.end());

	BOOST_FOREACH(const string& svc_key, _BPS_svc_keys) {
		string key = string("BPS.") + svc_key;

		// 获取BPS配置
		if (!api_mgr.get_config(config, key, _BPS_version)) {
			SGT->_SGT_error_code = SGESYSTEM;
			GPP->write_log(__FILE__, __LINE__, (_("ERROR: Failure to load BPS configuration, svc_key={1}") % svc_key).str(APPLOCALE));
			return retval;
		}

		// 加载配置
		try {
			load_config(config);
		} catch (exception& ex) {
			SGT->_SGT_error_code = SGESYSTEM;
			GPP->write_log(__FILE__, __LINE__, ex.what());
			return retval;
		}
	}

	retval = true;
	return retval;
}

void bps_ctx::set_svcname(const string& svc_key, string& svc_name) const
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	svc_name = "BPS_";
	svc_name += svc_key;
}

bps_ctx::bps_ctx()
{
	_BPS_version = -1;
	_BPS_cmpl = NULL;
	_BPS_use_dbc = false;
	_BPS_data_mgr = NULL;
}

bps_ctx::~bps_ctx()
{
}

void bps_ctx::load_config(const string& config)
{
	bpt::iptree pt;
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	istringstream is(config);
	bpt::read_xml(is, pt);

	load_services(pt);
}

void bps_ctx::load_services(const bpt::iptree& pt)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif
	svcp_ctx *SVCP = svcp_ctx::instance();

	try {
		BOOST_FOREACH(const bpt::iptree::value_type& v, pt.get_child("services")) {
			if (v.first != "service")
				continue;

			const bpt::iptree& v_pt = v.second;
			svc_adaptor_t adaptor;

			load_parms(v_pt, adaptor);

			if (!adaptor.parms.disable_global)
				load_global(v_pt, adaptor);

			load_input(v_pt, adaptor);
			load_output(v_pt, adaptor);

			bps_adaptor_t bps_adaptor;
			load_process(v_pt, bps_adaptor);

			SVCP->_SVCP_adaptors[adaptor.parms.svc_key] = adaptor;
			_BPS_adaptors[adaptor.parms.svc_key] = bps_adaptor;
		}
	} catch (bpt::ptree_bad_data& ex) {
		throw sg_exception(__FILE__, __LINE__, SGESYSTEM, 0, (_("ERROR: Parse section services failure, {1} ({2})") % ex.what() % ex.data<string>()).str(APPLOCALE));
	} catch (bpt::ptree_error& ex) {
		throw sg_exception(__FILE__, __LINE__, SGESYSTEM, 0, (_("ERROR: Parse section services failure, {1}") % ex.what()).str(APPLOCALE));
	}

	if (SVCP->_SVCP_adaptors.empty())
		throw sg_exception(__FILE__, __LINE__, SGESYSTEM, 0, (_("ERROR: services.service missing.")).str(APPLOCALE));
}

void bps_ctx::load_parms(const bpt::iptree& pt, svc_adaptor_t& adaptor)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(100, __PRETTY_FUNCTION__, "", NULL);
#endif
	svc_parms_t& parms = adaptor.parms;

	try {
		const bpt::iptree& v_pt = pt.get_child("resource");

		parms.svc_key = v_pt.get<string>("svc_key");
		parms.version = get_value(v_pt, "version", DFLT_VERSION);

		string disable_global = get_value(v_pt, "disable_global", string("N"));
		if (strcasecmp(disable_global.c_str(), "Y") == 0)
			parms.disable_global = true;
		else if (strcasecmp(disable_global.c_str(), "N") == 0)
			parms.disable_global = false;
		else
			throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: resource.disable_global invalid")).str(APPLOCALE));
	} catch (bpt::ptree_bad_data& ex) {
		throw sg_exception(__FILE__, __LINE__, SGESYSTEM, 0, (_("ERROR: Parse section resource failure, {1} ({2})") % ex.what() % ex.data<string>()).str(APPLOCALE));
	} catch (bpt::ptree_error& ex) {
		throw sg_exception(__FILE__, __LINE__, SGESYSTEM, 0, (_("ERROR: Parse section resource failure, {1}") % ex.what()).str(APPLOCALE));
	}
}

void bps_ctx::load_global(const bpt::iptree& pt, svc_adaptor_t& adaptor)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(100, __PRETTY_FUNCTION__, "", NULL);
#endif
	field_map_t& global_map = adaptor.global_map;
	vector<int>& input_globals = adaptor.input_globals;
	vector<int>& output_globals = adaptor.output_globals;
	vector<int>& global_sizes = adaptor.global_sizes;
	int field_serial = 0;

	global_map.clear();
	input_globals.clear();
	output_globals.clear();
	global_sizes.clear();

	global_map["svc_key"] = field_serial;
	input_globals.push_back(field_serial);
	global_sizes.push_back(SVC_KEY_LEN);
	field_serial++;

	global_map["file_name"] = field_serial;
	input_globals.push_back(field_serial);
	global_sizes.push_back(FILE_NAME_LEN);
	field_serial++;

	global_map["file_sn"] = field_serial;
	input_globals.push_back(field_serial);
	global_sizes.push_back(FILE_SN_LEN);
	field_serial++;

	global_map["redo_mark"] = field_serial;
	input_globals.push_back(field_serial);
	global_sizes.push_back(REDO_MARK_LEN);
	field_serial++;

	global_map["sysdate"] = field_serial;
	input_globals.push_back(field_serial);
	global_sizes.push_back(SYSDATE_LEN);
	field_serial++;

	global_map["first_serial"] = field_serial;
	input_globals.push_back(field_serial);
	global_sizes.push_back(SERIAL_LEN);
	field_serial++;

	global_map["first_serial"] = field_serial;
	input_globals.push_back(field_serial);
	global_sizes.push_back(SERIAL_LEN);
	field_serial++;

	try {
		boost::optional<const bpt::iptree&> node = pt.get_child_optional("global.fields");
		if (!node)
			return;

		BOOST_FOREACH(const bpt::iptree::value_type& v, *node) {
			if (v.first != "field")
				continue;

			const bpt::iptree& v_pt = v.second;

			string field_name = boost::to_lower_copy(v_pt.get<string>("field_name"));
			if (field_name.empty())
				throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: global.field_name is empty")).str(APPLOCALE));

			int field_size = v_pt.get<int>("field_size");
			if (field_size <= 0)
				throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: global.field_size must be positive")).str(APPLOCALE));

			bool readonly;
			string readonly_str = get_value(v_pt, "readonly", string("N"));
			if (strcasecmp(readonly_str.c_str(), "Y") == 0)
				readonly = true;
			else if (strcasecmp(readonly_str.c_str(), "N") == 0)
				readonly = false;
			else
				throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: global.readonly invalid")).str(APPLOCALE));

			field_map_t::const_iterator iter = global_map.find(field_name);
			if (iter != global_map.end())
				throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: duplicate global.field_name {1}") % field_name).str(APPLOCALE));

			global_map[field_name] = field_serial;
			input_globals.push_back(field_serial);
			global_sizes.push_back(field_size);
			if (!readonly)
				output_globals.push_back(field_serial);
			field_serial++;
		}
	} catch (bpt::ptree_bad_data& ex) {
		throw sg_exception(__FILE__, __LINE__, SGESYSTEM, 0, (_("ERROR: Parse section global failure, {1} ({2})") % ex.what() % ex.data<string>()).str(APPLOCALE));
	} catch (bpt::ptree_error& ex) {
		throw sg_exception(__FILE__, __LINE__, SGESYSTEM, 0, (_("ERROR: Parse section global failure, {1}") % ex.what()).str(APPLOCALE));
	}
}

void bps_ctx::load_input(const bpt::iptree& pt, svc_adaptor_t& adaptor)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif
	vector<field_map_t>& input_maps = adaptor.input_maps;
	vector<int>& input_sizes = adaptor.input_sizes;

	input_maps.clear();
	input_sizes.clear();

	try {
		BOOST_FOREACH(const bpt::iptree::value_type& v, pt.get_child("input")) {
			if (v.first != "table")
				continue;

			const bpt::iptree& v_pt = v.second;
			field_map_t input_map;
			int field_serial = 0;

			// 增加默认字段record_sn
			input_map["record_sn"] = field_serial++;
			if (input_sizes.empty())
				input_sizes.push_back(RECORD_SN_LEN);

			string table_name = boost::to_lower_copy(v_pt.get<string>("name"));

			BOOST_FOREACH(const bpt::iptree::value_type& vf, v_pt.get_child("fields")) {
				if (vf.first != "field")
					continue;

				const bpt::iptree& v_pf = vf.second;

				string field_name = table_name;
				field_name += ".";
				field_name += boost::to_lower_copy(v_pf.get<string>("field_name"));

				int field_size = v_pf.get<int>("field_size");
				if (field_size <= 0)
					throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: input.table.fields.field.field_size must be positive.")).str(APPLOCALE));

				input_map[field_name] = field_serial;

				if (field_serial < input_sizes.size()) {
					if (input_sizes[field_serial] < field_size)
						input_sizes[field_serial] = field_size;
				} else {
					input_sizes.push_back(field_size);
				}

				field_serial++;
			}

			if (input_map.empty())
				throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: input.table.fields.field missing.")).str(APPLOCALE));

			input_maps.push_back(input_map);
		}
	} catch (bpt::ptree_bad_data& ex) {
		throw sg_exception(__FILE__, __LINE__, SGESYSTEM, 0, (_("ERROR: Parse section input failure, {1} ({2})") % ex.what() % ex.data<string>()).str(APPLOCALE));
	} catch (bpt::ptree_error& ex) {
		throw sg_exception(__FILE__, __LINE__, SGESYSTEM, 0, (_("ERROR: Parse section input failure, {1}") % ex.what()).str(APPLOCALE));
	}

	if (input_maps.empty())
		throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: input.table missing.")).str(APPLOCALE));
}

void bps_ctx::load_output(const bpt::iptree& pt, svc_adaptor_t& adaptor)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif
	field_map_t& output_map = adaptor.output_map;
	vector<int>& output_sizes = adaptor.output_sizes;
	int field_serial = 0;

	output_sizes.clear();
	output_map.clear();

	try {
		BOOST_FOREACH(const bpt::iptree::value_type& v, pt.get_child("output")) {
			if (v.first != "table")
				continue;

			const bpt::iptree& v_pt = v.second;

			string table_name = boost::to_lower_copy(v_pt.get<string>("name"));

			BOOST_FOREACH(const bpt::iptree::value_type& vf, v_pt.get_child("fields")) {
				if (vf.first != "field")
					continue;

				const bpt::iptree& v_pf = vf.second;

				string field_name = table_name;
				field_name += ".";
				field_name += boost::to_lower_copy(v_pf.get<string>("field_name"));

				int field_size = v_pf.get<int>("field_size");
				if (field_size <= 0)
					throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: output.table.fields.field.field_size must be positive.")).str(APPLOCALE));

				output_map[field_name] = field_serial++;
				output_sizes.push_back(field_size);
			}
		}
	} catch (bpt::ptree_bad_data& ex) {
		throw sg_exception(__FILE__, __LINE__, SGESYSTEM, 0, (_("ERROR: Parse section output failure, {1} ({2})") % ex.what() % ex.data<string>()).str(APPLOCALE));
	} catch (bpt::ptree_error& ex) {
		throw sg_exception(__FILE__, __LINE__, SGESYSTEM, 0, (_("ERROR: Parse section output failure, {1}") % ex.what()).str(APPLOCALE));
	}
}

void bps_ctx::load_process(const bpt::iptree& pt, bps_adaptor_t& bps_adaptor)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif
	vector<bps_process_t>& processes = bps_adaptor.processes;
	vector<int>& indice = bps_adaptor.indice;
	vector<compiler::func_type>& funs = bps_adaptor.funs;

	processes.clear();
	indice.clear();
	funs.clear();

	try {
		BOOST_FOREACH(const bpt::iptree::value_type& v, pt.get_child("processes")) {
			if (v.first != "process")
				continue;

			const bpt::iptree& v_pt = v.second;
			processes.push_back(bps_process_t());
			bps_process_t& process = *processes.rbegin();
			vector<string>& rules = process.rules;

			BOOST_FOREACH(const bpt::iptree::value_type& vf, v_pt) {
				if (vf.first != "rule")
					continue;

				const bpt::iptree& v_pf = vf.second;

				rules.push_back(v_pf.get_value<string>());
			}
		}
	} catch (bpt::ptree_bad_data& ex) {
		throw sg_exception(__FILE__, __LINE__, SGESYSTEM, 0, (_("ERROR: Parse section processes failure, {1} ({2})") % ex.what() % ex.data<string>()).str(APPLOCALE));
	} catch (bpt::ptree_error& ex) {
		throw sg_exception(__FILE__, __LINE__, SGESYSTEM, 0, (_("ERROR: Parse section processes failure, {1}") % ex.what()).str(APPLOCALE));
	}
}

void bps_ctx::init_once()
{
	_instance = new bps_ctx();
}

}
}

