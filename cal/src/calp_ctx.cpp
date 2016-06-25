#include "calp_ctx.h"
#include "sap_ctx.h"

typedef boost::tokenizer<boost::char_separator<char> > tokenizer;

namespace ai
{
namespace app
{

using namespace ai::sg;
using namespace ai::scci;

boost::once_flag calp_ctx::once_flag = BOOST_ONCE_INIT;
calp_ctx * calp_ctx::_instance = NULL;

calp_ctx * calp_ctx::instance()
{
	if (_instance == NULL)
		boost::call_once(once_flag, calp_ctx::init_once);
	return _instance;
}

bool calp_ctx::get_config(const string& svc_key, int version)
{
	string config;
	sg_api& api_mgr = sg_api::instance();
	gpp_ctx *GPP = gpp_ctx::instance();
	sgt_ctx *SGT = sgt_ctx::instance();
	bool retval = false;
#if defined(DEBUG)
	scoped_debug<bool> debug(1000, __PRETTY_FUNCTION__, "", &retval);
#endif

	string key = string("CAL.") + svc_key;
	// 获取CAL配置
	if (!api_mgr.get_config(config, key, version)) {
		SGT->_SGT_error_code = SGESYSTEM;
		GPP->write_log(__FILE__, __LINE__, (_("ERROR: Failure to load CAL configuration")).str(APPLOCALE));
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

	if (!get_partitions())
		return retval;

	retval = true;
	return retval;
}

calp_ctx::calp_ctx()
{
	gpenv& env_mgr = gpenv::instance();

	char *ptr  = env_mgr.getenv("UNICAL_RUN_COMPLEX");
	if (ptr == NULL || strcasecmp(ptr, "Y") != 0)
		_CALP_run_complex = false;
	else
		_CALP_run_complex = true;

	ptr = env_mgr.getenv("UNICAL_INV_OUT");
	if (ptr == NULL || strcasecmp(ptr, "Y") != 0)
		_CALP_inv_out= false;
	else
		_CALP_inv_out = true;

	ptr = env_mgr.getenv("UNICAL_COLLECT_DRIVER");
	if (ptr == NULL || strcasecmp(ptr, "Y") != 0)
		_CALP_collect_driver = false;
	else
		_CALP_collect_driver = true;

	ptr = env_mgr.getenv("UNICAL_MOD_IDS");
	if (ptr != NULL) {
		string str = ptr;
		boost::char_separator<char> sep(" \t\b");
		tokenizer tokens(str, sep);

		for (tokenizer::iterator iter = tokens.begin(); iter != tokens.end(); ++iter) {
			string tmp_str = *iter;
			_CALP_env_mod_ids.insert(atoi(tmp_str.c_str()));
		}
	}

	ptr = env_mgr.getenv("UNICAL_DEBUG_INV");
	if (ptr == NULL || strcasecmp(ptr, "Y") != 0)
		_CALP_debug_inv = false;
	else
		_CALP_debug_inv = true;

	ptr = env_mgr.getenv("UNICAL_COLLECT_BUSI");
	if (ptr == NULL)
		_CALP_busi_type = -1;
	else
		_CALP_busi_type = atoi(ptr);
}

calp_ctx::~calp_ctx()
{
}

void calp_ctx::load_config(const string& config)
{
	bpt::iptree pt;
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	istringstream is(config);
	bpt::read_xml(is, pt);

	try {
		const bpt::iptree& v_pt = pt.get_child("cal");

		load_parms(v_pt);
		load_tables(v_pt);
		load_aliases(v_pt);
		load_input(v_pt);
		load_output(v_pt);
	} catch (bpt::ptree_bad_data& ex) {
		throw sg_exception(__FILE__, __LINE__, SGESYSTEM, 0, (_("ERROR: Parse section cal failure, {1} ({2})") % ex.what() % ex.data<string>()).str(APPLOCALE));
	} catch (bpt::ptree_error& ex) {
		throw sg_exception(__FILE__, __LINE__, SGESYSTEM, 0, (_("ERROR: Parse section cal failure, {1}") % ex.what()).str(APPLOCALE));
	}
}

void calp_ctx::load_parms(const bpt::iptree& pt)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(100, __PRETTY_FUNCTION__, "", NULL);
#endif
	gpenv& env_mgr = gpenv::instance();
	sap_ctx *SAP = sap_ctx::instance();

	try {
		const bpt::iptree& v_pt = pt.get_child("resource");

		_CALP_parms.dbname = get_value(v_pt, "dbname", SAP->_SAP_resource.dbname);
		env_mgr.expand_var(_CALP_parms.openinfo, get_value(v_pt, "openinfo", SAP->_SAP_resource.openinfo));

		env_mgr.expand_var(_CALP_parms.province_code, v_pt.get<string>("province_code"));
		_CALP_parms.driver_data = v_pt.get<string>("driver_data");
		_CALP_parms.stage = v_pt.get<int>("stage");

		string collision_level = get_value(v_pt, "collision_level", string("NONE"));
		if (strcasecmp(collision_level.c_str(), "NONE") == 0)
			_CALP_parms.collision_level = COLLISION_LEVEL_NONE;
		else if (strcasecmp(collision_level.c_str(), "SOURCE") == 0)
			_CALP_parms.collision_level = COLLISION_LEVEL_SOURCE;
		else if (strcasecmp(collision_level.c_str(), "ALL") == 0)
			_CALP_parms.collision_level = COLLISION_LEVEL_ALL;
		else
			throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: resource.collision_level invalid")).str(APPLOCALE));

		string channel_level = get_value(v_pt, "channel_level", string("N"));
		if (strcasecmp(channel_level.c_str(), "Y") == 0)
			_CALP_parms.channel_level = true;
		else if (strcasecmp(channel_level.c_str(), "N") == 0)
			_CALP_parms.channel_level = false;
		else
			throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: resource.channel_level invalid")).str(APPLOCALE));

		string cycle_type = get_value(v_pt, "cycle_type", string("MONTH"));
		if (strcasecmp(cycle_type.c_str(), "DAY") == 0)
			_CALP_parms.cycle_type = CYCLE_TYPE_DAY;
		else if (strcasecmp(cycle_type.c_str(), "MONTH") == 0)
			_CALP_parms.cycle_type = CYCLE_TYPE_MONTH;
		else
			throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: resource.cycle_type invalid")).str(APPLOCALE));

		_CALP_parms.busi_limit_month = get_value(v_pt, "busi_limit_month", 1);
		_CALP_parms.rule_dup = get_value(v_pt, "rule_dup", string(""));
		_CALP_parms.rule_dup_crbo = get_value(v_pt, "rule_dup_crbo", string(""));
		_CALP_parms.dup_svc_key = get_value(v_pt, "dup_svc_key", string("ALL"));
		_CALP_parms.dup_version = get_value(v_pt, "dup_version", -1);
	} catch (bpt::ptree_bad_data& ex) {
		throw sg_exception(__FILE__, __LINE__, SGESYSTEM, 0, (_("ERROR: Parse section resource failure, {1} ({2})") % ex.what() % ex.data<string>()).str(APPLOCALE));
	} catch (bpt::ptree_error& ex) {
		throw sg_exception(__FILE__, __LINE__, SGESYSTEM, 0, (_("ERROR: Parse section resource failure, {1}") % ex.what()).str(APPLOCALE));
	}
}

void calp_ctx::load_tables(const bpt::iptree& pt)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif
	_CALP_dbc_tables.clear();

	try {
		BOOST_FOREACH(const bpt::iptree::value_type& v, pt.get_child("tables")) {
			if (v.first != "table")
				continue;

			const bpt::iptree& v_pt = v.second;
			cal_dbc_table_t item;

			item.table_name = boost::to_lower_copy(v_pt.get<string>("table_name"));
			item.index_id = get_value(v_pt, "index_id", 0);

			_CALP_dbc_tables.push_back(item);
		}
	} catch (bpt::ptree_bad_data& ex) {
		throw sg_exception(__FILE__, __LINE__, SGESYSTEM, 0, (_("ERROR: Parse section tables failure, {1} ({2})") % ex.what() % ex.data<string>()).str(APPLOCALE));
	} catch (bpt::ptree_error& ex) {
		throw sg_exception(__FILE__, __LINE__, SGESYSTEM, 0, (_("ERROR: Parse section tables failure, {1}") % ex.what()).str(APPLOCALE));
	}
}

void calp_ctx::load_aliases(const bpt::iptree& pt)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif
	_CALP_dbc_aliases.clear();

	try {
		boost::optional<const bpt::iptree&> node = pt.get_child_optional("aliases");
		if (!node)
			return;

		BOOST_FOREACH(const bpt::iptree::value_type& v, *node) {
			if (v.first != "alias")
				continue;

			const bpt::iptree& v_pt = v.second;
			cal_dbc_alias_t item;

			item.table_name = boost::to_lower_copy(v_pt.get<string>("table_name"));
			item.orig_table_name = boost::to_lower_copy(v_pt.get<string>("orig_table_name"));
			item.orig_index_id = get_value(v_pt, "orig_index_id", 0);

			boost::optional<const bpt::iptree&> fields_node = v_pt.get_child_optional("fields");
			if (fields_node) {
				BOOST_FOREACH(const bpt::iptree::value_type& vf, *fields_node) {
					if (vf.first != "field")
						continue;

					const bpt::iptree& v_pf = vf.second;

					string field_name = boost::to_lower_copy(v_pf.get<string>("field_name"));
					string orig_field_name = boost::to_lower_copy(v_pf.get<string>("orig_field_name"));

					item.field_map[orig_field_name] = field_name;
				}
			}

			_CALP_dbc_aliases.push_back(item);
		}
	} catch (bpt::ptree_bad_data& ex) {
		throw sg_exception(__FILE__, __LINE__, SGESYSTEM, 0, (_("ERROR: Parse section aliases failure, {1} ({2})") % ex.what() % ex.data<string>()).str(APPLOCALE));
	} catch (bpt::ptree_error& ex) {
		throw sg_exception(__FILE__, __LINE__, SGESYSTEM, 0, (_("ERROR: Parse section aliases failure, {1}") % ex.what()).str(APPLOCALE));
	}
}

void calp_ctx::load_input(const bpt::iptree& pt)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif
	_CALP_field_types.clear();

	try {
		BOOST_FOREACH(const bpt::iptree::value_type& v, pt.get_child("input.fields")) {
			if (v.first != "field")
				continue;

			const bpt::iptree& v_pt = v.second;

			string field_name = boost::to_lower_copy(v_pt.get<string>("field_name"));
			string field_type = v_pt.get<string>("field_type");
			if (strcasecmp(field_type.c_str(), "CHAR") == 0)
				_CALP_field_types[field_name] = SQLTYPE_CHAR;
			else if (strcasecmp(field_type.c_str(), "SHORT") == 0)
				_CALP_field_types[field_name] = SQLTYPE_SHORT;
			else if (strcasecmp(field_type.c_str(), "INT") == 0)
				_CALP_field_types[field_name] = SQLTYPE_INT;
			else if (strcasecmp(field_type.c_str(), "LONG") == 0)
				_CALP_field_types[field_name] = SQLTYPE_LONG;
			else if (strcasecmp(field_type.c_str(), "FLOAT") == 0)
				_CALP_field_types[field_name] = SQLTYPE_FLOAT;
			else if (strcasecmp(field_type.c_str(), "DOUBLE") == 0)
				_CALP_field_types[field_name] = SQLTYPE_DOUBLE;
			else if (strcasecmp(field_type.c_str(), "STRING") == 0)
				_CALP_field_types[field_name] = SQLTYPE_STRING;
			else if (strcasecmp(field_type.c_str(), "TIME") == 0)
				_CALP_field_types[field_name] = SQLTYPE_TIME;
			else
				throw sg_exception(__FILE__, __LINE__, SGESYSTEM, 0, (_("ERROR: unsupported field_type {1} given.") % field_type).str(APPLOCALE));
		}
	} catch (bpt::ptree_bad_data& ex) {
		throw sg_exception(__FILE__, __LINE__, SGESYSTEM, 0, (_("ERROR: Parse section input failure, {1} ({2})") % ex.what() % ex.data<string>()).str(APPLOCALE));
	} catch (bpt::ptree_error& ex) {
		throw sg_exception(__FILE__, __LINE__, SGESYSTEM, 0, (_("ERROR: Parse section input failure, {1}") % ex.what()).str(APPLOCALE));
	}
}

void calp_ctx::load_output(const bpt::iptree& pt)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif
	_CALP_default_values.clear();

	try {
		BOOST_FOREACH(const bpt::iptree::value_type& v, pt.get_child("output.fields")) {
			if (v.first != "field")
				continue;

			const bpt::iptree& v_pt = v.second;

			string field_name = boost::to_lower_copy(v_pt.get<string>("field_name"));
			string default_value = v_pt.get<string>("default_value");

			_CALP_default_values[field_name] = default_value;
		}
	} catch (bpt::ptree_bad_data& ex) {
		throw sg_exception(__FILE__, __LINE__, SGESYSTEM, 0, (_("ERROR: Parse section output failure, {1} ({2})") % ex.what() % ex.data<string>()).str(APPLOCALE));
	} catch (bpt::ptree_error& ex) {
		throw sg_exception(__FILE__, __LINE__, SGESYSTEM, 0, (_("ERROR: Parse section output failure, {1}") % ex.what()).str(APPLOCALE));
	}
}

bool calp_ctx::get_partitions()
{
	string config;
	sg_api& api_mgr = sg_api::instance();
	gpp_ctx *GPP = gpp_ctx::instance();
	sgt_ctx *SGT = sgt_ctx::instance();
	bool retval = false;
#if defined(DEBUG)
	scoped_debug<bool> debug(1000, __PRETTY_FUNCTION__, "", &retval);
#endif

	string key = string("DUP.") + _CALP_parms.dup_svc_key;
	// 获取DUPCHK配置
	if (!api_mgr.get_config(config, key, _CALP_parms.dup_version)) {
		SGT->_SGT_error_code = SGESYSTEM;
		GPP->write_log(__FILE__, __LINE__, (_("ERROR: Failure to load DUP configuration")).str(APPLOCALE));
		return retval;
	}

	bpt::iptree pt;
	istringstream is(config);
	bpt::read_xml(is, pt);

	try {
		_CALP_parms.dup_partitions = pt.get<int>("resource.partitions");

		int hosts = 0;
		string lmids;
		gpenv& env_mgr = gpenv::instance();
		env_mgr.expand_var(lmids, pt.get<string>("resource.hostid"));
		boost::char_separator<char> sep(" \t\b");
		tokenizer lmid_tokens(lmids, sep);
		for (tokenizer::iterator iter = lmid_tokens.begin(); iter != lmid_tokens.end(); ++iter)
			hosts++;

		_CALP_parms.dup_all_partitions = _CALP_parms.dup_partitions * hosts;
	} catch (bpt::ptree_bad_data& ex) {
		throw sg_exception(__FILE__, __LINE__, SGESYSTEM, 0, (_("ERROR: Parse section resource failure, {1} ({2})") % ex.what() % ex.data<string>()).str(APPLOCALE));
	} catch (bpt::ptree_error& ex) {
		throw sg_exception(__FILE__, __LINE__, SGESYSTEM, 0, (_("ERROR: Parse section resource failure, {1}") % ex.what()).str(APPLOCALE));
	}

	retval = true;
	return retval;
}

void calp_ctx::init_once()
{
	_instance = new calp_ctx();
}

}
}

