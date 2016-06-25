#include "schd_config.h"

namespace ai
{
namespace app
{

using namespace std;
using namespace ai::sg;
typedef boost::tokenizer<boost::char_separator<char> > tokenizer;

schd_config::schd_config()
{
	SCHD = schd_ctx::instance();
}

schd_config::~schd_config()
{
}

void schd_config::load(const string& config_file)
{
	bpt::iptree pt;
#if defined(DEBUG)
	scoped_debug<bool> debug(100, __PRETTY_FUNCTION__, "", NULL);
#endif

	bpt::read_xml(config_file, pt);

	load_entries(pt);
	load_dependencies(pt);
	load_conditions(pt);
	load_actions(pt);
}

void schd_config::load_xml(const string& config)
{
	bpt::iptree pt;
#if defined(DEBUG)
	scoped_debug<bool> debug(100, __PRETTY_FUNCTION__, "", NULL);
#endif

	istringstream is(config);

	bpt::read_xml(is, pt);

	load_entries(pt);
	load_dependencies(pt);
}

void schd_config::load_entries(const bpt::iptree& pt)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(100, __PRETTY_FUNCTION__, "", NULL);
#endif
	gpenv& env_mgr = gpenv::instance();
	boost::char_separator<char> sep(" \t\b");

	try {
		BOOST_FOREACH(const bpt::iptree::value_type& v, pt.get_child("workflow.entries")) {
			if (v.first != "entry")
				continue;

			const bpt::iptree& v_pt = v.second;
			schd_cfg_entry_t entry;

			string entry_name = v_pt.get<string>("entry_name");
			if (entry_name.empty())
				throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: workflow.entries.entry.entry_name is empty")).str(APPLOCALE));

			entry.command = v_pt.get<string>("command");
			if (entry.command.empty())
				throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: workflow.entries.entry.command is empty")).str(APPLOCALE));

			entry.iclean = get_value(v_pt, "iclean", string(""));
			entry.undo = get_value(v_pt, "undo", string(""));
			entry.autoclean = get_value(v_pt, "autoclean", string(""));
			env_mgr.expand_var(entry.usrname, get_value(v_pt, "usrname", string("")));

			string hostname;
			env_mgr.expand_var(hostname, v_pt.get<string>("hostname"));
			tokenizer tokens(hostname, sep);
			for (tokenizer::iterator iter = tokens.begin(); iter != tokens.end(); ++iter) {
				string str = *iter;

				entry.hostnames.push_back(str);
			}

			string restart = get_value(v_pt, "restart", string("N"));
			if (strcasecmp(restart.c_str(), "Y") == 0)
				entry.flags |= SCHD_FLAG_RESTARTABLE;
			else if (strcasecmp(restart.c_str(), "N") == 0)
				entry.flags &= ~SCHD_FLAG_RESTARTABLE;
			else
				throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: workflow.entries.entry.restart invalid")).str(APPLOCALE));

			if (entry.flags & SCHD_FLAG_RESTARTABLE)
				entry.maxgen = get_value(v_pt, "maxrst", std::numeric_limits<int>::max());
			else
				entry.maxgen = 0;
			entry.min = get_value(v_pt, "min", DFLT_SCHD_MIN);
			entry.max = get_value(v_pt, "max", DFLT_SCHD_MAX);
			if (entry.max < entry.min)
				entry.max = entry.min;

			const char *ptr = env_mgr.getenv("LOGNAME");
			if (ptr == NULL)
				throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: environment variable LOGNAME not set")).str(APPLOCALE));

			string prefix = ptr;
			prefix += "@";

			string directories;
			env_mgr.expand_var(directories, get_value(v_pt, "directories", string("")));
			tokenizer dir_tokens(directories, sep);
			for (tokenizer::iterator iter = dir_tokens.begin(); iter != dir_tokens.end(); ++iter) {
				schd_directory_t dir;

				dir.directory = *iter;
				(void)common::parse(dir.directory, dir.sftp_prefix);
				if (dir.sftp_prefix.empty()) {
					BOOST_FOREACH(const string& hostname, entry.hostnames) {
						dir.sftp_prefix = prefix + hostname;
						entry.directories.push_back(dir);
					}
				} else {
					entry.directories.push_back(dir);
				}
			}

			entry.pattern = get_value(v_pt, "pattern", string("^.*$"));

			string policy = get_value(v_pt, "policy", string(""));
			if (strcasecmp(policy.c_str(), "BYHAND") == 0)
				entry.policy = SCHD_POLICY_BYHAND;
			else if (strcasecmp(policy.c_str(), "AUTO") == 0)
				entry.policy = SCHD_POLICY_AUTO;
			else
				throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: workflow.entries.entry.policy invalid")).str(APPLOCALE));

			string enable_proxy = get_value(v_pt, "enable_proxy", string("N"));
			if (strcasecmp(enable_proxy.c_str(), "Y") == 0)
				entry.enable_proxy = true;
			else if (strcasecmp(enable_proxy.c_str(), "N") == 0)
				entry.enable_proxy = false;
			else
				throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: workflow.entries.entry.enable_proxy invalid")).str(APPLOCALE));

			string enable_cproxy = get_value(v_pt, "enable_cproxy", enable_proxy);
			if (strcasecmp(enable_cproxy.c_str(), "Y") == 0)
				entry.enable_cproxy = true;
			else if (strcasecmp(enable_cproxy.c_str(), "N") == 0)
				entry.enable_cproxy = false;
			else
				throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: workflow.entries.entry.enable_cproxy invalid")).str(APPLOCALE));

			string enable_iproxy = get_value(v_pt, "enable_iproxy", enable_proxy);
			if (strcasecmp(enable_iproxy.c_str(), "Y") == 0)
				entry.enable_iproxy = true;
			else if (strcasecmp(enable_iproxy.c_str(), "N") == 0)
				entry.enable_iproxy = false;
			else
				throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: workflow.entries.entry.enable_iproxy invalid")).str(APPLOCALE));

			string enable_uproxy = get_value(v_pt, "enable_uproxy", enable_proxy);
			if (strcasecmp(enable_uproxy.c_str(), "Y") == 0)
				entry.enable_uproxy = true;
			else if (strcasecmp(enable_uproxy.c_str(), "N") == 0)
				entry.enable_uproxy = false;
			else
				throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: workflow.entries.entry.enable_uproxy invalid")).str(APPLOCALE));

			SCHD->_SCHD_cfg_entries[entry_name] = entry;
		}
	} catch (bpt::ptree_bad_data& ex) {
		throw sg_exception(__FILE__, __LINE__, SGESYSTEM, 0, (_("ERROR: Parse section entries failure, {1} ({2})") % ex.what() % ex.data<string>()).str(APPLOCALE));
	} catch (bpt::ptree_error& ex) {
		throw sg_exception(__FILE__, __LINE__, SGESYSTEM, 0, (_("ERROR: Parse section entries failure, {1}") % ex.what()).str(APPLOCALE));
	}
}

void schd_config::load_dependencies(const bpt::iptree& pt)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(100, __PRETTY_FUNCTION__, "", NULL);
#endif

	try {
		boost::optional<const bpt::iptree&> node = pt.get_child_optional("workflow.dependencies");
		if (!node)
			return;

		BOOST_FOREACH(const bpt::iptree::value_type& v, *node) {
			if (v.first != "dependency")
				continue;

			const bpt::iptree& v_pt = v.second;

			string entry_name = v_pt.get<string>("entry_name");
			if (entry_name.empty())
				throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: workflow.dependencies.dependency.entry_name is empty")).str(APPLOCALE));

			string dependency_name = v_pt.get<string>("dependency_name");
			if (dependency_name.empty())
				throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: workflow.dependencies.dependency.dependency_name is empty")).str(APPLOCALE));

			SCHD->_SCHD_deps[entry_name].push_back(dependency_name);
			SCHD->_SCHD_rdeps[dependency_name].push_back(entry_name);
		}
	} catch (bpt::ptree_bad_data& ex) {
		throw sg_exception(__FILE__, __LINE__, SGESYSTEM, 0, (_("ERROR: Parse section dependencies failure, {1} ({2})") % ex.what() % ex.data<string>()).str(APPLOCALE));
	} catch (bpt::ptree_error& ex) {
		throw sg_exception(__FILE__, __LINE__, SGESYSTEM, 0, (_("ERROR: Parse section dependencies failure, {1}") % ex.what()).str(APPLOCALE));
	}
}

void schd_config::load_conditions(const bpt::iptree& pt)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(100, __PRETTY_FUNCTION__, "", NULL);
#endif

	try {
		boost::optional<const bpt::iptree&> node = pt.get_child_optional("workflow.conditions");
		if (!node)
			return;

		BOOST_FOREACH(const bpt::iptree::value_type& v, *node) {
			if (v.first != "condition")
				continue;

			const bpt::iptree& v_pt = v.second;
			schd_condition_t item;

			string entry_name = v_pt.get<string>("entry_name");
			if (entry_name.empty())
				throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: workflow.conditions.condition.entry_name is empty")).str(APPLOCALE));

			item.entry_name = v_pt.get<string>("related_name");
			if (item.entry_name.empty())
				throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: workflow.conditions.condition.related_name is empty")).str(APPLOCALE));

			item.finish = get_value(v_pt, "finish", false);

			SCHD->_SCHD_conditions[entry_name].push_back(item);
		}
	} catch (bpt::ptree_bad_data& ex) {
		throw sg_exception(__FILE__, __LINE__, SGESYSTEM, 0, (_("ERROR: Parse section conditions failure, {1} ({2})") % ex.what() % ex.data<string>()).str(APPLOCALE));
	} catch (bpt::ptree_error& ex) {
		throw sg_exception(__FILE__, __LINE__, SGESYSTEM, 0, (_("ERROR: Parse section conditions failure, {1}") % ex.what()).str(APPLOCALE));
	}
}

void schd_config::load_actions(const bpt::iptree& pt)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(100, __PRETTY_FUNCTION__, "", NULL);
#endif

	try {
		boost::optional<const bpt::iptree&> node = pt.get_child_optional("workflow.actions");
		if (!node)
			return;

		BOOST_FOREACH(const bpt::iptree::value_type& v, *node) {
			if (v.first != "action")
				continue;

			const bpt::iptree& v_pt = v.second;

			string entry_name = v_pt.get<string>("entry_name");
			if (entry_name.empty())
				throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: workflow.actions.action.entry_name is empty")).str(APPLOCALE));

			string affected_name = v_pt.get<string>("affected_name");
			if (affected_name.empty())
				throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: workflow.actions.action.affected_name is empty")).str(APPLOCALE));

			SCHD->_SCHD_actions[entry_name].push_back(affected_name);
		}
	} catch (bpt::ptree_bad_data& ex) {
		throw sg_exception(__FILE__, __LINE__, SGESYSTEM, 0, (_("ERROR: Parse section actions failure, {1} ({2})") % ex.what() % ex.data<string>()).str(APPLOCALE));
	} catch (bpt::ptree_error& ex) {
		throw sg_exception(__FILE__, __LINE__, SGESYSTEM, 0, (_("ERROR: Parse section actions failure, {1}") % ex.what()).str(APPLOCALE));
	}
}

}
}

