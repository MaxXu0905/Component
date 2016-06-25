#include "parser_config.h"
#include "sa_config.h"
#include "sg_internal.h"
#include <boost/scope_exit.hpp>
#include <boost/foreach.hpp>

extern FILE *yyin;
extern FILE *yyout;
extern void yyrestart(FILE *input_file);
extern int yyparse(void);

using namespace ai::sg;
using namespace ai::app;
#include "parser.cpp.h"

namespace ai
{
namespace app
{

int errcnt;

extern int inquote;
extern int incomments;
extern string tminline;
extern int line_no;
extern int column;

parser_config::parser_config()
{
	PRSP = prsp_ctx::instance();
}

parser_config::~parser_config()
{
}

void parser_config::run(const string& input_file, const string& output_file)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, (_("input_file={1}, output_file={2}") % input_file % output_file).str(APPLOCALE), NULL);
#endif
	parser_raw_type raw_type;

	// º”‘ÿ≈‰÷√
	load(input_file);

	BOOST_FOREACH(bps_config_t& config, configs) {
		vector<bps_input_table_t>& input = config.input;
		vector<string>& processes = config.processes;

		raw_type.set_is_text(false);
		raw_type.set_is_unsigned(false);
		raw_type.set_atomic_type(PARSER_TYPE_CHAR);
		raw_type.set_pointers(1);

		PRSP->_PRSP_output_types.clear();
		BOOST_FOREACH(const bps_output_table_t& output_table, config.output) {
			PRSP->_PRSP_output_names.push_back(output_table.name);
			PRSP->_PRSP_output_types.push_back(map<string, parser_raw_type>());
			map<string, parser_raw_type>& output_types = *PRSP->_PRSP_output_types.rbegin();

			BOOST_FOREACH(const bps_output_field_t& field, output_table.fields) {
				output_types[field.field_name] = raw_type;
			}
		}

		PRSP->_PRSP_temp_types.clear();

		for (int i = 0; i < processes.size(); i++) {
			bps_input_table_t& input_table = input[i];

			PRSP->_PRSP_input_name = input_table.name;

			PRSP->_PRSP_input_types.clear();
			BOOST_FOREACH(const bps_input_field_t& field, input_table.fields) {
				PRSP->_PRSP_input_types[field.field_name] = raw_type;
			}

			build(processes[i]);
		}
	}

	save(output_file);
}

void parser_config::build(string& process)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	int ret;
	char temp_name[4096];
	const char *tmpdir = ::getenv("TMPDIR");
	if (tmpdir == NULL)
		tmpdir = P_tmpdir;

	sprintf(temp_name, "%s/prsrXXXXXX", tmpdir);

	int fd = ::mkstemp(temp_name);
	if (fd == -1)
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: mkstemp() failure, {1}") % ::strerror(errno)).str(APPLOCALE));

	yyin = ::fdopen(fd, "w+");
	if (yyin == NULL) {
		::close(fd);
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: open temp file [{1}] failure, {2}") % temp_name % ::strerror(errno)).str(APPLOCALE));
	}

	BOOST_SCOPE_EXIT((&yyin)(&temp_name)) {
		::fclose(yyin);
		::unlink(temp_name);
	} BOOST_SCOPE_EXIT_END

	if (::fwrite(process.c_str(), process.length(), 1, yyin) != 1)
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: write code to file [{1}] failure, {2}") % temp_name % ::strerror(errno)).str(APPLOCALE));

	::fflush(yyin);
	::fseek(yyin, 0, SEEK_SET);

	yyout = stdout;		// default
	errcnt = 0;

	inquote = 0;
	incomments = 0;
	tminline = "";
	line_no = 0;
	column = 0;

	// call yyparse() to parse temp file
	yyrestart(yyin);
	if ((ret = yyparse()) < 0)
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Parse failed.")).str(APPLOCALE));

	if (ret == 1)
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Severe error found. Stop syntax checking.")).str(APPLOCALE));

	if (errcnt > 0)
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Above errors found during syntax checking.")).str(APPLOCALE));

	PRSP->_PRSP_stmts->preparse();
	process = PRSP->_PRSP_stmts->gen_code();
	delete PRSP->_PRSP_stmts;
	PRSP->_PRSP_stmts = NULL;
}

void parser_config::load(const string& config_file)
{
	bpt::iptree pt;
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, (_("config_file={1}") % config_file).str(APPLOCALE), NULL);
#endif

	ifstream ifs(config_file.c_str());
	bpt::read_xml(ifs, pt);

	load_services(pt);
}

void parser_config::save(const string& config_file)
{
	bpt::iptree pt;
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, (_("config_file={1}") % config_file).str(APPLOCALE), NULL);
#endif

	save_services(pt);

	bpt::write_xml(config_file, pt, std::locale(), bpt::xml_writer_settings<char>('\t', 1));
}

void parser_config::load_services(const bpt::iptree& pt)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	try {
		BOOST_FOREACH(const bpt::iptree::value_type& v, pt.get_child("services")) {
			if (v.first != "service")
				continue;

			const bpt::iptree& v_pt = v.second;
			bps_config_t config;

			load_parms(v_pt, config);

			if (!config.parms.disable_global)
				load_global(v_pt, config);

			load_input(v_pt, config);
			load_output(v_pt, config);

			load_process(v_pt, config);

			configs.push_back(config);
		}
	} catch (bpt::ptree_bad_data& ex) {
		throw sg_exception(__FILE__, __LINE__, SGESYSTEM, 0, (_("ERROR: Parse section services failure, {1} ({2})") % ex.what() % ex.data<string>()).str(APPLOCALE));
	} catch (bpt::ptree_error& ex) {
		throw sg_exception(__FILE__, __LINE__, SGESYSTEM, 0, (_("ERROR: Parse section services failure, {1}") % ex.what()).str(APPLOCALE));
	}
}

void parser_config::load_parms(const bpt::iptree& pt, bps_config_t& config)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(100, __PRETTY_FUNCTION__, "", NULL);
#endif
	svc_parms_t& parms = config.parms;

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

void parser_config::load_global(const bpt::iptree& pt, bps_config_t& config)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(100, __PRETTY_FUNCTION__, "", NULL);
#endif
	vector<bps_global_field_t>& global = config.global;

	try {
		boost::optional<const bpt::iptree&> node = pt.get_child_optional("global.fields");
		if (!node)
			return;

		BOOST_FOREACH(const bpt::iptree::value_type& v, *node) {
			if (v.first != "field")
				continue;

			const bpt::iptree& v_pt = v.second;
			bps_global_field_t field;

			field.field_name = boost::to_lower_copy(v_pt.get<string>("field_name"));
			if (field.field_name.empty())
				throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: global.field_name is empty")).str(APPLOCALE));

			field.field_size = v_pt.get<int>("field_size");
			if (field.field_size <= 0)
				throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: global.field_size must be positive")).str(APPLOCALE));

			string readonly_str = get_value(v_pt, "readonly", string("N"));
			if (strcasecmp(readonly_str.c_str(), "Y") == 0)
				field.readonly = true;
			else if (strcasecmp(readonly_str.c_str(), "N") == 0)
				field.readonly = false;
			else
				throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: global.readonly invalid")).str(APPLOCALE));

			global.push_back(field);
		}
	} catch (bpt::ptree_bad_data& ex) {
		throw sg_exception(__FILE__, __LINE__, SGESYSTEM, 0, (_("ERROR: Parse section global failure, {1} ({2})") % ex.what() % ex.data<string>()).str(APPLOCALE));
	} catch (bpt::ptree_error& ex) {
		throw sg_exception(__FILE__, __LINE__, SGESYSTEM, 0, (_("ERROR: Parse section global failure, {1}") % ex.what()).str(APPLOCALE));
	}
}

void parser_config::load_input(const bpt::iptree& pt, bps_config_t& config)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif
	vector<bps_input_table_t>& input = config.input;

	try {
		BOOST_FOREACH(const bpt::iptree::value_type& v, pt.get_child("input")) {
			if (v.first != "table")
				continue;

			const bpt::iptree& v_pt = v.second;
			input.push_back(bps_input_table_t());
			bps_input_table_t& table = *input.rbegin();

			table.name = boost::to_lower_copy(v_pt.get<string>("name"));

			BOOST_FOREACH(const bpt::iptree::value_type& vf, v_pt.get_child("fields")) {
				if (vf.first != "field")
					continue;

				const bpt::iptree& v_pf = vf.second;
				bps_input_field_t field;

				field.field_name += boost::to_lower_copy(v_pf.get<string>("field_name"));
				if (field.field_name.empty())
					throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: input.table.field_name is empty")).str(APPLOCALE));

				field.field_size = v_pf.get<int>("field_size");
				if (field.field_size <= 0)
					throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: input.table.fields.field.field_size must be positive.")).str(APPLOCALE));

				table.fields.push_back(field);
			}
		}
	} catch (bpt::ptree_bad_data& ex) {
		throw sg_exception(__FILE__, __LINE__, SGESYSTEM, 0, (_("ERROR: Parse section input failure, {1} ({2})") % ex.what() % ex.data<string>()).str(APPLOCALE));
	} catch (bpt::ptree_error& ex) {
		throw sg_exception(__FILE__, __LINE__, SGESYSTEM, 0, (_("ERROR: Parse section input failure, {1}") % ex.what()).str(APPLOCALE));
	}
}

void parser_config::load_output(const bpt::iptree& pt, bps_config_t& config)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif
	vector<bps_output_table_t>& output = config.output;

	try {
		BOOST_FOREACH(const bpt::iptree::value_type& v, pt.get_child("output")) {
			if (v.first != "table")
				continue;

			const bpt::iptree& v_pt = v.second;
			output.push_back(bps_output_table_t());
			bps_output_table_t& table = *output.rbegin();

			table.name = boost::to_lower_copy(v_pt.get<string>("name"));

			BOOST_FOREACH(const bpt::iptree::value_type& vf, v_pt.get_child("fields")) {
				if (vf.first != "field")
					continue;

				const bpt::iptree& v_pf = vf.second;
				bps_output_field_t field;

				field.field_name += boost::to_lower_copy(v_pf.get<string>("field_name"));
				if (field.field_name.empty())
					throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: output.table.field_name is empty")).str(APPLOCALE));

				field.field_size = v_pf.get<int>("field_size");
				if (field.field_size <= 0)
					throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: output.table.fields.field.field_size must be positive.")).str(APPLOCALE));

				table.fields.push_back(field);
			}
		}
	} catch (bpt::ptree_bad_data& ex) {
		throw sg_exception(__FILE__, __LINE__, SGESYSTEM, 0, (_("ERROR: Parse section output failure, {1} ({2})") % ex.what() % ex.data<string>()).str(APPLOCALE));
	} catch (bpt::ptree_error& ex) {
		throw sg_exception(__FILE__, __LINE__, SGESYSTEM, 0, (_("ERROR: Parse section output failure, {1}") % ex.what()).str(APPLOCALE));
	}
}

void parser_config::load_process(const bpt::iptree& pt, bps_config_t& config)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif
	vector<string>& processes = config.processes;

	try {
		BOOST_FOREACH(const bpt::iptree::value_type& v, pt.get_child("processes")) {
			if (v.first != "process")
				continue;

			const bpt::iptree& v_pt = v.second;
			string code;

			BOOST_FOREACH(const bpt::iptree::value_type& vf, v_pt) {
				if (vf.first != "rule")
					continue;

				const bpt::iptree& v_pf = vf.second;

				code += get_value(v_pf, "mapping", string(""));
			}

			processes.push_back(code);
		}
	} catch (bpt::ptree_bad_data& ex) {
		throw sg_exception(__FILE__, __LINE__, SGESYSTEM, 0, (_("ERROR: Parse section processes failure, {1} ({2})") % ex.what() % ex.data<string>()).str(APPLOCALE));
	} catch (bpt::ptree_error& ex) {
		throw sg_exception(__FILE__, __LINE__, SGESYSTEM, 0, (_("ERROR: Parse section processes failure, {1}") % ex.what()).str(APPLOCALE));
	}
}

void parser_config::save_services(bpt::iptree& pt)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	BOOST_FOREACH(const bps_config_t& config, configs) {
		bpt::iptree v_pt;

		save_parms(v_pt, config);
		save_global(v_pt, config);
		save_input(v_pt, config);
		save_output(v_pt, config);
		save_process(v_pt, config);

		pt.add_child("services.service", v_pt);
	}
}

void parser_config::save_parms(bpt::iptree& pt, const bps_config_t& config)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(100, __PRETTY_FUNCTION__, "", NULL);
#endif
	const svc_parms_t& parms = config.parms;
	bpt::iptree v_pt;

	v_pt.put("svc_key", parms.svc_key);

	if (parms.version != DFLT_VERSION)
		v_pt.put("version", parms.version);

	if (parms.disable_global)
		v_pt.put("disable_global", "Y");

	pt.add_child("resource", v_pt);
}

void parser_config::save_global(bpt::iptree& pt, const bps_config_t& config)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(100, __PRETTY_FUNCTION__, "", NULL);
#endif
	const vector<bps_global_field_t>& global = config.global;

	BOOST_FOREACH(const bps_global_field_t& field, global) {
		bpt::iptree v_pt;

		v_pt.put("field_name", field.field_name);
		v_pt.put("field_size", field.field_size);

		if (field.readonly)
			v_pt.put("readonly", "Y");

		pt.add_child("global.fields.field", v_pt);
	}
}

void parser_config::save_input(bpt::iptree& pt, const bps_config_t& config)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif
	const vector<bps_input_table_t>& input = config.input;

	BOOST_FOREACH(const bps_input_table_t& table, input) {
		bpt::iptree v_pt;

		v_pt.put("name", table.name);

		BOOST_FOREACH(const bps_input_field_t& field, table.fields) {
			bpt::iptree v_pf;

			v_pf.put("field_name", field.field_name);
			v_pf.put("field_size", field.field_size);

			v_pt.add_child("fields.field", v_pf);
		}

		pt.add_child("input.table", v_pt);
	}
}

void parser_config::save_output(bpt::iptree& pt, const bps_config_t& config)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif
	const vector<bps_output_table_t>& output = config.output;

	BOOST_FOREACH(const bps_output_table_t& table, output) {
		bpt::iptree v_pt;

		v_pt.put("name", table.name);

		BOOST_FOREACH(const bps_output_field_t& field, table.fields) {
			bpt::iptree v_pf;

			v_pf.put("field_name", field.field_name);
			v_pf.put("field_size", field.field_size);

			v_pt.add_child("fields.field", v_pf);
		}

		pt.add_child("output.table", v_pt);
	}
}

void parser_config::save_process(bpt::iptree& pt, const bps_config_t& config)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif
	const vector<string>& processes = config.processes;

	BOOST_FOREACH(const string& process, processes) {
		bpt::iptree v_pt;

		v_pt.put("rule.mapping", process);

		pt.add_child("processes.process", v_pt);
	}
}

}
}

