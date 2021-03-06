#include "sa_internal.h"

namespace ai
{
namespace app
{

using namespace std;
using namespace ai::sg;
typedef boost::tokenizer<boost::char_separator<char> > tokenizer;

sa_otarget::sa_otarget(sa_base& _sa, int _flags)
	: sa_output(_sa, _flags)
{
	map<string, string>::const_iterator iter;
	gpenv& env_mgr = gpenv::instance();
	const map<string, string> *args;

	switch (flags) {
	case OTYPE_TARGET:
		args = &adaptor.target.args;
		break_point = &SAT->_SAT_dblog[sa.get_id()].master.break_normal;
		break;
	default:
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: unsupported flags {1}") % flags).str(APPLOCALE));
	}

	begin_column = 0;
	varied = false;

	iter = args->find("options");
	if (iter != args->end()) {
		boost::char_separator<char> sep(" \t\b");
		tokenizer tokens(iter->second, sep);

		for (tokenizer::iterator iter = tokens.begin(); iter != tokens.end(); ++iter) {
			string str = *iter;

			if (strcasecmp(str.c_str(), "SKIP_FIXED") == 0)
				begin_column = FIXED_OUTPUT_SIZE;
			else if (strcasecmp(str.c_str(), "VARIED") == 0)
				varied = true;
		}
	}

	iter = args->find("delimiter");
	if (iter == args->end())
		delimiter = ',';
	else
		delimiter = static_cast<char>(iter->second[0]);

	iter = args->find("directory");
	if (iter == args->end())
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: directory missing in args.arg")).str(APPLOCALE));

	env_mgr.expand_var(dir_name, iter->second);
	if (*dir_name.rbegin() != '/')
		dir_name += '/';

	iter = args->find("rdirectory");
	if (iter != args->end()) {
		env_mgr.expand_var(rdir_name, iter->second);
		if (*rdir_name.rbegin() != '/')
			rdir_name += '/';

		if (!common::parse(rdir_name, sftp_prefix))
			throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: rdirectory invalid in args.arg")).str(APPLOCALE));

		// 对于本地协议，直接忽略
		if (common::is_local(sftp_prefix)) {
			sftp_prefix.clear();
			dir_name = rdir_name;
		}
	}

	iter = args->find("pattern");
	if (iter == args->end())
		pattern = (boost::format("^.*\\.%1%$") % sa.get_id()).str();
	else
		env_mgr.expand_var(pattern, iter->second);

	if (SAP->_SAP_auto_mkdir) {
		if (!common::mkdir(NULL, dir_name))
			GPP->write_log(__FILE__, __LINE__, (_("ERROR: Failed to create directory {1}") % dir_name).str(APPLOCALE));

		if (!sftp_prefix.empty()) {
			file_rpc& rpc_mgr = file_rpc::instance(SGT);

			if (!rpc_mgr.mkdir(sftp_prefix, rdir_name))
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: Failed to create directory {1} on {2} - {3}") % rdir_name % sftp_prefix % SGT->strerror()).str(APPLOCALE));
		}
	}
}

sa_otarget::~sa_otarget()
{
}

void sa_otarget::open()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	full_name = dir_name + SAT->_SAT_raw_file_name + "." + boost::lexical_cast<string>(sa.get_id());
	tmp_name = full_name + ".tmp";
	ofs.open(tmp_name.c_str(), ios_base::out);
	if (!ofs) {
		ofs.open(tmp_name.c_str(), ios_base::in | ios_base::out);
		if (!ofs)
			throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Can't open file {1} - {2}") % tmp_name % strerror(errno)).str(APPLOCALE));

		ofs.seekp(*break_point);
	}

	is_open = true;
}

int sa_otarget::write(int input_idx)
{
	int retval = 0;
#if defined(DEBUG)
	scoped_debug<int> debug(500, __PRETTY_FUNCTION__, (_("input_idx={1}") % input_idx).str(APPLOCALE), &retval);
#endif

	if (!is_open)
		open();

	const vector<output_field_t>& output_fields = adaptor.output_fields;
	const char **output = const_cast<const char **>(sa.get_output());

	for (int i = begin_column; i < output_fields.size(); i++) {
		if (i > begin_column)
			ofs << delimiter;
		if (!varied)
			ofs << std::setw(output_fields[i].field_size);
		ofs << output[i];
	}

	ofs << '\n';
	retval++;
	return retval;
}

void sa_otarget::flush(bool completed)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, (_("completed={1}") % completed).str(APPLOCALE), NULL);
#endif

	if (!is_open)
		return;

	ofs << std::flush;
	*break_point = ofs.tellp();
}

void sa_otarget::close()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	if (!is_open)
		return;

	*break_point = ofs.tellp();
	ofs.close();
	is_open = false;

	if (sftp_prefix.empty()) {
		if (!common::rename(tmp_name.c_str(), full_name.c_str()))
			throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Can't rename file {1} to {2} - {3}") % tmp_name % full_name % strerror(errno)).str(APPLOCALE));
	} else {
		string rfull_name = rdir_name + SAT->_SAT_raw_file_name + "." + boost::lexical_cast<string>(sa.get_id());
		string rtmp_name = rfull_name + ".tmp";
		file_rpc& rpc_mgr = file_rpc::instance(SGT);

		if (!rpc_mgr.put(sftp_prefix, tmp_name, rtmp_name))
			throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Can't put {1} to {2} on {3} - {4}") % tmp_name % rtmp_name % sftp_prefix % SGT->strerror()).str(APPLOCALE));

		if (!rpc_mgr.rename(sftp_prefix, rtmp_name, rfull_name))
			throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Can't rename {1} to {2} on {3} - {4}") % rtmp_name % rfull_name % sftp_prefix % SGT->strerror()).str(APPLOCALE));

		if (::remove(tmp_name.c_str()) == -1)
			throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Can't remove file {1} - {2}") % tmp_name % strerror(errno)).str(APPLOCALE));
	}
}

void sa_otarget::complete()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	if (*break_point <= 0)
		return;

	full_name = dir_name + SAT->_SAT_raw_file_name + "." + boost::lexical_cast<string>(sa.get_id());
	tmp_name = full_name + ".tmp";

	if (sftp_prefix.empty()) {
		if (!common::rename(tmp_name.c_str(), full_name.c_str()))
			GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't rename file {1} to {2} - {3}") % tmp_name % full_name % strerror(errno)).str(APPLOCALE));
	} else {
		string rfull_name = rdir_name + SAT->_SAT_raw_file_name + "." + boost::lexical_cast<string>(sa.get_id());
		string rtmp_name = rfull_name + ".tmp";
		file_rpc& rpc_mgr = file_rpc::instance(SGT);

		if (!rpc_mgr.put(sftp_prefix, tmp_name, rtmp_name)) {
			GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't put {1} to {2} on {3} - {4}") % tmp_name % rtmp_name % sftp_prefix % SGT->strerror()).str(APPLOCALE));
			return;
		}

		if (!rpc_mgr.rename(sftp_prefix, rtmp_name, rfull_name)) {
			GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't rename {1} to {2} on {3} - {4}") % rtmp_name % rfull_name % sftp_prefix % SGT->strerror()).str(APPLOCALE));
			return;
		}

		if (::remove(tmp_name.c_str()) == -1) {
			GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't remove file {1} - {2}") % tmp_name % strerror(errno)).str(APPLOCALE));
			return;
		}
	}
}

void sa_otarget::clean()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	if (!is_open)
		return;

	ofs.close();
	is_open = false;

	if (::remove(tmp_name.c_str()) == -1)
		GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't remove file {1} - {2}") % tmp_name % strerror(errno)).str(APPLOCALE));
}

void sa_otarget::recover()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	if (!is_open)
		return;

	ofs.close();
	is_open = false;
}

void sa_otarget::dump(ostream& os)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(100, __PRETTY_FUNCTION__, "", NULL);
#endif

	os << "mkdir -p " << dir_name << std::endl;
	os << std::endl;
}

void sa_otarget::rollback(const string& raw_file_name, int file_sn)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(100, __PRETTY_FUNCTION__, (_("raw_file_name={1}, file_sn={2}") % raw_file_name % file_sn).str(APPLOCALE), NULL);
#endif

	full_name = dir_name + raw_file_name + "." + boost::lexical_cast<string>(sa.get_id());
	(void)::unlink(full_name.c_str());

	full_name += ".tmp";
	(void)::unlink(full_name.c_str());

	if (!sftp_prefix.empty()) {
		file_rpc& rpc_mgr = file_rpc::instance(SGT);

		full_name = rdir_name + raw_file_name + "." + boost::lexical_cast<string>(sa.get_id());
		(void)rpc_mgr.unlink(sftp_prefix, full_name);

		full_name += ".tmp";
		(void)rpc_mgr.unlink(sftp_prefix, full_name);
	}
}

void sa_otarget::global_rollback()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(100, __PRETTY_FUNCTION__, "", NULL);
#endif

	vector<string> files;
	scan_file<> scan(dir_name, pattern);
	scan.get_files(files);
	BOOST_FOREACH(const string& filename, files) {
		string full_name = dir_name + filename;
		(void)::unlink(full_name.c_str());
	}

	if (!sftp_prefix.empty()) {
		file_rpc& rpc_mgr = file_rpc::instance(SGT);

		files.clear();
		if (!rpc_mgr.dir(files, sftp_prefix, rdir_name, pattern))
			return;

		BOOST_FOREACH(const string& filename, files) {
			string full_name = rdir_name + filename;
			(void)rpc_mgr.unlink(sftp_prefix, full_name);
		}
	}
}

DECLARE_SA_OUTPUT(OTYPE_TARGET, sa_otarget)

}
}

