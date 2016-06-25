#include <dlfcn.h>
#include "sa_internal.h"

using namespace ai::sg;

namespace ai
{
namespace app
{

boost::once_flag sap_ctx::once_flag = BOOST_ONCE_INIT;
sap_ctx * sap_ctx::_instance = NULL;

sap_ctx * sap_ctx::instance()
{
	if (_instance == NULL)
		boost::call_once(once_flag, sap_ctx::init_once);
	return _instance;
}

void sap_ctx::dump(ostream& os)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(100, __PRETTY_FUNCTION__, "", NULL);
#endif

	os << "The following error table(s) should be created before run:\n\n";
	BOOST_FOREACH(const sa_adaptor_t& adaptor, _SAP_adaptors) {
		const sa_parms_t& parms = adaptor.parms;
		const vector<input_record_t>& input_records = adaptor.input_records;
		bool multiple_flag = (input_records.size() > 1);

		for (int i = 0; i < input_records.size(); i++) {
			os << "create table err_" << parms.com_key << "_" << parms.svc_key;
			if (multiple_flag)
				os << "_" << i;
			os << "\n(\n"
				<< "\tfile_name varchar2(63) not null,\n"
				<< "\tfile_sn number(10) not null,\n"
				<< "\terror_code number(6) not null,\n"
				<< "\terror_pos number(3) not null,\n";

			BOOST_FOREACH(const input_field_t& field, input_records[i].field_vector) {
				os << "\t" << field.field_name << " varchar2(";
				if (field.encode_type == ENCODE_ASCII)
					os << field.factor2;
				else
					os << (field.factor2 * 2);
				os << "),\n";
			}
			os << "\tredo_mark number(10) not null,\n"
				<< "\tsrc_record varchar2(4000) not null,\n"
				<< "\tredo_flag number(1) not null\n"
				<< ");\n\n";
		}
	}
}

sa_base * sap_ctx::sa_base_factory(const string& com_key)
{
	bool found = false;
	BOOST_FOREACH(const sa_base_creator_t& v, _SAP_base_creator) {
		if (v.com_key == com_key) {
			try {
				sat_ctx *SAT = sat_ctx::instance();
				int sa_id = SAT->_SAT_sas.size();

				sa_base *sa = (*v.create)(sa_id);
				sa->init();
				SAT->_SAT_sas.push_back(sa);
				return sa;
			} catch (exception& ex) {
				throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Failed to create operation adaptor for {1}, {2}") % com_key % ex.what()).str(APPLOCALE));
			}

			found = true;
			break;
		}
	}

	throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Can't find operation adaptor for {1}") % com_key).str(APPLOCALE));
}

sa_input * sap_ctx::sa_input_factory(sa_base& sa, int flags, const string& class_name)
{
	bool found = false;
	BOOST_FOREACH(const sa_input_creator_t& v, _SAP_input_creator) {
		if (v.class_name == class_name) {
			try {
				return (*v.create)(sa, flags);
			} catch (exception& ex) {
				throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Failed to create input class for {1}, {2}") % class_name % ex.what()).str(APPLOCALE));
			}

			found = true;
			break;
		}
	}

	throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Can't find input class for {1}") % class_name).str(APPLOCALE));
}

sa_output * sap_ctx::sa_output_factory(sa_base& sa, int flags, const string& class_name)
{
	bool found = false;
	BOOST_FOREACH(const sa_output_creator_t& v, _SAP_output_creator) {
		if (v.class_name == class_name) {
			try {
				if (!(v.flags & flags))
					throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Can't create output class {1} for given flags {2}") % class_name % flags).str(APPLOCALE));

				return (*v.create)(sa, flags);
			} catch (exception& ex) {
				throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Failed to create output class for {1}, {2}") % class_name % ex.what()).str(APPLOCALE));
			}

			found = true;
			break;
		}
	}

	throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Can't find output class for {1}") % class_name).str(APPLOCALE));
}

sap_ctx::sap_ctx()
{
	_SAP_pid = getpid();

	_SAP_nclean = false;
	_SAP_undo_id = 0;

	_SAP_cmpl.set_cplusplus(true);
	_SAP_cmpl.set_search_type(SEARCH_TYPE_CAL);
	_SAP_auto_mkdir = false;

	gpenv& env_mgr = gpenv::instance();
	char *ptr = env_mgr.getenv("SA_RECORD_NO");
	if (ptr == NULL)
		_SAP_record_no = std::numeric_limits<int>::max();
	else
		_SAP_record_no = ::atoi(ptr);
}

sap_ctx::~sap_ctx()
{
	BOOST_FOREACH(void *handle, _SAP_lib_handles) {
		::dlclose(handle);
	}
}

void sap_ctx::init_once()
{
	_instance = new sap_ctx();
}

}
}

