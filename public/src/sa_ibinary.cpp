#include "sa_internal.h"

namespace ai
{
namespace app
{

using namespace ai::sg;
using namespace ai::scci;

sa_ibinary::sa_ibinary(sa_base& _sa, int _flags)
	: sa_ifixed(_sa, _flags)
{
	const map<string, string>& args = adaptor.source.args;
	map<string, string>::const_iterator iter;

	iter = args.find("block_filler");
	if (iter == args.end())
		block_filler = -1;
	else
		block_filler = atoi(iter->second.c_str());

	iter = args.find("rule_record_len");
	if (iter == args.end())
		throw sg_exception(__FILE__, __LINE__, SGEINVAL, 0, (_("ERROR: source.args.arg.rule_record_len value invalid.")).str(APPLOCALE));
	else
		rule_record_len = iter->second;

	const vector<input_record_t>& input_records = adaptor.input_records;

	input_mlen = std::numeric_limits<int>::max();
	BOOST_FOREACH(const input_record_t& input_record, input_records) {
		const vector<input_field_t>& field_vector = input_record.field_vector;

		int input_len = 0;
		for (int i = 0; i < field_vector.size(); i++)
			input_len += field_vector[i].factor2;

		input_mlen = std::min(input_mlen, input_len);
		input_lens.push_back(input_len);
	}
}

sa_ibinary::~sa_ibinary()
{
}

void sa_ibinary::init()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(600, __PRETTY_FUNCTION__, "", NULL);
#endif
	compiler& cmpl = SAP->_SAP_cmpl;

	try {
		record_len_idx = cmpl.create_function(rule_record_len, field_map_t(), field_map_t(), field_map_t());
	} catch (exception& ex) {
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Can't create function for rule_record_len, {1}") % ex.what()).str(APPLOCALE));
	}

	sa_ifixed::init();
}

void sa_ibinary::init2()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(600, __PRETTY_FUNCTION__, "", NULL);
#endif
	compiler& cmpl = SAP->_SAP_cmpl;

	record_len_func = cmpl.get_function(record_len_idx);

	BOOST_FOREACH(const int& idx, record_serial_idx) {
		record_serial_func.push_back(cmpl.get_function(idx));
	}

	sa_ifixed::init2();
}

int sa_ibinary::read()
{
	int retval = SA_SUCCESS;
#if defined(DEBUG)
	scoped_debug<int> debug(600, __PRETTY_FUNCTION__, "", &retval);
#endif

	int ch;
	while (1) {
		ch = is->peek();
		if (ch == EOF)
			break;

		if (ch == block_filler)
			is->seekg(1, std::ios::cur);
		else
			break;
	}

	if (ch == EOF) {
		retval = SA_EOF;
		return retval;
	}

	char record_buf[MAX_BLOCK_LEN];
	is->read(record_buf, input_mlen);
	if (!*is) {
		api_mgr.write_log(__FILE__, __LINE__, (_("ERROR: Can't read enough data from file {1}") % src_name).str(APPLOCALE));
		return SA_FATAL;
	}

	SAT->_SAT_rstr.assign(record_buf, record_buf + input_mlen);
	SAT->_SAT_rstrs.push_back(SAT->_SAT_rstr);
	dout << "record = [" << SAT->_SAT_rstr << "] length = " << SAT->_SAT_rstr.length() << std::endl;

	if ((master.record_count == 0) && (record_flag & RECORD_FLAG_HAS_HEAD))    // Head record.
		SAT->_SAT_record_type = RECORD_TYPE_HEAD;
	else if ((is->tellg() == end_pos) && (record_flag & RECORD_FLAG_HAS_TAIL))    // Tail record.
		SAT->_SAT_record_type = RECORD_TYPE_TAIL;
	else
		SAT->_SAT_record_type = RECORD_TYPE_BODY;

	retval = do_record();
	return retval;
}

int sa_ibinary::do_record()
{
	int retval = SA_SUCCESS;
#if defined(DEBUG)
	scoped_debug<int> debug(600, __PRETTY_FUNCTION__, "", &retval);
#endif

	int record_serial = get_record_serial(SAT->_SAT_rstr.c_str());
	if (record_serial < 0) {
		int record_len = get_record_len(SAT->_SAT_rstr.c_str());
		if (record_len <= 0) {
			retval = SA_FATAL;
			return retval;
		} else if (record_len > input_mlen) {
			char record_buf[MAX_BLOCK_LEN];
			is->read(record_buf, record_len - input_mlen);
			SAT->_SAT_rstr += record_buf;
		} else if (record_len < input_mlen) {
			is->seekg(record_len - input_mlen, std::ios::cur);
			SAT->_SAT_rstr.resize(record_len);
		}

		if (SAT->_SAT_record_type == RECORD_TYPE_BODY)
			retval = SA_INVALID;
		else
			retval = SA_SKIP;
		return retval;
	}

	const vector<input_record_t>& input_records = adaptor.input_records;
	const input_record_t& input_record = input_records[record_serial];
	const int& input_len = input_lens[record_serial];

	if (input_len > input_mlen) { // Read remain part of record
		char record_buf[MAX_BLOCK_LEN];
		is->read(record_buf, input_len - input_mlen);
		SAT->_SAT_rstr += record_buf;
	}

	if (!to_fields(input_record)) {
		retval = SA_ERROR;
		return retval;
	}

	return retval;
}

int sa_ibinary::get_record_len(const char *record) const
{
	char result[256];
	char *out[1] = { result };
	if ((*record_len_func)(NULL, 0, &record, out) < 0)
		return -1;
	else
		return ::atoi(result);
}

DECLARE_SA_INPUT(0, sa_ibinary)

}
}


