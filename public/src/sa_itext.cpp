#include "sa_internal.h"
#include "asn1.h"

namespace ai
{
namespace app
{

using namespace ai::sg;
using namespace ai::scci;
typedef boost::tokenizer<boost::char_separator<char> > tokenizer;

sa_itext::sa_itext(sa_base& _sa, int _flags)
	: sa_istream(_sa, _flags)
{
	const map<string, string>& args = adaptor.source.args;
	map<string, string>::const_iterator iter;

	record_flag = 0;

	iter = args.find("record_flag");
	if (iter != args.end()) {
		boost::char_separator<char> sep(" \t\b");
		tokenizer tokens(iter->second, sep);
		for (tokenizer::iterator iter = tokens.begin(); iter != tokens.end(); ++iter) {
			string str = *iter;

			if (strcasecmp(str.c_str(), "HEAD") == 0)
				record_flag |= RECORD_FLAG_HAS_HEAD;
			else if (strcasecmp(str.c_str(), "TAIL") == 0)
				record_flag |= RECORD_FLAG_HAS_TAIL;
		}
	}
}

sa_itext::~sa_itext()
{
}

int sa_itext::read()
{
	int retval = SA_SUCCESS;
#if defined(DEBUG)
	scoped_debug<int> debug(600, __PRETTY_FUNCTION__, "", &retval);
#endif

	while (1) {
		if (is->eof()) {
			retval = SA_EOF;
			return retval;
		}

		std::getline(*is, SAT->_SAT_rstr);
		if (!SAT->_SAT_rstr.empty())
			break;
	}

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

int sa_itext::do_record()
{
	int retval = SA_SUCCESS;
#if defined(DEBUG)
	scoped_debug<int> debug(600, __PRETTY_FUNCTION__, "", &retval);
#endif

	int record_serial = get_record_serial(SAT->_SAT_rstr.c_str());
	if (record_serial < 0) {
		if (SAT->_SAT_record_type == RECORD_TYPE_BODY)
			retval = SA_INVALID;
		else
			retval = SA_SKIP;
		return retval;
	}

	const vector<input_record_t>& input_records = adaptor.input_records;
	const input_record_t& input_record = input_records[record_serial];

	if (!to_fields(input_record)) {
		retval = SA_ERROR;
		return retval;
	}

	return retval;
}

int sa_itext::get_record_serial(const char *record) const
{
	int& input_serial = sa.get_input_serial();
	input_serial = -1;
#if defined(DEBUG)
	scoped_debug<int> debug(600, __PRETTY_FUNCTION__, (_("record={1}") % record).str(APPLOCALE), &input_serial);
#endif
	const vector<input_record_t>& input_records = adaptor.input_records;
	char **global = sa.get_global();

	for (int i = 0; i < record_serial_idx.size(); i++) {
		if (input_records[i].record_type != SAT->_SAT_record_type)
			continue;

		if ((*record_serial_func[i])(NULL, global, &record, 0) == 0) {
			input_serial = i;
			return input_serial;
		}
	}

	return input_serial;
}

}
}

