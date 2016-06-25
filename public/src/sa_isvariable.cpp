#include "sa_internal.h"

namespace ai
{
namespace app
{

using namespace ai::sg;
using namespace ai::scci;

sa_isvariable::sa_isvariable(sa_base& _sa, int _flags)
	: sa_isocket(_sa, _flags)
{
	const map<string, string>& args = adaptor.source.args;
	map<string, string>::const_iterator iter;

	iter = args.find("escape");
	if (iter == args.end()) {
		escape = '\0';
	} else {
		const string& escape_str = iter->second;
		if (escape_str.size() != 1)
			throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: one and only one character is allowed for escape node")).str(APPLOCALE));
		else
			escape = escape_str[0];
	}
}

sa_isvariable::~sa_isvariable()
{
}

bool sa_isvariable::to_fields(const input_record_t& input_record)
{
	const vector<input_field_t>& field_vector = input_record.field_vector;
	char **input = sa.get_input();
	int i = FIXED_INPUT_SIZE;
	string value;

	SAT->_SAT_error_pos = -1;
	if (escape == '\0') {
		char sep_str[2];
		sep_str[0] = input_record.delimiter;
		sep_str[1] = '\0';
		boost::char_separator<char> sep(sep_str, "", boost::keep_empty_tokens);
		boost::tokenizer<boost::char_separator<char> > tok(SAT->_SAT_rstr, sep);

		for (boost::tokenizer<boost::char_separator<char> >::iterator token_iter = tok.begin(); token_iter != tok.end(); ++token_iter) {
			const input_field_t& field = field_vector[i];
			value = boost::trim_copy(*token_iter);

			if (value.length() <= field.factor2) { // In range of max keep length
				memcpy(input[i], value.c_str(), value.length() + 1);
			} else if (value.length() <= field.factor1) { // In range of max allow length
				memcpy(input[i], value.c_str(), field.factor2);
				input[i][field.factor2] = '\0';
			} else {
				memcpy(input[i], value.c_str(), field.factor2);
				input[i][field.factor2] = '\0';
				if (SAT->_SAT_error_pos == -1)
					SAT->_SAT_error_pos = i - FIXED_INPUT_SIZE;
			}

			if (++i >= field_vector.size())
				break;
		}
	} else {
		boost::escaped_list_separator<char> sep(escape, input_record.delimiter);
		boost::tokenizer<boost::escaped_list_separator<char> > tok(SAT->_SAT_rstr, sep);

		for (boost::tokenizer<boost::escaped_list_separator<char> >::iterator token_iter = tok.begin(); token_iter != tok.end(); ++token_iter) {
			const input_field_t& field = field_vector[i];
			value = boost::trim_copy(*token_iter);

			if (value.length() <= field.factor2) { // In range of max keep length
				memcpy(input[i], value.c_str(), value.length() + 1);
			} else if (value.length() <= field.factor1) { // In range of max allow length
				memcpy(input[i], value.c_str(), field.factor2);
				input[i][field.factor2] = '\0';
			} else {
				memcpy(input[i], value.c_str(), field.factor2);
				input[i][field.factor2] = '\0';
				if (SAT->_SAT_error_pos == -1)
					SAT->_SAT_error_pos = i - FIXED_INPUT_SIZE;
			}

			if (++i >= field_vector.size())
				break;
		}
	}

	for (; i < field_vector.size(); i++)
		input[i][0] = '\0';

	dout << "In " << __PRETTY_FUNCTION__ << ", field size = " << field_vector.size() << "\n";
	for (i = 0; i < field_vector.size(); i++) {
		const input_field_t& field = field_vector[i];
		dout << field.field_name << " = [" << input[i] << "] length = " << strlen(input[i]) << "\n";
	}
	dout << std::flush;

	return (SAT->_SAT_error_pos == -1);
}

DECLARE_SA_INPUT(0, sa_isvariable)

}
}

