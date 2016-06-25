#include "sa_internal.h"

namespace ai
{
namespace app
{

using namespace ai::sg;
using namespace ai::scci;

sa_isfixed::sa_isfixed(sa_base& _sa, int _flags)
	: sa_isocket(_sa, _flags)
{
}

sa_isfixed::~sa_isfixed()
{
}

bool sa_isfixed::to_fields(const input_record_t& input_record)
{
	const vector<input_field_t>& field_vector = input_record.field_vector;
	char **input = sa.get_input();
	int i;
	char buff[4096];

	SAT->_SAT_error_pos = -1;
	for (i = FIXED_INPUT_SIZE; i < field_vector.size(); i++) {
		const input_field_t& field = field_vector[i];

		if (field.factor1 + field.factor2 > SAT->_SAT_rstr.length()) {
			// The last one can be variable length.
			if (i == field_vector.size() - 1) {
				if (SAT->_SAT_rstr.length() > field.factor1) {
					::memcpy(buff, SAT->_SAT_rstr.c_str() + field.factor1, SAT->_SAT_rstr.length() - field.factor1);
					buff[SAT->_SAT_rstr.length() - field.factor1] = '\0';
				} else {
					buff[0] = '\0';
				}
				break;
			} else {
				SAT->_SAT_error_pos = i - FIXED_INPUT_SIZE;
			}
		}
		// If an error occurred, set the rest fields to null string.
		if (SAT->_SAT_error_pos == -1) {
			::memcpy(buff, SAT->_SAT_rstr.c_str() + field.factor1, field.factor2);
			buff[field.factor2] = '\0';
		} else {
			buff[0] = '\0';
		}

		switch (field.encode_type) {
		case ENCODE_ASCII:
			common::trim(buff, ' ');
			::strcpy(input[i], buff);
			break;
		case ENCODE_BCD:
			asn1::bcd2asc(buff, input[i], field.factor2 * 2);
			break;
		case ENCODE_TBCD:
			asn1::tbcd2asc(buff, input[i], field.factor2 * 2);
			break;
		default:
			api_mgr.write_log(__FILE__, __LINE__, (_("ERROR: unknown encode_type {1}") % field.encode_type).str(APPLOCALE));
			return false;
		}
	}

	dout << "In " << __PRETTY_FUNCTION__ << ", field size = " << field_vector.size() << "\n";
	for (i = 0; i < field_vector.size(); i++) {
		const input_field_t& field = field_vector[i];
		dout << field.field_name << " = [" << input[i] << "] length = " << strlen(input[i]) << "\n";
	}
	dout << std::flush;

	return (SAT->_SAT_error_pos == -1);
}

DECLARE_SA_INPUT(0, sa_isfixed)

}
}

