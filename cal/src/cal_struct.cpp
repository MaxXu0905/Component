#include "cal_struct.h"
#include "calt_ctx.h"

namespace ai
{
namespace app
{

using namespace std;
using namespace ai::sg;
using namespace ai::scci;

field_data_t::field_data_t(int flag_, const string& busi_code_, const string& ref_busi_code_, int field_type_,
	int field_size_, int table_id_, int value_index_, int subject_index_, int deal_type_, bool use_default,
	const string& default_value_, int orig_deal_type_)
{
	flag = flag_;
	busi_code = busi_code_;
	ref_busi_code = ref_busi_code_;
	field_type = field_type_;
	field_size = field_size_;
	data = new char[field_size_];
	table_id = table_id_;
	value_index = value_index_;
	subject_index = subject_index_;
	deal_type = deal_type_;
	default_value = default_value_;
	orig_deal_type = orig_deal_type_;

	if (!busi_code.empty())
		flag |= FIELD_DATA_BELONG_BUSI;

	if (use_default) {
		flag |= FIELD_DATA_USE_DEFAULT;
		flag &= ~FIELD_DATA_NO_DEFAULT;
	}

	max_size = 1;
	if (flag & FIELD_DATA_GOTTEN) {
		memset(data, 0, field_size);
		data_size = 1;
	} else {
		data_size = 0;
	}

	in_array = NULL;
	func_idx = -1;
}

field_data_t::~field_data_t()
{
	delete []data;
	delete []in_array;
}

double field_data_t::get_double() const
{
	ostringstream fmt;

	switch (field_type) {
	case SQLTYPE_SHORT:
		return static_cast<double>(*reinterpret_cast<short *>(data));
	case SQLTYPE_INT:
		return  static_cast<double>(*reinterpret_cast<int *>(data));
	case SQLTYPE_LONG:
		return static_cast<double>(*reinterpret_cast<long *>(data));
	case SQLTYPE_FLOAT:
		return static_cast<double>(*reinterpret_cast<float *>(data));
	case SQLTYPE_DOUBLE:
		return *reinterpret_cast<double *>(data);
	default:
		fmt << "ERROR: Unsupported field_type " << field_type << " for busi_code " << busi_code;
		throw bad_msg(__FILE__, __LINE__, 457, fmt.str());
	}
}

void field_data_t::set_data(const string& value)
{
	switch (field_type) {
	case SQLTYPE_CHAR:
		data[0] = value[0];
		break;
	case SQLTYPE_SHORT:
		*reinterpret_cast<short *>(data) = static_cast<short>(atoi(value.c_str()));
		break;
	case SQLTYPE_INT:
		*reinterpret_cast<int *>(data) = atoi(value.c_str());
		break;
	case SQLTYPE_LONG:
		*reinterpret_cast<long *>(data) = atol(value.c_str());
		break;
	case SQLTYPE_FLOAT:
		*reinterpret_cast<float *>(data) = static_cast<float>(atof(value.c_str()));
		break;
	case SQLTYPE_DOUBLE:
		*reinterpret_cast<double *>(data) = atof(value.c_str());
		break;
	case SQLTYPE_STRING:
		memcpy(data, value.c_str(), value.length() + 1);
		break;
	case SQLTYPE_TIME:
		*reinterpret_cast<time_t *>(data) = static_cast<time_t>(atol(value.c_str()));
		break;
	default:
		throw bad_msg(__FILE__, __LINE__, 458, "ERROR: unsupported field_type given.");
	}
}

ostream& operator<<(ostream& os, const field_data_t& field_data)
{
	if (!(field_data.flag & FIELD_DATA_HAS_ALIAS)) {
		os << field_data.busi_code;
	} else {
		calt_ctx *CALT = calt_ctx::instance();
		cal_busi_alias_t alias;

		alias.rule_id = CALT->_CALT_rule_id;
		alias.busi_code = field_data.busi_code;

		map<cal_busi_alias_t, string>::const_iterator iter = CALT->_CALT_busi_alias.find(alias);
		if (iter == CALT->_CALT_busi_alias.end())
			os << field_data.busi_code;
		else
			os << iter->second;
	}

	os << '=';

	const char *data = field_data.data;
	for (int i = 0; i < field_data.data_size; i++) {
		if (i > 0)
			os << ':';

		switch (field_data.field_type) {
		case SQLTYPE_CHAR:
			os << data[0];
			break;
		case SQLTYPE_SHORT:
			os << *reinterpret_cast<const short *>(data);
			break;
		case SQLTYPE_INT:
			os << *reinterpret_cast<const int *>(data);
			break;
		case SQLTYPE_LONG:
			os << *reinterpret_cast<const long *>(data);
			break;
		case SQLTYPE_FLOAT:
			os << std::fixed << std::setprecision(6) << *reinterpret_cast<const float *>(data);
			break;
		case SQLTYPE_DOUBLE:
			os << std::fixed << std::setprecision(6) << *reinterpret_cast<const double *>(data);
			break;
		case SQLTYPE_STRING:
			os << data;
			break;
		case SQLTYPE_TIME:
			os << *reinterpret_cast<const time_t *>(data);
			break;
		}

		data += field_data.field_size;
	}

	return os;
}

dstream& operator<<(dstream& os, const field_data_t& field_data)
{
#if defined(DEBUG)
	cout << field_data;
#endif

	return os;
}


field_data_t::field_data_t()
{
	flag = 0;
	field_type = -1;
	field_size = -1;
	max_size = 1;
	data_size = 0;
	data = NULL;
	table_id = -1;
	value_index = -1;
	subject_index = -1;
	deal_type = -1;
	orig_deal_type = -1;
	in_array = NULL;
	func_idx = -1;
}

}
}

