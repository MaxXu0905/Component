#include "cal_rule.h"
#include "cal_policy.h"
#include "cal_tree.h"

namespace ai
{
namespace app
{

using namespace std;
using namespace ai::sg;
using namespace ai::scci;

bool cal_rule_area_t::operator<(const cal_rule_area_t& rhs) const
{
	if (rule_id < rhs.rule_id)
		return true;
	else if (rule_id > rhs.rule_id)
		return false;

	return city_code < rhs.city_code;
}

bool cal_rule_calc_detail_t::operator<(const cal_rule_calc_detail_t& rhs) const
{
	return lower_value < rhs.lower_value;
}

cal_rule_t::cal_rule_t()
{
	flag = RULE_FLAG_NO_CALC_BUSI;
	calc_field_data = NULL;
	sum_field_data = NULL;
	valid = true;
}

cal_rule_t::~cal_rule_t()
{
}

bool cal_rule_t::has_accu_cond(const string& busi_code)
{
	BOOST_FOREACH(const cal_accu_cond_t& item, accu_cond) {
		if (item.busi_code == busi_code)
			return true;
	}

	return false;
}

bool cal_rule_t::has_acct_busi_cond(const string& busi_code)
{
	BOOST_FOREACH(const cal_accu_cond_t& item, acct_busi_cond) {
		if (item.busi_code == busi_code)
			return true;
	}

	return false;
}
bool cal_rule_complex_t::operator<(const cal_rule_complex_t& rhs) const
{
	if (rule_id < rhs.rule_id)
		return true;
	else if (rule_id > rhs.rule_id)
		return false;

	return busi_code < rhs.busi_code;
}

string_token::string_token(const string& source_)
	: source(source_)
{
	iter = source.begin();
}

string_token::~string_token()
{
}

/*
 * 获取一个单词，有以下两种情况:
 * 1. 单个字符
 * 2. 以双引号标识的字符串
 */
string string_token::next()
{
	string token;
	char ch;

	// 去掉前导的空白字符
	for (; iter != source.end(); ++iter) {
		if (!::isspace(*iter))
			break;
	}

	if (iter == source.end())
		return token;

	if (*iter != '\"') {
		token += *iter;
		++iter;
		return token;
	}

	for (++iter; iter != source.end(); ++iter) {
		ch = *iter;
		if (ch == '\"') {
			++iter;
			return token;
		} else {
			token += ch;
		}
	}

	return token;
}

cal_rule& cal_rule::instance(calt_ctx *CALT)
{
	return *reinterpret_cast<cal_rule *>(CALT->_CALT_mgr_array[CAL_RULE_MANAGER]);
}

void cal_rule::load()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	load_rule_area();
	load_rule();
	load_rule_kpi();
	load_rule_detail();
	load_rule_detail_expr();
	load_rule_calc_detail();
	load_rule_payment();
	load_rule_payment_detail();
	load_rule_complex();
}

int cal_rule::do_calculate(const cal_rule_t& rule_cfg)
{
	int retval = -1;
#if defined(DEBUG)
	scoped_debug<int> debug(500, __PRETTY_FUNCTION__, "", &retval);
#endif

	const cal_parms_t& parms = CALP->_CALP_parms;

	if (!parms.channel_level)
		retval = do_user_calculate(rule_cfg);
	else
		retval = do_channel_calculate(rule_cfg);

	return retval;
}

// 返回状态:
// 0. 输出正常记录
// 1. 输出冻结记录
// 2. 输出正常和冻结记录
int cal_rule::do_pay(const cal_rule_t& rule_cfg)
{
	int status = -1;
#if defined(DEBUG)
	scoped_debug<int> debug(500, __PRETTY_FUNCTION__, "", &status);
#endif

	cal_tree& tree_mgr = cal_tree::instance(CALT);
	map<string, double>& chl_taxtype_map = CALT->_CALT_chl_taxtype_map;
	field_data_t **global_fields = CALT->_CALT_global_fields;
	const cal_rule_payment_t& pay_cfg = rule_cfg.pay_cfg;
	field_data_t *calc_track_field = global_fields[CALC_TRACK_GI];
	char *calc_track = calc_track_field->data;
	int& field_size = calc_track_field->field_size;
	string calc_str;

	double comm_fee = *reinterpret_cast<double *>(global_fields[COMM_FEE_GI]->data);
	double& fee = *reinterpret_cast<double *>(global_fields[FEE_GI]->data);
	double& sett_fee = *reinterpret_cast<double *>(global_fields[SETT_FEE_GI]->data);
	double& expire_fee = *reinterpret_cast<double *>(global_fields[EXPIRE_FEE_GI]->data);
	double& vat_price = *reinterpret_cast<double *>(global_fields[VAT_PRICE_GI]->data);
	double& vat_tax = *reinterpret_cast<double *>(global_fields[VAT_TAX_GI]->data);
	double tax_value;
	int& freeze_expire = *reinterpret_cast<int *>(global_fields[FREEZE_EXPIRE_GI]->data);
	int pay_date;
	char *arre_cycle = global_fields[ARRE_CYCLE_GI]->data;
	char *sett_cycle = global_fields[SETT_CYCLE_GI]->data;
	char *pay_chnl_id= global_fields[PAY_CHNL_ID_GI]->data;
	const vector<cal_rule_payment_detail_t>& pay_detail = pay_cfg.pay_detail;
	vector<cal_rule_payment_detail_t>::const_iterator pay_iter;

	field_data_t *taxpayer_type = global_fields[TAXPAYER_TYPE_GI];
	if (!(taxpayer_type->flag & FIELD_DATA_GOTTEN))
		tree_mgr.get_field_value(taxpayer_type);

	if (taxpayer_type->data_size != 1)
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: pay_chnl_id = {1}, taxpayer_type not found.") % pay_chnl_id).str(APPLOCALE));

	map<string, double>::const_iterator chl_taxtype_iter = chl_taxtype_map.find(taxpayer_type->data);
	if (chl_taxtype_iter != chl_taxtype_map.end())
		tax_value = chl_taxtype_iter->second;
	else
		tax_value = 0.0;

	switch (pay_cfg.pay_type) {
	case PAY_TYPE_ONCE:
		fee = comm_fee;
		sett_fee = comm_fee;
		freeze_expire = 1;
		status = PAY_RETURN_NORMAL;
		break;
	case PAY_TYPE_PERCENT:
		pay_date = get_pay_date(pay_cfg);

		// 超出最大支付周期
		if (pay_date >= pay_cfg.pay_cycle) {
			fee = 0.0;
			status = PAY_RETURN_FREEZE;
			freeze_expire = 1;
			break;
		}

		if (pay_date == pay_cfg.pay_cycle - 1)
			freeze_expire = 1;
		else
			freeze_expire = 0;

		for (pay_iter = pay_detail.begin(); pay_iter != pay_detail.end(); ++pay_iter) {
			if (pay_iter->pay_date == pay_date) {
				if (check_cond(pay_iter->pay_cond)) {
					fee = comm_fee * pay_iter->pay_value / 100.0;
					sett_fee += fee;
					calc_str += (boost::format("%1%*%2%%%") % comm_fee % pay_iter->pay_value).str();
					status = PAY_RETURN_NORMAL_FREEZE;
				} else {
					fee = 0.0;
					expire_fee += comm_fee * pay_iter->pay_value / 100.0;
					status = PAY_RETURN_FREEZE;
				}
				break;
			}
		}
		if (pay_iter == pay_detail.end()) {
			fee = 0.0;
			status = PAY_RETURN_FREEZE;
		}
		break;
	case PAY_TYPE_ABSOLUTE:
		pay_date = get_pay_date(pay_cfg);

		// 超出最大支付周期
		if (pay_date >= pay_cfg.pay_cycle) {
			fee = 0.0;
			status = PAY_RETURN_FREEZE;
			freeze_expire = 1;
			break;
		}

		if (pay_date == pay_cfg.pay_cycle - 1)
			freeze_expire = 1;
		else
			freeze_expire = 0;

		for (pay_iter = pay_detail.begin(); pay_iter != pay_detail.end(); ++pay_iter) {
			if (pay_iter->pay_date == pay_date) {
				if (check_cond(pay_iter->pay_cond)) {
					if (comm_fee < 1e-6) {
						fee = 0.0;
					} else {
						fee = pay_iter->pay_value;
						sett_fee += fee;
					}
					status = PAY_RETURN_NORMAL_FREEZE;
				} else {
					fee = 0.0;
					if (comm_fee > 1e-6)
						expire_fee += pay_iter->pay_value;
					status = PAY_RETURN_FREEZE;
				}
				break;
			}
		}
		if (pay_iter == pay_detail.end()) {
			fee = 0.0;
			status = PAY_RETURN_FREEZE;
		}
		break;
	case PAY_TYPE_COND_ONCE:
		pay_date = get_pay_date(pay_cfg);
		// 在该支付类型下，pay_date为支付起始周期
		pay_iter = pay_detail.begin();
		pay_date -= pay_iter->pay_date;

		// 超出最大支付周期
		if (pay_date >= pay_cfg.pay_cycle) {
			fee = 0.0;
			status = PAY_RETURN_FREEZE;
			freeze_expire = 1;
			break;
		}

		if (pay_date < 0) { // 还没到支付周期开始点
			fee = 0.0;
			status = PAY_RETURN_FREEZE;
			freeze_expire = 0;
		} else if (check_cond(pay_iter->pay_cond)) {
			fee = comm_fee;
			sett_fee = comm_fee;
			status = PAY_RETURN_NORMAL_FREEZE;
			freeze_expire = 1;
		} else {
			fee = 0.0;
			status = PAY_RETURN_FREEZE;
			if (pay_date == pay_cfg.pay_cycle - 1) {
				expire_fee = comm_fee;
				freeze_expire = 1;
			} else {
				freeze_expire = 0;
			}
		}
		break;
	case PAY_TYPE_COND_CONT:
		//连续条件到达时，欠费账期为结算账期
		memcpy(arre_cycle, sett_cycle, ARRE_CYCLE_LEN);
		arre_cycle[ARRE_CYCLE_LEN] = '\0';

		pay_date = get_pay_date(pay_cfg);
		// 在该支付类型下，pay_date为支付起始周期
		pay_iter = pay_detail.begin();
		pay_date -= pay_iter->pay_date;

		// 超出最大支付周期
		if (pay_date >= pay_cfg.pay_cycle) {
			fee = 0.0;
			status = PAY_RETURN_FREEZE;
			freeze_expire = 1;
			break;
		}

		if (pay_date < 0) { // 还没到支付周期开始点
			fee = 0.0;
			status = PAY_RETURN_FREEZE;
			freeze_expire = 0;
		} else if (check_cond(pay_iter->pay_cond)) {
			if (pay_date == pay_cfg.pay_cycle - 1) {
				fee = comm_fee;
				sett_fee = comm_fee;
				status = PAY_RETURN_NORMAL_FREEZE;
				freeze_expire = 1;
			} else {
				fee = 0.0;
				status = PAY_RETURN_FREEZE;
				freeze_expire = 0;
			}
		} else {
			fee = 0.0;
			expire_fee = comm_fee;
			status = PAY_RETURN_FREEZE;
			freeze_expire = 1;
		}
		break;
	}

	vat_price = fee / (1 + tax_value);
	vat_tax = fee - vat_price;

	if (calc_str.empty())
		calc_str = boost::lexical_cast<string>(fee);

	int track_len;
	char *ptr = strchr(calc_track, ';');
	if (ptr != NULL)
		track_len = ptr - calc_track;
	else
		track_len = strlen(calc_track);

	// 如果预分配的内存不足，则扩展该内存
	if (track_len + 1 + (int)calc_str.length() >= field_size) {
		char *old_data = calc_track_field->data;
		calc_track_field->field_size = track_len + calc_str.length() + 2;
		calc_track_field->data = new char[calc_track_field->field_size];
		calc_track = calc_track_field->data;
		memcpy(calc_track, old_data, track_len);
		delete []old_data;
	}

	calc_track[track_len] = ';';
	memcpy(calc_track + track_len + 1, calc_str.c_str(), calc_str.length() + 1);

	return status;
}

bool cal_rule::check_rule(int rule_id, const cal_rule_t& rule_cfg)
{
	bool retval = false;
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, (_("rule_id={1}") % rule_id).str(APPLOCALE), &retval);
#endif

	field_data_t **global_fields = CALT->_CALT_global_fields;
	const set<cal_rule_area_t>& rule_area_set = CALT->_CALT_rule_area_set;

	// 首先检查当前规则是否适用于当前地市
	cal_rule_area_t key;
	key.rule_id = rule_id;
	key.city_code = "";
	set<cal_rule_area_t>::const_iterator area_iter = rule_area_set.find(key);
	if (area_iter == rule_area_set.end()) {
		key.city_code = global_fields[CITY_CODE_GI]->data;
		area_iter = rule_area_set.find(key);
		if (area_iter == rule_area_set.end()) {
			dout << "rule_id = " << rule_id << " is not in ai_rule_area, city_code = " << key.city_code << endl;
if(CALP->_CALP_inv_out) {
			CALT->_CALT_rule_error_no = ERULE_NOT_AREA;
			CALT->_CALT_rule_error_string = (_("city_code={1}") % key.city_code).str(APPLOCALE);
}
			return retval;
		}
	}

	const cal_rule_calc_cfg_t& calc_cfg = rule_cfg.calc_cfg;
	int cycle_id = get_cycle_id(calc_cfg);
	if (cycle_id < 0
		|| (rule_cfg.cycles > 1 && cycle_id >= rule_cfg.cycles)
		|| (rule_cfg.cycles <= 1 && cycle_id > 0 && calc_cfg.calc_cycle>0 && cycle_id >= calc_cfg.calc_cycle)) {
		dout << "rule_id = " << rule_id << " is out of cycles." << endl;
if(CALP->_CALP_inv_out) {
		CALT->_CALT_rule_error_no = ERULE_OUT_OF_CYCLE;
		CALT->_CALT_rule_error_string = (_("cycle_id={1}") % cycle_id).str(APPLOCALE);
}
		return retval;
	}

	retval = true;
	return retval;
}

bool cal_rule::check_cond(const vector<cal_cond_para_t>& conds)
{
	bool retval = false;
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, (_("rule_id={1}") % CALT->_CALT_rule_id).str(APPLOCALE), &retval);
#endif
	cal_tree& tree_mgr = cal_tree::instance(CALT);
	const cal_parms_t& parms = CALP->_CALP_parms;

	for (vector<cal_cond_para_t>::const_iterator cond_iter = conds.begin(); cond_iter != conds.end(); ++cond_iter) {
		field_data_t *field_data = cond_iter->field_data;
		if (!(field_data->flag & FIELD_DATA_GOTTEN))
			tree_mgr.get_field_value(field_data);

		if ((field_data->flag & (FIELD_DATA_SET_DEFAULT | FIELD_DATA_USE_DEFAULT)) == FIELD_DATA_SET_DEFAULT) {
			dout << "default data is set, not matched." << endl;
if(CALP->_CALP_inv_out) {
			CALT->_CALT_rule_error_no = ERULE_NO_DATA;
			ostringstream fmt;
			fmt << *field_data;
			CALT->_CALT_rule_error_string = fmt.str();
}
			return retval;
		}

		if (field_data->data_size != 1) {
if(CALP->_CALP_inv_out) {
			CALT->_CALT_rule_error_no = ERULE_TOO_MANY_VALUES;
			ostringstream fmt;
			fmt << *field_data;
			CALT->_CALT_rule_error_string = fmt.str();
}
			GPP->write_log(__FILE__, __LINE__, (_("ERROR: expect one value for field_name {1}, gotten {2} value(s).") % cond_iter->field_name % field_data->data_size).str(APPLOCALE));
			return retval;
		}

		long long_value = 0;
		double double_value = 0.0;
		string str_value;
		int const LONG_TYPE = 1;
		int const DOUBLE_TYPE = 2;
		int const STRING_TYPE = 3;
		int flag = 0;
		switch (field_data->field_type) {
		case SQLTYPE_CHAR:
			long_value = static_cast<long>(field_data->data[0]);
			flag = LONG_TYPE;
			break;
		case SQLTYPE_SHORT:
			long_value = static_cast<long>(*reinterpret_cast<short *>(field_data->data));
			flag = LONG_TYPE;
			break;
		case SQLTYPE_INT:
			long_value = static_cast<long>(*reinterpret_cast<int *>(field_data->data));
			flag = LONG_TYPE;
			break;
		case SQLTYPE_LONG:
			long_value = *reinterpret_cast<long *>(field_data->data);
			flag = LONG_TYPE;
			break;
		case SQLTYPE_FLOAT:
			double_value = static_cast<double>(*reinterpret_cast<float *>(field_data->data));
			flag = DOUBLE_TYPE;
			break;
		case SQLTYPE_DOUBLE:
			double_value = *reinterpret_cast<double *>(field_data->data);
			flag = DOUBLE_TYPE;
			break;
		case SQLTYPE_STRING:
			str_value = field_data->data;
			flag = STRING_TYPE;
			break;
		case SQLTYPE_TIME:
			long_value = static_cast<long>(*reinterpret_cast<time_t *>(field_data->data));
			flag = LONG_TYPE;
			break;
		}

		switch (cond_iter->field_type) {
		case SQLTYPE_LONG:
			if (flag == DOUBLE_TYPE)
				long_value = static_cast<long>(double_value * cond_iter->multiplicator + 0.5);
			else if (flag == STRING_TYPE)
				long_value = static_cast<long>(atol(str_value.c_str()) * cond_iter->multiplicator + 0.5);
			flag = LONG_TYPE;
			break;
		case SQLTYPE_DOUBLE:
			if (flag == LONG_TYPE)
				double_value = static_cast<double>(long_value);
			else if (flag == STRING_TYPE)
				double_value = ::atof(str_value.c_str());
			flag = DOUBLE_TYPE;
			break;
		case SQLTYPE_STRING:
			if (flag == LONG_TYPE) {
				str_value = boost::lexical_cast<string>(long_value);
				flag = STRING_TYPE;
			}
			break;
		}

		bool result = false;
		switch (flag) { // 长整型
		case LONG_TYPE:
			{
				const long& long_value1 = cond_iter->numeric_value.long_value;
				const long& long_value2 = cond_iter->numeric_value2.long_value;

				switch (cond_iter->opt) {
				case COND_OPT_GT:
					if (long_value > long_value1)
						result = true;
					break;
				case COND_OPT_GE:
					if (long_value >= long_value1)
						result = true;
					break;
				case COND_OPT_LT:
					if (long_value < long_value1)
						result = true;
					break;
				case COND_OPT_LE:
					if (long_value <= long_value1)
						result = true;
					break;
				case COND_OPT_EQ:
					if (long_value == long_value1)
						result = true;
					break;
				case COND_OPT_NE:
					if (long_value != long_value1)
						result = true;
					break;
				case COND_OPT_OOI:
					if (long_value > long_value1 && long_value < long_value2)
						result = true;
					break;
				case COND_OPT_OCI:
					if (long_value > long_value1 && long_value <= long_value2)
						result = true;
					break;
				case COND_OPT_COI:
					if (long_value >= long_value1 && long_value < long_value2)
						result = true;
					break;
				case COND_OPT_CCI:
					if (long_value >= long_value1 && long_value <= long_value2)
						result = true;
					break;
				case COND_OPT_INC:
					{
						string str = boost::lexical_cast<string>(long_value);
						string::size_type pos = cond_iter->value.find(str);
						if (pos != string::npos)
							result = true;
						break;
					}
				case COND_OPT_EXC:
					{
						string str = boost::lexical_cast<string>(long_value);
						string::size_type pos = cond_iter->value.find(str);
						if (pos == string::npos)
							result = true;
						break;
					}
				case COND_OPT_UNKNOWN:
					break;
				}
			}
			break;
		case DOUBLE_TYPE: // 浮点型
			{
				const double& double_value1 = cond_iter->numeric_value.double_value;
				const double& double_value2 = cond_iter->numeric_value2.double_value;

				switch (cond_iter->opt) {
				case COND_OPT_GT:
					if (double_value > double_value1)
						result = true;
					break;
				case COND_OPT_GE:
					if (double_value >= double_value1)
						result = true;
					break;
				case COND_OPT_LT:
					if (double_value < double_value1)
						result = true;
					break;
				case COND_OPT_LE:
					if (double_value <= double_value1)
						result = true;
					break;
				case COND_OPT_EQ:
					if (double_value == double_value1)
						result = true;
					break;
				case COND_OPT_NE:
					if (double_value != double_value1)
						result = true;
					break;
				case COND_OPT_OOI:
					if (double_value > double_value1 && double_value < double_value2)
						result = true;
					break;
				case COND_OPT_OCI:
					if (double_value > double_value1 && double_value <= double_value2)
						result = true;
					break;
				case COND_OPT_COI:
					if (double_value >= double_value1 && double_value < double_value2)
						result = true;
					break;
				case COND_OPT_CCI:
					if (double_value >= double_value1 && double_value <= double_value2)
						result = true;
					break;
				case COND_OPT_INC:
				case COND_OPT_EXC:
				case COND_OPT_UNKNOWN:
					break;
				}
			}
			break;
		case STRING_TYPE: // 字符串
			switch (cond_iter->opt) {
			case COND_OPT_GT:
				if (str_value > cond_iter->value)
					result = true;
				break;
			case COND_OPT_GE:
				if (str_value >= cond_iter->value)
					result = true;
				break;
			case COND_OPT_LT:
				if (str_value < cond_iter->value)
					result = true;
				break;
			case COND_OPT_LE:
				if (str_value <= cond_iter->value)
					result = true;
				break;
			case COND_OPT_EQ:
				if (str_value == cond_iter->value)
					result = true;
				break;
			case COND_OPT_NE:
				if (str_value != cond_iter->value)
					result = true;
				break;
			case COND_OPT_OOI:
				if (str_value > cond_iter->value && str_value < cond_iter->value2)
					result = true;
				break;
			case COND_OPT_OCI:
				if (str_value > cond_iter->value && str_value <= cond_iter->value2)
					result = true;
				break;
			case COND_OPT_COI:
				if (str_value >= cond_iter->value && str_value < cond_iter->value2)
					result = true;
				break;
			case COND_OPT_CCI:
				if (str_value >= cond_iter->value && str_value <= cond_iter->value2)
					result = true;
				break;
			case COND_OPT_INC:
				{
					string str = "," + str_value + ",";
					string::size_type pos = cond_iter->value.find(str);
					if (pos != string::npos)
						result = true;
					break;
				}
			case COND_OPT_EXC:
				{
					string str = "," + str_value + ",";
					string::size_type pos = cond_iter->value.find(str);
					if (pos == string::npos)
						result = true;
					break;
				}
			case COND_OPT_UNKNOWN:
				break;
			}
		}

		if (!result) {
			dout << "data = " << *cond_iter->field_data << ", opt = " << cond_iter->opt
				<< ", value = " << cond_iter->value << ", value2 = " << cond_iter->value2
				<< ", field_type = " << cond_iter->field_type << ", busi_code = " << cond_iter->busi_code
				<< ", field_name = " << cond_iter->field_name << endl;
if(CALP->_CALP_inv_out) {
			CALT->_CALT_rule_error_no = ERULE_NOT_MATCH;
			ostringstream fmt;
			fmt << *field_data;
			CALT->_CALT_rule_error_string = fmt.str();
}
			return retval;
		}
	}

	retval = true;
	return retval;
}

cal_rule_t& cal_rule::get_rule(int rule_id)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, (_("rule_id={1}") % rule_id).str(APPLOCALE), NULL);
#endif

	map<int, cal_rule_t>& rule_map = CALT->_CALT_rule_map;
	map<int, cal_rule_t>::iterator iter = rule_map.find(rule_id);
	if (iter == rule_map.end())
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Can't find rule_id {1}, please check configuration file to see if svc_key, tables, aliases, province_code, and driver_data are the same as step 1.") % rule_id).str(APPLOCALE));

	return iter->second;
}

cal_rule::cal_rule()
{
}

cal_rule::~cal_rule()
{
}

void cal_rule::load_rule_area()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	const cal_parms_t& parms = CALP->_CALP_parms;
	set<cal_rule_area_t>& rule_area_set = CALT->_CALT_rule_area_set;
	rule_area_set.clear();

	string sql_stmt = (boost::format("SELECT rule_id{int}, city_code{char[%1%]} "
		"FROM ai_rule_area WHERE (province_code = :province_code{char[%2%]} OR province_code IS NULL) "
		"AND rule_id IN (SELECT rule_id FROM ai_rule_calculation_cfg WHERE cycle_time_unit = ")
		% CITY_CODE_LEN % PROVINCE_CODE_LEN).str();

	if (parms.cycle_type == CYCLE_TYPE_DAY) {
		sql_stmt += "'2'";
	} else if (CALP->_CALP_run_complex) {
		if (parms.busi_limit_month == 1)
			sql_stmt += "'0' AND busi_limit_month = '1'";
		else if (parms.busi_limit_month == 2)
			sql_stmt += "'0' AND busi_limit_month = '2'";
		else
			throw bad_param(__FILE__, __LINE__, 0, (_("ERROR: invalid value for busi_limit_month")).str(APPLOCALE));
	} else {
		sql_stmt += "'0'";
	}

	sql_stmt += ")";

	try {
		Generic_Database *db = CALT->_CALT_db;
		auto_ptr<Generic_Statement> auto_stmt(db->create_statement());
		Generic_Statement *stmt = auto_stmt.get();

		auto_ptr<struct_dynamic> auto_data(db->create_data(true));
		struct_dynamic *data = auto_data.get();
		if (data == NULL) {
			GPP->write_log(__FILE__, __LINE__, (_("ERROR: Memory allocation failure")).str(APPLOCALE));
			exit(1);
		}

		data->setSQL(sql_stmt);
		stmt->bind(data);

		stmt->setString(1, parms.province_code);

		auto_ptr<Generic_ResultSet> auto_rset(stmt->executeQuery());
		Generic_ResultSet *rset = auto_rset.get();

		while (rset->next()) {
			cal_rule_area_t item;
			item.rule_id = rset->getInt(1);
			item.city_code = rset->getString(2);
			rule_area_set.insert(item);
		}
	} catch (bad_sql& ex) {
		throw bad_msg(__FILE__, __LINE__, 0, ex.what());
	}
}

void cal_rule::load_rule()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	const cal_parms_t& parms = CALP->_CALP_parms;
	map<int, cal_rule_t>& rule_map = CALT->_CALT_rule_map;
	rule_map.clear();

	string sql_stmt = (boost::format("SELECT a.rule_id{int}, a.eff_date{datetime}, "
		"nvl(a.exp_date, to_date('20371231235959', 'YYYYMMDDHH24MISS')){datetime}, b.cal_start_time_unit{int}, "
		"b.cal_start_time{int}, b.cycle_time_unit{int}, b.cycle_time{int}, b.cycle_type{int}, b.cal_method{int}, "
		"b.cal_cycle{int}, b.cal_start_type{int}, b.cal_symbol{int}, "
		"b.settlement_to_service{int}, b.as_operating_costs{int}, b.share_ratio{double} "
		"FROM ai_rule_config a, ai_rule_calculation_cfg b "
		"WHERE a.rule_id = b.rule_id "
		"AND a.rule_id IN (SELECT rule_id FROM ai_rule_area WHERE province_code = :province_code{char[%1%]} OR province_code IS NULL) "
		"AND b.cycle_time_unit = ")
		% PROVINCE_CODE_LEN).str();

	if (parms.cycle_type == CYCLE_TYPE_DAY) {
		sql_stmt += "'2'";
	} else if (CALP->_CALP_run_complex) {
		if (parms.busi_limit_month == 1)
			sql_stmt += "'0' AND busi_limit_month = '1'";
		else if (parms.busi_limit_month == 2)
			sql_stmt += "'0' AND busi_limit_month = '2'";
		else
			throw bad_param(__FILE__, __LINE__, 0, (_("ERROR: invalid value for busi_limit_month")).str(APPLOCALE));
	} else {
		sql_stmt += "'0'";
	}

	try {
		Generic_Database *db = CALT->_CALT_db;
		auto_ptr<Generic_Statement> auto_stmt(db->create_statement());
		Generic_Statement *stmt = auto_stmt.get();

		auto_ptr<struct_dynamic> auto_data(db->create_data(true));
		struct_dynamic *data = auto_data.get();
		if (data == NULL) {
			GPP->write_log(__FILE__, __LINE__, (_("ERROR: Memory allocation failure")).str(APPLOCALE));
			exit(1);
		}

		data->setSQL(sql_stmt);
		stmt->bind(data);

		stmt->setString(1, parms.province_code);

		auto_ptr<Generic_ResultSet> auto_rset(stmt->executeQuery());
		Generic_ResultSet *rset = auto_rset.get();

		while (rset->next()) {
			cal_rule_t item;
			cal_rule_calc_cfg_t& calc_cfg = item.calc_cfg;

			int rule_id = rset->getInt(1);
			item.eff_date = static_cast<time_t>(rset->getLong(2));
			item.exp_date = static_cast<time_t>(rset->getLong(3));
			item.cycles = 0;

			calc_cfg.calc_start_time_unit = rset->getInt(4);
			if (calc_cfg.calc_start_time_unit < CALC_UNIT_MIN || calc_cfg.calc_start_time_unit > CALC_UNIT_MAX) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: calc_start_time_unit for rule_id {1} is not correct.") % rule_id).str(APPLOCALE));
				item.valid = false;
			}

			calc_cfg.calc_start_time = rset->getInt(5);
			calc_cfg.cycle_time_unit = rset->getInt(6);
			if (calc_cfg.cycle_time_unit < CYCLE_TIME_UNIT_MIN || calc_cfg.cycle_time_unit > CYCLE_TIME_UNIT_MAX) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: cycle_time_unit for rule_id {1} is not correct.") % rule_id).str(APPLOCALE));
				item.valid = false;
			}

			calc_cfg.cycle_time = rset->getInt(7);
			calc_cfg.cycle_type = rset->getInt(8);
			calc_cfg.calc_method = rset->getInt(9);
			if (calc_cfg.calc_method < CALC_METHOD_MIN || calc_cfg.calc_method > CALC_METHOD_MAX) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: calc_method for rule_id {1} is not correct.") % rule_id).str(APPLOCALE));
				item.valid = false;
			}

			calc_cfg.calc_cycle = rset->getInt(10);
			calc_cfg.calc_start_type = rset->getInt(11);
			calc_cfg.calc_symbol = rset->getInt(12);
			calc_cfg.settlement_to_service = rset->getInt(13);
			calc_cfg.as_operating_costs = rset->getInt(14);
			calc_cfg.share_ratio = rset->getDouble(15);
			//解决没有计算指标在load_rule_detail()里面执行不到的情况
			if (calc_cfg.calc_start_type == 1){
				item.flag |= RULE_FLAG_OPEN_TIME;
				if (parms.driver_data != "01" && parms.driver_data != "09")
					item.flag |= (RULE_FLAG_OPEN_TIME | RULE_FLAG_NULL_DRIVER | RULE_FLAG_CONFLICT_DRIVER);
			}
			rule_map[rule_id] = item;
		}
	} catch (bad_sql& ex) {
		throw bad_msg(__FILE__, __LINE__, 0, ex.what());
	}
}

// 加载政策相关的规则，只加载指定省分的数据
void cal_rule::load_rule_detail()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	cal_tree& tree_mgr = cal_tree::instance(CALT);
	const cal_parms_t& parms = CALP->_CALP_parms;
	map<int, cal_rule_t>& rule_map = CALT->_CALT_rule_map;

	string sql_stmt = (boost::format("SELECT rule_id{int}, busi_code{char[%1%]}, cal_value{char[4000]} "
		"FROM ai_rule_detail "
		"WHERE detail_type = 1 "
		"AND busi_code IS NOT NULL "
		"AND rule_id IN (SELECT rule_id FROM ai_rule_area WHERE (province_code = :province_code{char[%2%]} OR province_code IS NULL)) "
		"AND rule_id IN (SELECT rule_id FROM ai_rule_calculation_cfg WHERE cycle_time_unit = ")
		% BUSI_CODE_LEN % PROVINCE_CODE_LEN).str();

	if (parms.cycle_type == CYCLE_TYPE_DAY) {
		sql_stmt += "'2'";
	} else if (CALP->_CALP_run_complex) {
		if (parms.busi_limit_month == 1)
			sql_stmt += "'0' AND busi_limit_month = '1'";
		else if (parms.busi_limit_month == 2)
			sql_stmt += "'0' AND busi_limit_month = '2'";
		else
			throw bad_param(__FILE__, __LINE__, 0, (_("ERROR: invalid value for busi_limit_month")).str(APPLOCALE));
	} else {
		sql_stmt += "'0'";
	}

	sql_stmt += ")";

	try {
		Generic_Database *db = CALT->_CALT_db;
		auto_ptr<Generic_Statement> auto_stmt(db->create_statement());
		Generic_Statement *stmt = auto_stmt.get();

		auto_ptr<struct_dynamic> auto_data(db->create_data(true));
		struct_dynamic *data = auto_data.get();
		if (data == NULL) {
			GPP->write_log(__FILE__, __LINE__, (_("ERROR: Memory allocation failure")).str(APPLOCALE));
			exit(1);
		}

		data->setSQL(sql_stmt);
		stmt->bind(data);

		stmt->setString(1, parms.province_code);

		auto_ptr<Generic_ResultSet> auto_rset(stmt->executeQuery());
		Generic_ResultSet *rset = auto_rset.get();

		map<string, cal_busi_t>::iterator busi_iter;
		field_data_t *field_data;
		map<string, cal_busi_t>& busi_map = CALT->_CALT_busi_map;

		while (rset->next()) {
			int rule_id = rset->getInt(1);
			map<int, cal_rule_t>::iterator rule_iter = rule_map.find(rule_id);
			if (rule_iter == rule_map.end()) {
				// 如果只有规则细则，没有配置数据，则记录错误
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: rule_id {1} in ai_rule_detail is missing in ai_rule_config.") % rule_id).str(APPLOCALE));
				continue;
			}

			cal_rule_t& rule_cfg = rule_iter->second;
			if (!rule_cfg.valid)
				continue;

			cal_rule_calc_cfg_t& calc_cfg = rule_cfg.calc_cfg;
			string busi_code = rset->getString(2);
			string cal_value = rset->getString(3);
			if (memcmp(busi_code.c_str(), "expr", 4) == 0) {
				string alias_name = busi_code;
				if (!tree_mgr.add_rule_busi(rule_id, alias_name, cal_value, busi_code)) {
					GPP->write_log(__FILE__, __LINE__, (_("ERROR: For rule_id {1} busi_code {2} cal_value {3}, can't add rule busi.") % rule_id % busi_code % cal_value).str(APPLOCALE));
					rule_cfg.valid = false;
					continue;
				}

				busi_iter = busi_map.find(busi_code);
				cal_busi_t& busi_item = busi_iter->second;
				rule_cfg.sum_busi_code = busi_code;
				rule_cfg.sum_field_name = busi_item.field_name;
				rule_cfg.sum_field_data = busi_item.field_data;
				add_track_field(rule_cfg, rule_cfg.sum_field_data, 1);
				continue;
			}

			/*
			 * 如果是用户级的数据源，计算过程中要做到有效摊分，
			 * 实际上都是对单条记录的计算， 因此，渠道级的汇总指标，
			 * 在计算指标与参考指标中稍微有些不同的含义，
			 * 渠道级的汇总，在计算时，其含义是单条记录中的量
			 * 因此，在设置计算指标的值时，需要先用加后缀的指标(渠道级汇总
			 * 指标的原始来源值)来查找，找不到则表示该指标是非渠道级汇总指
			 * 标，再用正常的指标查找
			 */
			busi_iter = busi_map.find(busi_code);
			if (busi_iter == busi_map.end()) {
				// 如果计算指标没有定义，则记录错误
				// 如果计算指标是可忽略的指标，则不记录错误，直接忽略该规则
				if (!tree_mgr.ignored(busi_code)) {
					string str = __FILE__;
					str += ":";
					str += boost::lexical_cast<string>(__LINE__);
					str += ": ";
					str += (_("ERROR: For rule_id {1} busi_code {2} in ai_rule_detail is missing in ai_busi_cfg/unical_datasource_cfg.") % rule_id % busi_code).str(APPLOCALE);
					CALT->_CALT_busi_errors[rule_id] = str;
					rule_cfg.valid = false;
				}
				rule_cfg.flag |= RULE_FLAG_UNKNOWN_CALC_BUSI;
				continue;
			}

			cal_busi_t& busi_item = busi_iter->second;

			// 如果计算指标上有驱动源，则规则以指标上的驱动源为准
			rule_cfg.flag &= ~RULE_FLAG_NO_CALC_BUSI;

			//如果计算起始点类型为开户时间，驱动源必须为01(用户资料)
			if (calc_cfg.calc_start_type == 1) {
				if ((parms.driver_data == "01") && !(busi_item.flag & BUSI_USER_CHNL_LEVEL)){
					rule_cfg.flag &= ~(RULE_FLAG_NULL_DRIVER | RULE_FLAG_CONFLICT_DRIVER);
					rule_cfg.flag |= (RULE_FLAG_OPEN_TIME | RULE_FLAG_MATCH_DRIVER | RULE_FLAG_MATCH_ONE_DRIVER);
				} else if (parms.driver_data == "09" && (busi_item.complex_type != COMPLEX_TYPE_NONE || (busi_item.flag & BUSI_USER_CHNL_LEVEL))){
					rule_cfg.flag &= ~(RULE_FLAG_NULL_DRIVER | RULE_FLAG_CONFLICT_DRIVER);
					rule_cfg.flag |= (RULE_FLAG_OPEN_TIME | RULE_FLAG_MATCH_DRIVER | RULE_FLAG_MATCH_ONE_DRIVER);
				} else if (!rule_cfg.flag & RULE_FLAG_MATCH_ONE_DRIVER){
					rule_cfg.flag |= (RULE_FLAG_OPEN_TIME | RULE_FLAG_NULL_DRIVER | RULE_FLAG_CONFLICT_DRIVER);
					GPP->write_log(__FILE__, __LINE__, (_("ERROR: driver_data must be 01 if rule_id={1}, calc_start_type=1.") % rule_id).str(APPLOCALE));
				}
			} else {
				if (busi_item.driver_data == parms.driver_data)
					rule_cfg.flag |= (RULE_FLAG_MATCH_DRIVER | RULE_FLAG_MATCH_ONE_DRIVER);
				else if (busi_item.driver_data.empty())
					rule_cfg.flag |= (RULE_FLAG_NULL_DRIVER | RULE_FLAG_CONFLICT_DRIVER);
				else if (!busi_item.driver_data.empty())
					rule_cfg.flag |= RULE_FLAG_MATCH_ONE_DRIVER;
			}
			//计算指标是复合指标，则驱动源为09 (渠道级)
			if (busi_item.complex_type != COMPLEX_TYPE_NONE && parms.driver_data == "09")
				rule_cfg.flag |= RULE_FLAG_MATCH_DRIVER;

			if ((busi_item.flag & (BUSI_CHANNEL_LEVEL | BUSI_DYNAMIC))) {
				rule_cfg.flag |= RULE_FLAG_CHANNEL;
				// 渠道级的基础指标是可以摊分的
				if (busi_item.complex_type == COMPLEX_TYPE_NONE)
					rule_cfg.flag |= RULE_FLAG_ALLOCATABLE;
			}
			if ((busi_item.flag & (BUSI_USER_CHNL_LEVEL))) {
				rule_cfg.flag |= RULE_FLAG_CHANNEL;
			}

			if ((busi_item.flag & BUSI_ACCT_LEVEL) == BUSI_ACCT_LEVEL)
				rule_cfg.flag |= RULE_FLAG_ACCT;

			rule_cfg.sum_busi_code = busi_code;
			rule_cfg.sum_field_name = busi_item.field_name;
			rule_cfg.sum_field_data = busi_item.field_data;

			add_track_field(rule_cfg, rule_cfg.sum_field_data, 1);

			if (!busi_item.valid) {
				if (parms.stage == 3) {
					GPP->write_log(__FILE__, __LINE__, (_("ERROR: For rule_id {1} busi_code {2} in ai_rule_detail is missing in ai_busi_cfg/unical_datasource_cfg.") % rule_id % busi_code).str(APPLOCALE));
					rule_cfg.valid = false;
				}
				continue;
			}

			field_data = busi_item.field_data;
			if (field_data->field_type != SQLTYPE_SHORT
				&& field_data->field_type != SQLTYPE_INT
				&& field_data->field_type != SQLTYPE_LONG
				&& field_data->field_type != SQLTYPE_FLOAT
				&& field_data->field_type != SQLTYPE_DOUBLE) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: busi_code {1} of rule_id {2} should have a numeric type.") % busi_code % rule_id).str(APPLOCALE));
				rule_cfg.valid = false;
				continue;
			}

			// 如果计算指标是渠道级的，也需要预先计算
			if (busi_item.flag & BUSI_CHANNEL_LEVEL && !(busi_item.flag & BUSI_USER_CHNL_LEVEL))
				add_accu_cond(rule_cfg, busi_code, busi_item);
			//账户级指标累计
			if (busi_item.flag & BUSI_ACCT_LEVEL)
				add_acct_busi_cond(rule_cfg, busi_code, busi_item);

			// 渠道级指标，而且是来源于当前输入源
			if ((busi_item.flag & (BUSI_DYNAMIC | BUSI_ALLOCATABLE)) == (BUSI_DYNAMIC | BUSI_ALLOCATABLE)) {
				map<string, cal_busi_t>::iterator sub_busi_iter = busi_map.find(busi_item.sub_busi_code);
				if (sub_busi_iter == busi_map.end())
					throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: busi_code {1} of rule_id {2} has no sub_busi_code {3}") % busi_code % rule_id % busi_item.sub_busi_code).str(APPLOCALE));

				cal_busi_t& sub_busi_item = sub_busi_iter->second;
				if (!sub_busi_item.valid)
					continue;

				field_data = sub_busi_item.field_data;
				if (field_data->field_type != SQLTYPE_SHORT
					&& field_data->field_type != SQLTYPE_INT
					&& field_data->field_type != SQLTYPE_LONG
					&& field_data->field_type != SQLTYPE_FLOAT
					&& field_data->field_type != SQLTYPE_DOUBLE) {
					GPP->write_log(__FILE__, __LINE__, (_("ERROR: busi_code {1} of rule_id {2} should have a numeric type.") % busi_code % rule_id).str(APPLOCALE));
					rule_cfg.valid = false;
					continue;
				}

				rule_cfg.calc_busi_code = sub_busi_iter->first;
				rule_cfg.calc_field_name = sub_busi_item.field_name;
				rule_cfg.calc_field_data = sub_busi_item.field_data;

				add_track_field(rule_cfg, rule_cfg.calc_field_data, 1);
			} else if (!(busi_item.flag & BUSI_DYNAMIC)) {
				// 用户级的固定佣金实际上单价佣金
				if (!parms.channel_level) {
					if (calc_cfg.calc_method == CALC_METHOD_FIXED)
						calc_cfg.calc_method = CALC_METHOD_PRICE;
				}
			}

			// 根据计算类型，判断是否有缺少计算指标
			if (parms.channel_level) {
				switch (calc_cfg.calc_method) {
				case CALC_METHOD_PRICE:
				case CALC_METHOD_PERCENT:
					if (rule_cfg.sum_field_data == NULL) {
						GPP->write_log(__FILE__, __LINE__, (_("ERROR: calculation busi_code is missing for rule_id {1}") % rule_id).str(APPLOCALE));
						rule_cfg.valid = false;
					}
					break;
				}
			} else {
				switch (calc_cfg.calc_method) {
				case CALC_METHOD_PRICE:
					if ((rule_cfg.flag & RULE_FLAG_STEP) && rule_cfg.sum_field_data == NULL) {
						GPP->write_log(__FILE__, __LINE__, (_("ERROR: calculation busi_code is missing for rule_id {1}") % rule_id).str(APPLOCALE));
						rule_cfg.valid = false;
					}
					break;
				case CALC_METHOD_PERCENT:
					if (rule_cfg.sum_field_data == NULL) {
						GPP->write_log(__FILE__, __LINE__, (_("ERROR: calculation busi_code is missing for rule_id {1}") % rule_id).str(APPLOCALE));
						rule_cfg.valid = false;
					}
					break;
				case CALC_METHOD_FIXED:
					if (rule_cfg.sum_field_data == NULL || rule_cfg.calc_field_data == NULL) {
						GPP->write_log(__FILE__, __LINE__, (_("ERROR: calculation busi_code is missing or not from driver_data for rule_id {1}") % rule_id).str(APPLOCALE));
						rule_cfg.valid = false;
					}
					break;
				}
			}
		}
	} catch (bad_sql& ex) {
		throw bad_msg(__FILE__, __LINE__, 0, ex.what());
	}
}

// 加载规则表达式中用到的指标
void cal_rule::load_rule_detail_expr()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	cal_tree& tree_mgr = cal_tree::instance(CALT);
	const cal_parms_t& parms = CALP->_CALP_parms;
	map<int, cal_rule_t>& rule_map = CALT->_CALT_rule_map;

	string sql_stmt = (boost::format("SELECT a.rule_id{int}, b.busi_code{char[%1%]} "
		"FROM ai_rule_detail a, ai_rule_detail_expr b "
		"WHERE a.detail_type = 1 "
        "AND REGEXP_SUBSTR(a.busi_code, '.{4}') = 'expr' "
        "AND a.detail_id = b.detail_id "
		"AND a.rule_id IN (SELECT rule_id FROM ai_rule_area WHERE (province_code = :province_code{char[%2%]} OR province_code IS NULL)) "
		"AND rule_id IN (SELECT rule_id FROM ai_rule_calculation_cfg WHERE cycle_time_unit = ")
		% BUSI_CODE_LEN % PROVINCE_CODE_LEN).str();

	if (parms.cycle_type == CYCLE_TYPE_DAY) {
		sql_stmt += "'2'";
	} else if (CALP->_CALP_run_complex) {
		if (parms.busi_limit_month == 1)
			sql_stmt += "'0' AND busi_limit_month = '1'";
		else if (parms.busi_limit_month == 2)
			sql_stmt += "'0' AND busi_limit_month = '2'";
		else
			throw bad_param(__FILE__, __LINE__, 0, (_("ERROR: invalid value for busi_limit_month")).str(APPLOCALE));
	} else {
		sql_stmt += "'0'";
	}

	sql_stmt += ")";

	try {
		Generic_Database *db = CALT->_CALT_db;
		auto_ptr<Generic_Statement> auto_stmt(db->create_statement());
		Generic_Statement *stmt = auto_stmt.get();

		auto_ptr<struct_dynamic> auto_data(db->create_data(true));
		struct_dynamic *data = auto_data.get();
		if (data == NULL) {
			GPP->write_log(__FILE__, __LINE__, (_("ERROR: Memory allocation failure")).str(APPLOCALE));
			exit(1);
		}

		data->setSQL(sql_stmt);
		stmt->bind(data);

		stmt->setString(1, parms.province_code);

		auto_ptr<Generic_ResultSet> auto_rset(stmt->executeQuery());
		Generic_ResultSet *rset = auto_rset.get();

		map<string, cal_busi_t>::iterator busi_iter;
		field_data_t *field_data;
		map<string, cal_busi_t>& busi_map = CALT->_CALT_busi_map;

		set<string>::iterator  specify_driver_busi_iter;
		set<string>& specify_driver_busi_set = CALT->_CALT_specify_driver_busi_set;

		while (rset->next()) {
			int rule_id = rset->getInt(1);
			map<int, cal_rule_t>::iterator rule_iter = rule_map.find(rule_id);
			if (rule_iter == rule_map.end()) {
				// 如果只有规则细则，没有配置数据，则记录错误
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: rule_id {1} in ai_rule_detail is missing in ai_rule_config.") % rule_id).str(APPLOCALE));
				continue;
			}

			cal_rule_t& rule_cfg = rule_iter->second;
			if (!rule_cfg.valid)
				continue;

			cal_rule_calc_cfg_t& calc_cfg = rule_cfg.calc_cfg;
			string busi_code = rset->getString(2);

			/*
			 * 如果是用户级的数据源，计算过程中要做到有效摊分，
			 * 实际上都是对单条记录的计算， 因此，渠道级的汇总指标，
			 * 在计算指标与参考指标中稍微有些不同的含义，
			 * 渠道级的汇总，在计算时，其含义是单条记录中的量
			 * 因此，在设置计算指标的值时，需要先用加后缀的指标(渠道级汇总
			 * 指标的原始来源值)来查找，找不到则表示该指标是非渠道级汇总指
			 * 标，再用正常的指标查找
			 */
			busi_iter = busi_map.find(busi_code);
			if (busi_iter == busi_map.end()) {
				// 如果计算指标没有定义，则记录错误
				// 如果计算指标是可忽略的指标，则不记录错误，直接忽略该规则
				if (!tree_mgr.ignored(busi_code)) {
					string str = __FILE__;
					str += ":";
					str += boost::lexical_cast<string>(__LINE__);
					str += ": ";
					str += (_("ERROR: For rule_id {1} busi_code {2} in ai_rule_detail is missing in ai_busi_cfg/unical_datasource_cfg.") % rule_id % busi_code).str(APPLOCALE);
					CALT->_CALT_busi_errors[rule_id] = str;
					rule_cfg.valid = false;
				}
				rule_cfg.flag |= RULE_FLAG_UNKNOWN_CALC_BUSI;
				continue;
			}

			cal_busi_t& busi_item = busi_iter->second;

			// 如果计算指标上有驱动源，则规则以指标上的驱动源为准
			rule_cfg.flag &= ~RULE_FLAG_NO_CALC_BUSI;

			//如果计算起始点类型为开户时间，驱动源必须为01(用户资料)
			if (calc_cfg.calc_start_type == 1) {
				if ((parms.driver_data == "01") && !(busi_item.flag & BUSI_USER_CHNL_LEVEL)){
					rule_cfg.flag &= ~(RULE_FLAG_NULL_DRIVER | RULE_FLAG_CONFLICT_DRIVER);
					rule_cfg.flag |= (RULE_FLAG_OPEN_TIME | RULE_FLAG_MATCH_DRIVER | RULE_FLAG_MATCH_ONE_DRIVER);
				} else if (parms.driver_data == "09" && (busi_item.complex_type != COMPLEX_TYPE_NONE || (busi_item.flag & BUSI_USER_CHNL_LEVEL))){
					rule_cfg.flag &= ~(RULE_FLAG_NULL_DRIVER | RULE_FLAG_CONFLICT_DRIVER);
					rule_cfg.flag |= (RULE_FLAG_OPEN_TIME | RULE_FLAG_MATCH_DRIVER | RULE_FLAG_MATCH_ONE_DRIVER);
				} else if (!rule_cfg.flag & RULE_FLAG_MATCH_ONE_DRIVER){
					rule_cfg.flag |= (RULE_FLAG_OPEN_TIME | RULE_FLAG_NULL_DRIVER | RULE_FLAG_CONFLICT_DRIVER);
					GPP->write_log(__FILE__, __LINE__, (_("ERROR: driver_data must be 01 if rule_id={1}, calc_start_type=1.") % rule_id).str(APPLOCALE));
				}
			} else {
				if (busi_item.driver_data == parms.driver_data)
					rule_cfg.flag |= (RULE_FLAG_MATCH_DRIVER | RULE_FLAG_MATCH_ONE_DRIVER);
				else if (busi_item.driver_data.empty())
					rule_cfg.flag |= (RULE_FLAG_NULL_DRIVER | RULE_FLAG_CONFLICT_DRIVER);
				else if (!busi_item.driver_data.empty())
					rule_cfg.flag |= RULE_FLAG_MATCH_ONE_DRIVER;
			}
			//计算指标是复合指标，则驱动源为09 (渠道级)
			if (busi_item.complex_type != COMPLEX_TYPE_NONE && parms.driver_data == "09")
				rule_cfg.flag |= RULE_FLAG_MATCH_DRIVER;

			if ((busi_item.flag & (BUSI_CHANNEL_LEVEL | BUSI_DYNAMIC))) {
				rule_cfg.flag |= RULE_FLAG_CHANNEL;

			}
			if ((busi_item.flag & (BUSI_USER_CHNL_LEVEL))) {
				rule_cfg.flag |= RULE_FLAG_CHANNEL;
			}

			if ((busi_item.flag & BUSI_ACCT_LEVEL) == BUSI_ACCT_LEVEL)
				rule_cfg.flag |= RULE_FLAG_ACCT;
/* 已经在load_rule_detail实现
			rule_cfg.sum_busi_code = busi_code;
			rule_cfg.sum_field_name = busi_item.field_name;
			rule_cfg.sum_field_data = busi_item.field_data;
*/
			add_track_field(rule_cfg, busi_item.field_data, 1);

			specify_driver_busi_iter = specify_driver_busi_set.find(busi_code);
			//计算指标是表达式并且包含政策指定驱动源的指标
			if (calc_cfg.calc_start_type != 1 && specify_driver_busi_iter != specify_driver_busi_set.end())
				rule_cfg.flag |= RULE_FLAG_CAL_EXPR_SEPCIFY_DRIVER;

			if (!busi_item.valid) {
				if (parms.stage == 3) {
					GPP->write_log(__FILE__, __LINE__, (_("ERROR: For rule_id {1} busi_code {2} in ai_rule_detail is missing in ai_busi_cfg/unical_datasource_cfg.") % rule_id % busi_code).str(APPLOCALE));
					rule_cfg.valid = false;
				}
				continue;
			}

			field_data = busi_item.field_data;
			if (field_data->field_type != SQLTYPE_SHORT
				&& field_data->field_type != SQLTYPE_INT
				&& field_data->field_type != SQLTYPE_LONG
				&& field_data->field_type != SQLTYPE_FLOAT
				&& field_data->field_type != SQLTYPE_DOUBLE) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: busi_code {1} of rule_id {2} should have a numeric type.") % busi_code % rule_id).str(APPLOCALE));
				rule_cfg.valid = false;
				continue;
			}

			// 如果计算指标是渠道级的，也需要预先计算
			if (busi_item.flag & BUSI_CHANNEL_LEVEL && !(busi_item.flag & BUSI_USER_CHNL_LEVEL))
				add_accu_cond(rule_cfg, busi_code, busi_item);
			//账户级指标累计
			if (busi_item.flag & BUSI_ACCT_LEVEL)
				add_acct_busi_cond(rule_cfg, busi_code, busi_item);

			// 渠道级指标，而且是来源于当前输入源
			if ((busi_item.flag & (BUSI_DYNAMIC | BUSI_ALLOCATABLE)) == (BUSI_DYNAMIC | BUSI_ALLOCATABLE)) {
				map<string, cal_busi_t>::iterator sub_busi_iter = busi_map.find(busi_item.sub_busi_code);
				if (sub_busi_iter == busi_map.end())
					throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: busi_code {1} of rule_id {2} has no sub_busi_code {3}") % busi_code % rule_id % busi_item.sub_busi_code).str(APPLOCALE));

				cal_busi_t& sub_busi_item = sub_busi_iter->second;
				if (!sub_busi_item.valid)
					continue;

				field_data = sub_busi_item.field_data;
				if (field_data->field_type != SQLTYPE_SHORT
					&& field_data->field_type != SQLTYPE_INT
					&& field_data->field_type != SQLTYPE_LONG
					&& field_data->field_type != SQLTYPE_FLOAT
					&& field_data->field_type != SQLTYPE_DOUBLE) {
					GPP->write_log(__FILE__, __LINE__, (_("ERROR: busi_code {1} of rule_id {2} should have a numeric type.") % busi_code % rule_id).str(APPLOCALE));
					rule_cfg.valid = false;
					continue;
				}

				rule_cfg.calc_busi_code = sub_busi_iter->first;
				rule_cfg.calc_field_name = sub_busi_item.field_name;
				rule_cfg.calc_field_data = sub_busi_item.field_data;

				add_track_field(rule_cfg, rule_cfg.calc_field_data, 1);
			} else if (!(busi_item.flag & BUSI_DYNAMIC)) {
				// 用户级的固定佣金实际上单价佣金
				if (!parms.channel_level) {
					if (calc_cfg.calc_method == CALC_METHOD_FIXED)
						calc_cfg.calc_method = CALC_METHOD_PRICE;
				}
			}

			// 根据计算类型，判断是否有缺少计算指标
			if (parms.channel_level) {
				switch (calc_cfg.calc_method) {
				case CALC_METHOD_PRICE:
				case CALC_METHOD_PERCENT:
					if (rule_cfg.sum_field_data == NULL) {
						GPP->write_log(__FILE__, __LINE__, (_("ERROR: calculation busi_code is missing for rule_id {1}") % rule_id).str(APPLOCALE));
						rule_cfg.valid = false;
					}
					break;
				}
			} else {
				switch (calc_cfg.calc_method) {
				case CALC_METHOD_PRICE:
					if ((rule_cfg.flag & RULE_FLAG_STEP) && rule_cfg.sum_field_data == NULL) {
						GPP->write_log(__FILE__, __LINE__, (_("ERROR: calculation busi_code is missing for rule_id {1}") % rule_id).str(APPLOCALE));
						rule_cfg.valid = false;
					}
					break;
				case CALC_METHOD_PERCENT:
					if (rule_cfg.sum_field_data == NULL) {
						GPP->write_log(__FILE__, __LINE__, (_("ERROR: calculation busi_code is missing for rule_id {1}") % rule_id).str(APPLOCALE));
						rule_cfg.valid = false;
					}
					break;
				case CALC_METHOD_FIXED:
					if (rule_cfg.sum_field_data == NULL || rule_cfg.calc_field_data == NULL) {
						GPP->write_log(__FILE__, __LINE__, (_("ERROR: calculation busi_code is missing or not from driver_data for rule_id {1}") % rule_id).str(APPLOCALE));
						rule_cfg.valid = false;
					}
					break;
				}
			}
		}
	} catch (bad_sql& ex) {
		throw bad_msg(__FILE__, __LINE__, 0, ex.what());
	}
}

void cal_rule::load_rule_calc_detail()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	const cal_parms_t& parms = CALP->_CALP_parms;
	map<int, cal_rule_t>& rule_map = CALT->_CALT_rule_map;

	string sql_stmt = (boost::format("SELECT rule_id{int}, cond_para_value{char[4000]}, cal_para_value{char[4000]} "
		"FROM ai_rule_cal_busi_detail "
		"WHERE rule_id IN (SELECT rule_id FROM ai_rule_area WHERE (province_code = :province_code{char[%1%]} OR province_code IS NULL) "
		"AND rule_id IN (SELECT rule_id FROM ai_rule_calculation_cfg WHERE cycle_time_unit = ")
		% PROVINCE_CODE_LEN).str();

	if (parms.cycle_type == CYCLE_TYPE_DAY) {
		sql_stmt += "'2'";
	} else if (CALP->_CALP_run_complex) {
		if (parms.busi_limit_month == 1)
			sql_stmt += "'0' AND busi_limit_month = '1'";
		else if (parms.busi_limit_month == 2)
			sql_stmt += "'0' AND busi_limit_month = '2'";
		else
			throw bad_param(__FILE__, __LINE__, 0, (_("ERROR: invalid value for busi_limit_month")).str(APPLOCALE));
	} else {
		sql_stmt += "'0'";
	}

	sql_stmt += ")) ORDER BY rule_id, sort_id";

	try {
		Generic_Database *db = CALT->_CALT_db;
		auto_ptr<Generic_Statement> auto_stmt(db->create_statement());
		Generic_Statement *stmt = auto_stmt.get();

		auto_ptr<struct_dynamic> auto_data(db->create_data(true));
		struct_dynamic *data = auto_data.get();
		if (data == NULL) {
			GPP->write_log(__FILE__, __LINE__, (_("ERROR: Memory allocation failure")).str(APPLOCALE));
			exit(1);
		}

		data->setSQL(sql_stmt);
		stmt->bind(data);

		stmt->setString(1, parms.province_code);

		auto_ptr<Generic_ResultSet> auto_rset(stmt->executeQuery());
		Generic_ResultSet *rset = auto_rset.get();

		while (rset->next()) {
			int rule_id = rset->getInt(1);
			map<int, cal_rule_t>::iterator rule_iter = rule_map.find(rule_id);
			if (rule_iter == rule_map.end()) {
				// 如果只有规则细则，没有配置数据，则记录错误
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: rule_id {1} in ai_rule_cal_busi_detail is missing in ai_rule_config") % rule_id).str(APPLOCALE));
				continue;
			}

			cal_rule_t& rule_cfg = rule_iter->second;
			if (!rule_cfg.valid)
				continue;

			string cond_para_value = rset->getString(2);
			string cal_para_value = rset->getString(3);

			if (!parse_cond(rule_id, rule_cfg, cond_para_value)) {
				rule_cfg.valid = false;
				continue;
			}

			if (!parse_calc(rule_id, rule_cfg, cal_para_value)) {
				rule_cfg.valid = false;
				continue;
			}

			// 加入条件轨迹需要的指标
			add_track_field(rule_cfg, rule_cfg.ref_cond, 0);

			// 加入计算轨迹需要的指标
			if (parms.stage == 3)
				add_track_field(rule_cfg, rule_cfg.calc_detail[0].ref_cond, 1);
		}
	} catch (bad_sql& ex) {
		throw bad_msg(__FILE__, __LINE__, 0, ex.what());
	}
}

// 加载规则支付关系表
void cal_rule::load_rule_payment()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	const cal_parms_t& parms = CALP->_CALP_parms;
	map<int, cal_rule_t>& rule_map = CALT->_CALT_rule_map;

	string sql_stmt = (boost::format("SELECT rule_id{int}, pay_type{int}, pay_cycle_unit{int}, pay_cycle{int} "
		"FROM ai_rule_payment_cfg "
		"WHERE rule_id IN (SELECT rule_id FROM ai_rule_area WHERE (province_code = :province_code{char[%1%]} OR province_code IS NULL) "
		"AND rule_id IN (SELECT rule_id FROM ai_rule_calculation_cfg WHERE cycle_time_unit = ")
		% PROVINCE_CODE_LEN).str();

	if (parms.cycle_type == CYCLE_TYPE_DAY) {
		sql_stmt += "'2'";
	} else if (CALP->_CALP_run_complex) {
		if (parms.busi_limit_month == 1)
			sql_stmt += "'0' AND busi_limit_month = '1'";
		else if (parms.busi_limit_month == 2)
			sql_stmt += "'0' AND busi_limit_month = '2'";
		else
			throw bad_param(__FILE__, __LINE__, 0, (_("ERROR: invalid value for busi_limit_month")).str(APPLOCALE));
	} else {
		sql_stmt += "'0'";
	}

	sql_stmt += "))";

	try {
		Generic_Database *db = CALT->_CALT_db;
		auto_ptr<Generic_Statement> auto_stmt(db->create_statement());
		Generic_Statement *stmt = auto_stmt.get();

		auto_ptr<struct_dynamic> auto_data(db->create_data(true));
		struct_dynamic *data = auto_data.get();
		if (data == NULL) {
			GPP->write_log(__FILE__, __LINE__, (_("ERROR: Memory allocation failure")).str(APPLOCALE));
			exit(1);
		}

		data->setSQL(sql_stmt);
		stmt->bind(data);

		stmt->setString(1, parms.province_code);

		auto_ptr<Generic_ResultSet> auto_rset(stmt->executeQuery());
		Generic_ResultSet *rset = auto_rset.get();

		while (rset->next()) {
			int rule_id = rset->getInt(1);
			map<int, cal_rule_t>::iterator rule_iter = rule_map.find(rule_id);
			if (rule_iter == rule_map.end()) {
				// 如果只有支付配置，没有规则配置数据，则记录错误
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: rule_id {1} in ai_rule_payment_cfg is missing in ai_rule_config.") % rule_id).str(APPLOCALE));
				continue;
			}

			cal_rule_t& rule_cfg = rule_iter->second;
			if (!rule_cfg.valid)
				continue;

			cal_rule_payment_t& pay_cfg = rule_cfg.pay_cfg;

			pay_cfg.pay_type = rset->getInt(2);
			if (pay_cfg.pay_type < PAY_TYPE_MIN || pay_cfg.pay_type > PAY_TYPE_MAX) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: pay_type for rule_id {1} is not correct.") % rule_id).str(APPLOCALE));
				rule_cfg.valid = false;
				continue;
			}

			pay_cfg.pay_cycle_unit = rset->getInt(3);
			if (pay_cfg.pay_cycle_unit < PAY_CYCLE_UNIT_MIN || pay_cfg.pay_cycle_unit > PAY_CYCLE_UNIT_MAX) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: pay_cycle_unit for rule_id {1} is not correct.") % rule_id).str(APPLOCALE));
				rule_cfg.valid = false;
				continue;
			}

			pay_cfg.pay_cycle = rset->getInt(4);
		}
	} catch (bad_sql& ex) {
		throw bad_msg(__FILE__, __LINE__, 0, ex.what());
	}
}

// 加载规则支付关系表
void cal_rule::load_rule_payment_detail()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	const cal_parms_t& parms = CALP->_CALP_parms;
	map<int, cal_rule_t>& rule_map = CALT->_CALT_rule_map;

	string sql_stmt = (boost::format("SELECT rule_id{int}, pay_date{int}, pay_condition{char[4000]}, pay_val{double} "
		"FROM ai_rule_payment_detail "
		"WHERE rule_id IN (SELECT rule_id FROM ai_rule_area WHERE (province_code = :province_code{char[%1%]} OR province_code IS NULL)) "
		"AND rule_id IN (SELECT rule_id FROM ai_rule_calculation_cfg WHERE cycle_time_unit = ")
		% PROVINCE_CODE_LEN).str();

	if (parms.cycle_type == CYCLE_TYPE_DAY) {
		sql_stmt += "'2'";
	} else if (CALP->_CALP_run_complex) {
		if (parms.busi_limit_month == 1)
			sql_stmt += "'0' AND busi_limit_month = '1'";
		else if (parms.busi_limit_month == 2)
			sql_stmt += "'0' AND busi_limit_month = '2'";
		else
			throw bad_param(__FILE__, __LINE__, 0, (_("ERROR: invalid value for busi_limit_month")).str(APPLOCALE));
	} else {
		sql_stmt += "'0'";
	}

	sql_stmt += ")";

	try {
		Generic_Database *db = CALT->_CALT_db;
		auto_ptr<Generic_Statement> auto_stmt(db->create_statement());
		Generic_Statement *stmt = auto_stmt.get();

		auto_ptr<struct_dynamic> auto_data(db->create_data(true));
		struct_dynamic *data = auto_data.get();
		if (data == NULL) {
			GPP->write_log(__FILE__, __LINE__, (_("ERROR: Memory allocation failure")).str(APPLOCALE));
			exit(1);
		}

		data->setSQL(sql_stmt);
		stmt->bind(data);

		stmt->setString(1, parms.province_code);

		auto_ptr<Generic_ResultSet> auto_rset(stmt->executeQuery());
		Generic_ResultSet *rset = auto_rset.get();

		while (rset->next()) {
			cal_rule_payment_detail_t item;
			int rule_id = rset->getInt(1);
			map<int, cal_rule_t>::iterator rule_iter = rule_map.find(rule_id);
			if (rule_iter == rule_map.end()) {
				// 如果只有规则细则，没有配置数据，则记录错误
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: rule_id {1} in ai_rule_payment_cfg/ai_rule_payment_detail is missing in ai_rule_config.") % rule_id).str(APPLOCALE));
				continue;
			}

			cal_rule_t& rule_cfg = rule_iter->second;
			if (!rule_cfg.valid)
				continue;

			cal_rule_payment_t& pay_cfg = rule_cfg.pay_cfg;

			item.pay_date = rset->getInt(2) - 1;
			if (item.pay_date < 0
				|| (pay_cfg.pay_type != PAY_TYPE_COND_ONCE
				&& pay_cfg.pay_type != PAY_TYPE_COND_CONT
				&& item.pay_date >= pay_cfg.pay_cycle)) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: pay_date for rule_id {1} is not correct.") % rule_id).str(APPLOCALE));
				rule_cfg.valid = false;
				continue;
			}

			if (!parse_pay_cond(rule_id, rule_cfg, item.pay_cond, rset->getString(3))) {
				rule_cfg.valid = false;
				continue;
			}

			// 加入支付轨迹需要的指标
			add_track_field(rule_cfg, item.pay_cond, 2);

			item.pay_value = rset->getDouble(4);
			pay_cfg.pay_detail.push_back(item);
		}
	} catch (bad_sql& ex) {
		throw bad_msg(__FILE__, __LINE__, 0, ex.what());
	}
}

// 加载复合指标定义，只加载指定省分的数据
void cal_rule::load_rule_complex()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	cal_tree& tree_mgr = cal_tree::instance(CALT);
	const cal_parms_t& parms = CALP->_CALP_parms;
	map<int, cal_rule_t>& rule_map = CALT->_CALT_rule_map;
	set<cal_rule_complex_t>& rule_complex_set = CALT->_CALT_rule_complex_set;

	for (int i = 0; i < 2; i++){
		string sql_stmt;
		if (i == 0){
			sql_stmt = (boost::format("SELECT a.rule_id{int}, a.busi_code{char[%1%]}, b.attr_value{char[4000]} "
				"FROM ai_rule_detail a, ai_rule_detail_attr b, ai_busi_cfg c "
				"WHERE a.busi_code IS NOT NULL "
				"AND REGEXP_SUBSTR(a.busi_code, '.{4}') <> 'expr' "
				"AND a.busi_code = c.busi_code "
				"AND a.detail_id = b.detail_id "
				"AND c.is_mix_flag = 1 "
				"AND a.rule_id IN (SELECT rule_id FROM ai_rule_area WHERE (province_code = :province_code{char[%2%]} OR province_code IS NULL)) "
				"AND a.rule_id IN (SELECT rule_id FROM ai_rule_calculation_cfg WHERE cycle_time_unit = ")
				% BUSI_CODE_LEN % PROVINCE_CODE_LEN).str();
		}else{
			sql_stmt = (boost::format("SELECT a.rule_id{int}, d.busi_code{char[%1%]}, b.attr_value{char[4000]} "
				"FROM ai_rule_detail a, ai_rule_detail_attr b, ai_busi_cfg c, ai_rule_detail_expr d "
				"where REGEXP_SUBSTR(a.busi_code, '.{4}') = 'expr' "
				"AND d.busi_code = c.busi_code "
                  		"AND c.is_mix_flag = 1 "
		  		"AND a.detail_id = d.detail_id "
                  		"AND d.detail_attr_id = b.detail_id "
				"AND a.rule_id IN (SELECT rule_id FROM ai_rule_area WHERE (province_code = :province_code{char[%2%]} OR province_code IS NULL)) "
				"AND a.rule_id IN (SELECT rule_id FROM ai_rule_calculation_cfg WHERE cycle_time_unit = ")
				% BUSI_CODE_LEN % PROVINCE_CODE_LEN).str();
		}

		if (parms.cycle_type == CYCLE_TYPE_DAY) {
			sql_stmt += "'2'";
		} else if (CALP->_CALP_run_complex) {
			if (parms.busi_limit_month == 1)
				sql_stmt += "'0' AND busi_limit_month = '1'";
			else if (parms.busi_limit_month == 2)
				sql_stmt += "'0' AND busi_limit_month = '2'";
			else
				throw bad_param(__FILE__, __LINE__, 0, (_("ERROR: invalid value for busi_limit_month")).str(APPLOCALE));
		} else {
			sql_stmt += "'0'";
		}

		sql_stmt += ")";

		try {
			Generic_Database *db = CALT->_CALT_db;
			auto_ptr<Generic_Statement> auto_stmt(db->create_statement());
			Generic_Statement *stmt = auto_stmt.get();

			auto_ptr<struct_dynamic> auto_data(db->create_data(true));
			struct_dynamic *data = auto_data.get();
			if (data == NULL) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: Memory allocation failure")).str(APPLOCALE));
				exit(1);
			}

			data->setSQL(sql_stmt);
			stmt->bind(data);

			stmt->setString(1, parms.province_code);

			auto_ptr<Generic_ResultSet> auto_rset(stmt->executeQuery());
			Generic_ResultSet *rset = auto_rset.get();

			map<string, cal_busi_t>::iterator busi_iter;
			map<string, cal_busi_t>& busi_map = CALT->_CALT_busi_map;

			while (rset->next()) {
				cal_rule_complex_t item;
				cal_complex_busi_attr_t item2;
				item.rule_id = rset->getInt(1);
				item.busi_code = rset->getString(2);

				map<int, cal_rule_t>::iterator rule_iter = rule_map.find(item.rule_id);
				if (rule_iter == rule_map.end()) {
					// 如果只有规则细则，没有配置数据，则记录错误
					GPP->write_log(__FILE__, __LINE__, (_("ERROR: rule_id {1} in ai_rule_detail/ai_rule_detail_attr/ai_busi_datasource_attr is missing in ai_rule_config.") % item.rule_id).str(APPLOCALE));
					continue;
				}

				cal_rule_t& rule_cfg = rule_iter->second;

				map<string, cal_busi_t>::iterator busi_iter = busi_map.find(item.busi_code);
				if (busi_iter == busi_map.end()) {
					if (!tree_mgr.ignored(item.busi_code)) {
						// 如果只有指标属性，则记录错误
						GPP->write_log(__FILE__, __LINE__, (_("ERROR: busi_code {1} of rule_id {2} is missing in ai_busi_cfg.") % item.busi_code % item.rule_id).str(APPLOCALE));
						rule_cfg.valid = false;
					}
					continue;
				}

				cal_busi_t& busi_item = busi_iter->second;
				if (!(busi_item.flag & BUSI_IS_MIXED))
					continue;

				// 如果指标被重定义为非复合指标，则修改指标属性
				if (busi_item.complex_type == COMPLEX_TYPE_NONE) {
					busi_item.flag &= ~BUSI_IS_MIXED;
					continue;
				}

				string attr_value = rset->getString(3);
				if (attr_value.empty()) {
					item.units = 1;
				} else if (!parse_unit(rset->getString(3), item.units)) {
					GPP->write_log(__FILE__, __LINE__,  (_("ERROR: parse attr_value failed, complex busi_code {1} of rule_id {2}") % item.busi_code % item.rule_id).str(APPLOCALE));
					rule_cfg.valid = false;
					continue;
				}

				item.complex_type = busi_item.complex_type;
				rule_complex_set.insert(item);

				item2.busi_code = item.busi_code;
				item2.units = item.units;
				item2.complex_type = item.complex_type;
				rule_cfg.complex_attr.push_back(item2);
			}
		} catch (bad_sql& ex) {
			throw bad_msg(__FILE__, __LINE__, 0, ex.what());
		}
	}
}

// 加载渠道承诺任务绩效指标定义，只加载指定省分的数据
void cal_rule::load_rule_kpi()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	cal_tree& tree_mgr = cal_tree::instance(CALT);
	const cal_parms_t& parms = CALP->_CALP_parms;
	map<int, cal_rule_t>& rule_map = CALT->_CALT_rule_map;

	for (int i = 0; i < 2; i++){
		string sql_stmt;
		if (i == 0){
			sql_stmt = (boost::format("SELECT a.rule_id{int}, a.busi_code{char[%1%]}, b.attr_value{char[4000]}, c.is_kpi_flag{int} "
			"FROM ai_rule_detail a, ai_rule_detail_attr b, ai_busi_cfg c,  ai_rule_calculation_cfg f "
			"WHERE a.busi_code IS NOT NULL "
			"AND REGEXP_SUBSTR(a.busi_code, '.{4}') <> 'expr' "
			"AND a.busi_code = c.busi_code "
			"AND a.detail_id = b.detail_id "
			"AND c.is_kpi_flag in (2, 3) "
			"AND a.rule_id in(select e.rule_id from ai_rule_area e where (e.province_code = :province_code{char[%2%]} OR e.province_code IS NULL)) "
			"AND a.rule_id = f.rule_id "
			"AND f.cycle_time_unit = ")
			% BUSI_CODE_LEN % PROVINCE_CODE_LEN).str();
		}else{
			sql_stmt = (boost::format("SELECT a.rule_id{int}, d.busi_code{char[%1%]}, b.attr_value{char[4000]}, c.is_kpi_flag{int} "
			"FROM ai_rule_detail a, ai_rule_detail_attr b, ai_busi_cfg c, ai_rule_detail_expr d, ai_rule_calculation_cfg f "
			"WHERE REGEXP_SUBSTR(a.busi_code, '.{4}') = 'expr' "
      		"AND d.busi_code = c.busi_code "
        	"AND c.IS_KPI_FLAG in (2, 3) "
			"AND a.detail_id = d.detail_id "
        	"AND b.detail_id = d.detail_attr_id "
        	"AND a.rule_id in(select e.rule_id from ai_rule_area e where (e.province_code = :province_code{char[%2%]} OR e.province_code IS NULL)) "
			"AND a.rule_id = f.rule_id "
			"AND f.cycle_time_unit = ")
			% BUSI_CODE_LEN % PROVINCE_CODE_LEN).str();
		}

		if (parms.cycle_type == CYCLE_TYPE_DAY) {
			sql_stmt += "'2'";
		} else if (CALP->_CALP_run_complex) {
			if (parms.busi_limit_month == 1)
				sql_stmt += "'0' AND f.busi_limit_month = '1'";
			else if (parms.busi_limit_month == 2)
				sql_stmt += "'0' AND f.busi_limit_month = '2'";
			else
				throw bad_param(__FILE__, __LINE__, 0, (_("ERROR: invalid value for busi_limit_month")).str(APPLOCALE));
		} else {
			sql_stmt += "'0'";
		}


		try {
			Generic_Database *db = CALT->_CALT_db;
			auto_ptr<Generic_Statement> auto_stmt(db->create_statement());
			Generic_Statement *stmt = auto_stmt.get();

			auto_ptr<struct_dynamic> auto_data(db->create_data(true));
			struct_dynamic *data = auto_data.get();
			if (data == NULL) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: Memory allocation failure")).str(APPLOCALE));
				exit(1);
			}

			data->setSQL(sql_stmt);
			stmt->bind(data);

			stmt->setString(1, parms.province_code);

			auto_ptr<Generic_ResultSet> auto_rset(stmt->executeQuery());
			Generic_ResultSet *rset = auto_rset.get();

			map<string, cal_busi_t>::iterator busi_iter;
			map<string, cal_busi_t>& busi_map = CALT->_CALT_busi_map;
		 	int rule_id;

			while (rset->next()) {
				rule_id = rset->getInt(1);
				map<int, cal_rule_t>::iterator rule_iter = rule_map.find(rule_id);
				if (rule_iter == rule_map.end()) {
					// 如果只有规则细则，没有配置数据，则记录错误
					GPP->write_log(__FILE__, __LINE__, (_("ERROR: rule_id {1} in ai_rule_detail/ai_rule_detail_attr/ai_busi_datasource_attr is missing in ai_rule_config.") % rule_id).str(APPLOCALE));
					continue;
				}

				cal_rule_t& rule_cfg = rule_iter->second;
				string attr_value = rset->getString(3);
				if (rset->getInt(4) == 3) {
					if (!parse_unit(rset->getString(3), rule_cfg.cycles))
						rule_cfg.cycles = 1;
					if (rule_cfg.cycles == -1)
						rule_cfg.cycles = std::numeric_limits<int>::max();
					continue;
				}else{
					rule_cfg.cycles = 0;
				}

				cal_kpi_busi_attr_t item;
				item.busi_code = rset->getString(2);

				map<string, cal_busi_t>::iterator busi_iter = busi_map.find(item.busi_code);
				if (busi_iter == busi_map.end()) {
					if (!tree_mgr.ignored(item.busi_code)) {
						// 如果只有指标属性，则记录错误
						GPP->write_log(__FILE__, __LINE__, (_("ERROR: busi_code {1} of rule_id {2} is missing in ai_busi_cfg.") % item.busi_code % rule_id).str(APPLOCALE));
						rule_cfg.valid = false;
					}
					continue;
				}

				if (attr_value.empty()) {
					item.kpi_unit= 1;
				} else if (!parse_unit(rset->getString(3), item.kpi_unit)) {
					GPP->write_log(__FILE__, __LINE__,  (_("ERROR: parse attr_value failed, complex busi_code {1} of rule_id {2}") % item.busi_code % rule_id).str(APPLOCALE));
					rule_cfg.valid = false;
					continue;
				}

				rule_cfg.kpi_attr.push_back(item);

			}
		} catch (bad_sql& ex) {
			throw bad_msg(__FILE__, __LINE__, 0, ex.what());
		}
	}
}

int cal_rule::do_user_calculate(const cal_rule_t& rule_cfg)
{
	int retval = -1;
#if defined(DEBUG)
	scoped_debug<int> debug(500, __PRETTY_FUNCTION__, "", &retval);
#endif

	field_data_t **global_fields = CALT->_CALT_global_fields;
	cal_tree& tree_mgr = cal_tree::instance(CALT);
	field_data_t *field_data;
	double ref_value = 0.0;
	double sum_value;
	double& comm_fee = *reinterpret_cast<double *>(global_fields[COMM_FEE_GI]->data);
	const cal_rule_calc_cfg_t& calc_cfg = rule_cfg.calc_cfg;
	const vector<cal_rule_calc_detail_t>& calc_detail = rule_cfg.calc_detail;
	int cycle_id = get_cycle_id(calc_cfg);

	field_data_t *calc_track_field = global_fields[CALC_TRACK_GI];
	char *calc_track = calc_track_field->data;
	int& field_size = calc_track_field->field_size;
	string calc_str;
	bool first = true;
	bool matched = false;

	comm_fee = 0.0;
	for (vector<cal_rule_calc_detail_t>::const_iterator iter = calc_detail.begin(); iter != calc_detail.end(); ++iter) {
		const vector<cal_cond_para_t>& ref_cond = iter->ref_cond;
		const map<int, double>& calc_values = iter->calc_values;
		const vector<cal_rule_calc_para_t>& interval_calc_para = iter->interval_calc_para;
		vector<cal_cond_para_t>::const_iterator cond_iter;

		if (!check_cond(ref_cond))
			continue;

		double calc_value;
		bool value_find = false;
		if (rule_cfg.calc_cfg.calc_cycle == -1 || interval_calc_para.size()>0)  //长期分成
		{
			for (vector<cal_rule_calc_para_t>::const_iterator calc_iter = interval_calc_para.begin(); calc_iter != interval_calc_para.end(); ++calc_iter) {
				if (cycle_id>=calc_iter->month1 && cycle_id<=calc_iter->month2) {
					calc_value = calc_iter->value;
					value_find = true;
				}
			}
		}
		else {
			map<int, double>::const_iterator value_iter = calc_values.find(cycle_id);
			if (value_iter != calc_values.end())  {
				calc_value = value_iter->second;
				value_find = true;
				}
		}
		if (value_find) {
			dout << "calc_method = " << calc_cfg.calc_method << endl;

			switch (calc_cfg.calc_method) {
			case CALC_METHOD_PRICE:
				if (rule_cfg.flag & RULE_FLAG_STEP) {
					field_data = rule_cfg.sum_field_data;
					if (!(field_data->flag & FIELD_DATA_GOTTEN))
						tree_mgr.get_field_value(field_data);

					ref_value = field_data->get_double();
					dout << "ref_value = " << ref_value << endl;

					if (ref_value >= iter->lower_value) {
						if (!first)
							calc_str += "+";
						else
							first = false;
						calc_str += boost::lexical_cast<string>(calc_value);
						comm_fee += calc_value;
					}
				} else {
					calc_str += boost::lexical_cast<string>(calc_value);
					comm_fee = calc_value;
				}
				break;
			case CALC_METHOD_PERCENT:
				if (rule_cfg.calc_field_data != NULL)
					field_data = rule_cfg.calc_field_data;
				else
					field_data = rule_cfg.sum_field_data;
				if (!(field_data->flag & FIELD_DATA_GOTTEN))
					tree_mgr.get_field_value(field_data);

				ref_value = field_data->get_double();
				dout << "ref_value = " << ref_value << endl;

				if (rule_cfg.flag & RULE_FLAG_STEP) { // 阶梯方式
					if (ref_value > iter->lower_value) {
						if (!first)
							calc_str += "+";
						else
							first = false;

						if (ref_value <= iter->upper_value) {
							calc_str += (boost::format("(%1%-%2%)*%3%%%") % ref_value % iter->lower_value % calc_value).str();
							comm_fee += (ref_value - iter->lower_value) * calc_value / 100.0;
						} else {
							calc_str += (boost::format("(%1%-%2%)*%3%%%") % iter->upper_value % iter->lower_value % calc_value).str();
							comm_fee += (iter->upper_value - iter->lower_value) * calc_value / 100.0;
						}
					}
				} else {
					calc_str += (boost::format("%1%*%2%%%") % ref_value % calc_value).str();
					comm_fee = ref_value * calc_value / 100.0;
				}
				break;
			case CALC_METHOD_FIXED:
				field_data = rule_cfg.calc_field_data;
				if (!(field_data->flag & FIELD_DATA_GOTTEN))
					tree_mgr.get_field_value(field_data);

				ref_value = field_data->get_double();
				dout << "ref_value = " << ref_value << endl;

				field_data = rule_cfg.sum_field_data;
				if (!(field_data->flag & FIELD_DATA_GOTTEN))
					tree_mgr.get_field_value(rule_cfg.sum_field_data);

				sum_value = field_data->get_double();
				dout << "sum_value = " << sum_value << endl;

				calc_str += (boost::format("%1%*%2%/%3%") % calc_value % ref_value % sum_value).str();
				comm_fee = calc_value * ref_value / sum_value;
				break;
			}
		}

		// 对于阶梯方式，需要继续计算
		if ((rule_cfg.flag & RULE_FLAG_STEP) && ref_value > iter->upper_value)
			continue;

		matched = true;
		break;
	}

	//服务渠道结算政策计算
	double chnl_separate = *reinterpret_cast<double *>(global_fields[CHNL_SEPARATE_GI]->data);
	if (calc_cfg.settlement_to_service == 1)
		comm_fee = comm_fee * chnl_separate;
	
	// 如果预分配的内存不足，则扩展该内存
	if (calc_str.length() >= field_size) {
		delete []calc_track_field->data;
		calc_track_field->field_size = calc_str.length() + 1;
		calc_track_field->data = new char[calc_track_field->field_size];
		calc_track = calc_track_field->data;
	}

	memcpy(calc_track, calc_str.c_str(), calc_str.length() + 1);

	if (matched || (rule_cfg.flag & RULE_FLAG_STEP)) {
		dout << "comm_fee = " << comm_fee << endl;
		retval = 0;
		return retval;
	} else {
		dout << "comm_fee = " << comm_fee << endl;
		return retval;
	}
}

int cal_rule::do_channel_calculate(const cal_rule_t& rule_cfg)
{
	int retval = -1;
#if defined(DEBUG)
	scoped_debug<int> debug(500, __PRETTY_FUNCTION__, "", &retval);
#endif

	field_data_t **global_fields = CALT->_CALT_global_fields;
	cal_tree& tree_mgr = cal_tree::instance(CALT);
	field_data_t *field_data;
	double ref_value = 0.0;
	double& comm_fee = *reinterpret_cast<double *>(global_fields[COMM_FEE_GI]->data);
	const cal_rule_calc_cfg_t& calc_cfg = rule_cfg.calc_cfg;
	const vector<cal_rule_calc_detail_t>& calc_detail = rule_cfg.calc_detail;
	//int cycle_id = get_cycle_id(calc_cfg);
	int cycle_id = *reinterpret_cast<int *>(CALT->_CALT_global_fields[CYCLE_ID_GI]->data);

	field_data_t *calc_track_field = global_fields[CALC_TRACK_GI];
	char *calc_track = calc_track_field->data;
	int& field_size = calc_track_field->field_size;
	string calc_str;
	bool first = true;
	bool matched = false;

	comm_fee = 0.0;
	for (vector<cal_rule_calc_detail_t>::const_iterator iter = calc_detail.begin(); iter != calc_detail.end(); ++iter) {
		const vector<cal_cond_para_t>& ref_cond = iter->ref_cond;
		const map<int, double>& calc_values = iter->calc_values;
		const vector<cal_rule_calc_para_t>& interval_calc_para = iter->interval_calc_para;
		vector<cal_cond_para_t>::const_iterator cond_iter;

		if (!check_cond(ref_cond))
			continue;

		double calc_value;
		bool value_find = false;
		if (rule_cfg.calc_cfg.calc_cycle == -1 || interval_calc_para.size()>0)  //长期分成
		{
			for (vector<cal_rule_calc_para_t>::const_iterator calc_iter = interval_calc_para.begin(); calc_iter != interval_calc_para.end(); ++calc_iter) {
				if (cycle_id>=calc_iter->month1 && cycle_id<=calc_iter->month2) {
					calc_value = calc_iter->value;
					value_find = true;
				}
			}
		}
		else {
			map<int, double>::const_iterator value_iter = calc_values.find(cycle_id);
			if (value_iter != calc_values.end())  {
				calc_value = value_iter->second;
				value_find = true;
				}
		}
		if (value_find) {
			dout << "calc_method = " << calc_cfg.calc_method << endl;

			switch (calc_cfg.calc_method) {
			case CALC_METHOD_PRICE:
				field_data = rule_cfg.sum_field_data;
				if (!(field_data->flag & FIELD_DATA_GOTTEN))
					tree_mgr.get_field_value(rule_cfg.sum_field_data);

				ref_value = field_data->get_double();
				dout << "ref_value = " << ref_value << endl;

				if (rule_cfg.flag & RULE_FLAG_STEP) { // 阶梯方式
					if (ref_value >= iter->lower_value) {
						if (!first)
							calc_str += "+";
						else
							first = false;

						if (ref_value <= iter->upper_value) {
							calc_str += (boost::format("(%1%-%2%)*%3%") % ref_value % iter->lower_value % calc_value).str();
							comm_fee += (ref_value - iter->lower_value) * calc_value;
						} else {
							calc_str += (boost::format("(%1%-%2%)*%3%") % iter->upper_value % iter->lower_value % calc_value).str();
							comm_fee += (iter->upper_value - iter->lower_value) * calc_value;
						}
					}
				} else {
					calc_str += (boost::format("%1%*%2%") % ref_value % calc_value).str();
					comm_fee = ref_value * calc_value;
				}
				break;
			case CALC_METHOD_PERCENT:
				field_data = rule_cfg.sum_field_data;
				if (!(field_data->flag & FIELD_DATA_GOTTEN))
					tree_mgr.get_field_value(rule_cfg.sum_field_data);

				ref_value = field_data->get_double();
				dout << "ref_value = " << ref_value << endl;

				if (rule_cfg.flag & RULE_FLAG_STEP) { // 阶梯方式
					if (ref_value > iter->lower_value) {
						if (!first)
							calc_str += "+";
						else
							first = false;

						if (ref_value <= iter->upper_value) {
							calc_str += (boost::format("(%1%-%2%)*%3%%%") % ref_value % iter->lower_value % calc_value).str();
							comm_fee += (ref_value - iter->lower_value) * calc_value / 100.0;
						} else {
							calc_str += (boost::format("(%1%-%2%)*%3%%%") % iter->upper_value % iter->lower_value % calc_value).str();
							comm_fee += (iter->upper_value - iter->lower_value) * calc_value / 100.0;
						}
					}
				} else {
					calc_str += (boost::format("%1%*%2%%%") % ref_value % calc_value).str();
					comm_fee = ref_value * calc_value / 100.0;
				}
				break;
			case CALC_METHOD_FIXED:
				calc_str += boost::lexical_cast<string>(calc_value);
				comm_fee = calc_value;
				break;
			}
		}

		// 对于阶梯方式，需要继续计算
		if ((rule_cfg.flag & RULE_FLAG_STEP) && ref_value > iter->upper_value)
			continue;

		matched = true;
		break;
	}

	//服务渠道结算政策计算
	double chnl_separate = *reinterpret_cast<double *>(global_fields[CHNL_SEPARATE_GI]->data);
	if (calc_cfg.settlement_to_service == 1)
		comm_fee = comm_fee * chnl_separate;
	
	// 如果预分配的内存不足，则扩展该内存
	if (calc_str.length() >= field_size) {
		delete []calc_track_field->data;
		calc_track_field->field_size = calc_str.length() + 1;
		calc_track_field->data = new char[calc_track_field->field_size];
		calc_track = calc_track_field->data;
	}

	memcpy(calc_track, calc_str.c_str(), calc_str.length() + 1);

	if (matched || (rule_cfg.flag & RULE_FLAG_STEP)) {
		dout << "comm_fee = " << comm_fee << endl;
		retval = 0;
		return retval;
	} else {
		dout << "comm_fee = " << comm_fee << endl;
		return retval;
	}
}

bool cal_rule::parse_cond(int rule_id, cal_rule_t& rule_cfg, const string& cond_str)
{
	bool retval = false;
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, (_("rule_id={1}") % rule_id).str(APPLOCALE), &retval);
#endif

	const cal_parms_t& parms = CALP->_CALP_parms;
	cal_tree& tree_mgr = cal_tree::instance(CALT);
	string_token stoken(cond_str);
	int level = 0;
	bool busi_code_flag = false;
	bool opt_flag = false;
	bool step_flag = false;
	bool sub_opt_flag = false;
	bool value_flag = false;
	string busi_code;
	cond_opt_enum opt = COND_OPT_UNKNOWN;
	cond_sub_opt_enum sub_opt = COND_SUB_OPT_UNKNOWN;
	string value;
	vector<cal_cond_para_t>& ref_cond = rule_cfg.ref_cond;
	cal_rule_calc_detail_t calc_detail;
	map<string, cal_busi_t>& busi_map = CALT->_CALT_busi_map;
	vector<cal_cond_para_t>::iterator cond_iter;

	while (1) {
		string token = stoken.next();
		if (token.empty()) {
			GPP->write_log(__FILE__, __LINE__, (_("ERROR: For rule_id {1} failed, encountered unexcepted token '{2}'") % rule_id % cond_str).str(APPLOCALE));
			return retval;
		}

		if (token == "[") {
			if (level > 0) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: For rule_id {1} parse cond_str {2} failed, encountered unexcepted token '{3}'") % rule_id % cond_str % token).str(APPLOCALE));
				return retval;
			}
			level++;
		} else if (token == "]") {
			if (level != 1) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: For rule_id {1} parse cond_str {2} failed, encountered unexcepted token '{3}'") % rule_id % cond_str % token).str(APPLOCALE));
				return retval;
			}
			level--;

			token = stoken.next();
			if (!token.empty()) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: For rule_id {1} parse cond_str {2} failed, encountered unexcepted token '{3}'") % rule_id % cond_str % token).str(APPLOCALE));
				return retval;
			}
			break;
		} else if (token == "{") {
			if (level != 1) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: For rule_id {1} parse cond_str {2} failed, encountered unexcepted token '{3}'") % rule_id % cond_str % token).str(APPLOCALE));
				return retval;
			}
			level++;

			busi_code_flag = false;
			opt_flag = false;
			step_flag = false;
			sub_opt_flag = false;
			value_flag = false;
		} else if (token == "}") {
			if (level != 2) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: For rule_id {1} parse cond_str {2} failed, encountered unexcepted token '{3}'") % rule_id % cond_str % token).str(APPLOCALE));
				return retval;
			}
			level--;

			if (!busi_code_flag || !opt_flag || !value_flag) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: For rule_id {1} parse cond_str {2} failed, keyword busi_code, opt or value is missing.") % rule_id % cond_str).str(APPLOCALE));
				return retval;
			}

			cal_cond_para_t item;
			item.busi_code = busi_code;
			item.opt = opt;

			if (opt == COND_OPT_COI && sub_opt_flag) {
				switch (sub_opt) {
				case COND_SUB_OPT_LT_LT:
					item.opt = COND_OPT_OOI;
					break;
				case COND_SUB_OPT_LT_LE:
					item.opt = COND_OPT_OCI;
					break;
				case COND_SUB_OPT_LE_LT:
					item.opt = COND_OPT_COI;
					break;
				case COND_SUB_OPT_LE_LE:
					item.opt = COND_OPT_CCI;
					break;
				case COND_SUB_OPT_UNKNOWN:
					break;
				}
			}

			// 如果是包含或不包含，则需要在前后都加逗号，以方便比较
			if (opt == COND_OPT_INC || opt == COND_OPT_EXC) {
				string tmp = value;
				value = ",";
				for (string::const_iterator iter = tmp.begin(); iter != tmp.end(); ++iter) {
					if (isspace(*iter))
						continue;
					value += *iter;
				}
				value += ",";
			}

			bool is_interval;
			if (opt == COND_OPT_COI) {
				string::size_type pos = value.find(',');
				if (pos == string::npos) {
					GPP->write_log(__FILE__, __LINE__, (_("ERROR: For rule_id {1} format for value {2} of cond_str {3} is not correct.") % rule_id % value % cond_str).str(APPLOCALE));
					return retval;
				}
				if (*value.begin() == '[' || *value.begin() == '(')
					item.value = string(value.begin() + 1, value.begin() + pos);
				else
					item.value = string(value.begin(), value.begin() + pos);
				item.value2 = string(value.begin() + pos + 1, value.end());
				is_interval = true;
			} else {
				item.value = value;
				is_interval = false;
			}

			map<string, cal_busi_t>::iterator busi_iter = busi_map.find(busi_code);
			if (busi_iter == busi_map.end()) {
				if (!tree_mgr.ignored(busi_code)) {
					string str = __FILE__;
					str += ":";
					str += boost::lexical_cast<string>(__LINE__);
					str += ": ";
					str += (_("ERROR: For rule_id {1} busi_code {2} of cond_str {3} is not defined in unical_datasource_cfg.") % rule_id % busi_code % cond_str).str(APPLOCALE);
					CALT->_CALT_busi_errors[rule_id] = str;
					return retval;
				}
				continue;
			}

			cal_busi_t& busi_item = busi_iter->second;
			if (!busi_item.valid) {
				if (!tree_mgr.ignored(busi_code)) {
					GPP->write_log(__FILE__, __LINE__, (_("ERROR: For rule_id {1} busi_code {2} of cond_str {3} is not valid.") % rule_id % busi_code % cond_str).str(APPLOCALE));
					return retval;
				}
			}

			if (step_flag) {
				if (busi_code != rule_cfg.sum_busi_code) {
					GPP->write_log(__FILE__, __LINE__, (_("ERROR: For step rule_id {1} busi_code {2} of cond_str {3} should be equal to calculation busi_code {4}") % rule_id % busi_code % cond_str % rule_cfg.sum_busi_code).str(APPLOCALE));
					return retval;
				}

				rule_cfg.flag |= RULE_FLAG_STEP;
			}

			if (!busi_item.valid)
				continue;

			field_data_t * field_data = busi_item.field_data;

			item.field_name = busi_item.field_name;
			item.field_data = field_data;

			if ((opt == COND_OPT_INC || opt == COND_OPT_EXC)
				&& (field_data->field_type == SQLTYPE_FLOAT
				|| field_data->field_type == SQLTYPE_DOUBLE)) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: field_type {1} is not allowed for opt include/exclude, rule_id {2} busi_code {3} of cond_str {4}") % field_data->field_type % rule_id % busi_code % cond_str).str(APPLOCALE));
				return retval;
			}

			switch (field_data->field_type) {
			case SQLTYPE_CHAR:
				item.numeric_value.long_value = static_cast<long>(item.value[0]);
				if (item.value2.empty())
					item.numeric_value2.long_value = std::numeric_limits<long>::max();
				else
					item.numeric_value2.long_value = static_cast<long>(item.value2[0]);
				item.field_type = SQLTYPE_LONG;
				item.multiplicator = 1;
				break;
			case SQLTYPE_SHORT:
			case SQLTYPE_INT:
			case SQLTYPE_LONG:
			case SQLTYPE_TIME:
				item.numeric_value.long_value = atol(item.value.c_str());
				if (item.value2.empty())
					item.numeric_value2.long_value = std::numeric_limits<long>::max();
				else
					item.numeric_value2.long_value = atol(item.value2.c_str());
				item.field_type = SQLTYPE_LONG;
				item.multiplicator = 1;
				break;
			case SQLTYPE_FLOAT:
			case SQLTYPE_DOUBLE:
				if (field_data->field_type == SQLTYPE_STRING) {
					GPP->write_log(__FILE__, __LINE__, (_("ERROR: field_type string can't be converted to float/double, rule_id {1} busi_code {2} of cond_str {3}") % rule_id % busi_code % cond_str).str(APPLOCALE));
					return retval;
				}

				if (busi_item.multiplicator == -1) {
					item.numeric_value.double_value = ::atof(item.value.c_str());
					if (item.value2.empty())
						item.numeric_value2.double_value = std::numeric_limits<double>::max();
					else
						item.numeric_value2.double_value = ::atof(item.value2.c_str());
					item.field_type = SQLTYPE_DOUBLE;
				} else {
					item.numeric_value.long_value = static_cast<long>(::atof(item.value.c_str()) * busi_item.multiplicator + 0.5);
					if (item.value2.empty())
						item.numeric_value2.long_value = std::numeric_limits<long>::max();
					else
						item.numeric_value2.long_value = static_cast<long>(::atof(item.value2.c_str()) * busi_item.multiplicator + 0.5);

					item.field_type = SQLTYPE_LONG;
					item.multiplicator = busi_item.multiplicator;
				}

				break;
			case SQLTYPE_STRING:
				// 如果为空，增加一个不可能输入的最大字符，方便比较
				if (item.value2.empty())
					item.value2 = static_cast<char>(0xFF);
				item.field_type = SQLTYPE_STRING;
				break;
			}

			if (parms.stage != 3) {
				if (!(busi_item.flag & (BUSI_CHANNEL_LEVEL | BUSI_DYNAMIC)) && !(busi_item.flag & (BUSI_ACCT_LEVEL | BUSI_DYNAMIC))) {
					// 区间型指标，参考值为两者的上下限，并需在计算时再重新判断
					if (is_interval && !ref_cond.empty()) {
						for (cond_iter = ref_cond.begin(); cond_iter != ref_cond.end(); ++cond_iter) {
							if (cond_iter->busi_code == busi_code) {
								if (item.field_type == SQLTYPE_LONG) {
									if (cond_iter->numeric_value.long_value > item.numeric_value.long_value) {
										cond_iter->numeric_value.long_value = item.numeric_value.long_value;
										cond_iter->value = item.value;
									}
									if (cond_iter->numeric_value2.long_value < item.numeric_value2.long_value) {
										cond_iter->numeric_value2.long_value = item.numeric_value2.long_value;
										cond_iter->value2 = item.value2;
									}
								} else if (item.field_type == SQLTYPE_DOUBLE) {
									if (cond_iter->numeric_value.double_value > item.numeric_value.double_value) {
										cond_iter->numeric_value.double_value = item.numeric_value.double_value;
										cond_iter->value = item.value;
									}
									if (cond_iter->numeric_value2.double_value < item.numeric_value2.double_value) {
										cond_iter->numeric_value2.double_value = item.numeric_value2.double_value;
										cond_iter->value2 = item.value2;
									}
								} else {
									cond_iter->value = std::min(cond_iter->value, item.value);
									cond_iter->value2 = std::max(cond_iter->value2, item.value2);
								}
								break;
							}
						}
					} else {
						if (!(rule_cfg.flag & RULE_FLAG_INITED))
							ref_cond.push_back(item);
					}
				} else if (busi_item.flag & BUSI_CHANNEL_LEVEL && !(busi_item.flag & BUSI_USER_CHNL_LEVEL)) {
					if (!(rule_cfg.flag & RULE_FLAG_INITED))
						add_accu_cond(rule_cfg, busi_code, busi_item);
				} else if (busi_item.flag & BUSI_CHANNEL_LEVEL && strncmp(busi_code.c_str(), "~", 1) == 0) {
					if (!(rule_cfg.flag & RULE_FLAG_INITED))
						add_accu_cond(rule_cfg, busi_code, busi_item);
				} else if (busi_item.flag & BUSI_ACCT_LEVEL) {
					if (!(rule_cfg.flag & RULE_FLAG_INITED))
						add_acct_busi_cond(rule_cfg, busi_code, busi_item);
				}
			} else {
				if (is_interval || (busi_item.flag & BUSI_DYNAMIC) || (busi_item.flag & BUSI_USER_CHNL_LEVEL)) {
					if (step_flag) {
						calc_detail.lower_value = ::atof(item.value.c_str());
						if (item.value2.empty())
							calc_detail.upper_value = std::numeric_limits<double>::max();
						else
							calc_detail.upper_value = ::atof(item.value2.c_str());
					} else {
						calc_detail.ref_cond.push_back(item);
					}
				} else {
					ref_cond.push_back(item);
				}
			}
		} else if (token == "busi_code") {
			if (level != 2 || busi_code_flag) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: For rule_id {1} parse cond_str {2} failed, duplicate keyword '{3}' is defined.") % rule_id % cond_str % token).str(APPLOCALE));
				return retval;
			}

			token = stoken.next();
			if (token != ":") {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: For rule_id {1} parse cond_str {2} failed, encountered unexcepted token '{3}'") % rule_id % cond_str % token).str(APPLOCALE));
				return retval;
			}

			token = stoken.next();
			if (token.empty()) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: For rule_id {1} parse cond_str {2} failed, encountered unexcepted token '{3}'") % rule_id % cond_str % token).str(APPLOCALE));
				return retval;
			}
			busi_code = token;
			busi_code_flag = true;
		} else if (memcmp(token.c_str(), "expr", 4) == 0) {
			if (level != 2 || busi_code_flag) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: For rule_id {1} parse cond_str {2} failed, duplicate keyword '{3}' is defined.") % rule_id % cond_str % token).str(APPLOCALE));
				return retval;
			}

			string alias_name = token;

			token = stoken.next();
			if (token != ":") {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: For rule_id {1} parse cond_str {2} failed, encountered unexcepted token '{3}'") % rule_id % cond_str % token).str(APPLOCALE));
				return retval;
			}

			token = stoken.next();
			if (token.empty()) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: For rule_id {1} parse cond_str {2} failed, encountered unexcepted token '{3}'") % rule_id % cond_str % token).str(APPLOCALE));
				return retval;
			}
			busi_code_flag = true;
			if (!tree_mgr.add_rule_busi(rule_id, alias_name, token, busi_code)) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: For rule_id {1} parse cond_str {2} failed, encountered unexcepted token '{3}'") % rule_id % cond_str % token).str(APPLOCALE));
				return retval;
			}
		} else if (token == "opt") {
			if (level != 2 || opt_flag) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: For rule_id {1} parse cond_str {2} failed, duplicate keyword '{3}' is defined.") % rule_id % cond_str % token).str(APPLOCALE));
				return retval;
			}

			token = stoken.next();
			if (token != ":") {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: For rule_id {1} parse cond_str {2} failed, encountered unexcepted token '{3}'") % rule_id % cond_str % token).str(APPLOCALE));
				return retval;
			}

			token = stoken.next();
			if (token.empty()) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: For rule_id {1} parse cond_str {2} failed, encountered unexcepted token '{3}'") % rule_id % cond_str % token).str(APPLOCALE));
				return retval;
			}

			if (token == ">") {
				opt = COND_OPT_GT;
			} else if (token == ">=") {
				opt = COND_OPT_GE;
			} else if (token == "<") {
				opt = COND_OPT_LT;
			} else if (token == "<=") {
				opt = COND_OPT_LE;
			} else if (token == "==" || token == "=") {
				opt = COND_OPT_EQ;
			} else if (token == "!=") {
				opt = COND_OPT_NE;
			} else if (token == "[)") {
				opt = COND_OPT_COI;
			} else if (token == "{}") {
				step_flag = true;
				opt = COND_OPT_COI;
			} else if (token == "c") {
				opt = COND_OPT_INC;
			} else if (token == "!c") {
				opt = COND_OPT_EXC;
			} else {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: For rule_id {1} parse cond_str {2} failed, encountered unexcepted token '{3}'") % rule_id % cond_str % token).str(APPLOCALE));
				return retval;
			}
			opt_flag = true;
		} else if (token == "sub_opt") {
			if (level != 2 || sub_opt_flag) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: For rule_id {1} parse cond_str {2} failed, duplicate keyword '{3}' is defined.") % rule_id % cond_str % token).str(APPLOCALE));
				return retval;
			}

			token = stoken.next();
			if (token != ":") {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: For rule_id {1} parse cond_str {2} failed, encountered unexcepted token '{3}'") % rule_id % cond_str % token).str(APPLOCALE));
				return retval;
			}

			token = stoken.next();

			if (token == "<,<") {
				sub_opt = COND_SUB_OPT_LT_LT;
			} else if (token == "<,<=") {
				sub_opt = COND_SUB_OPT_LT_LE;
			} else if (token == "<=,<") {
				sub_opt = COND_SUB_OPT_LE_LT;
			} else if (token == "<=,<=") {
				sub_opt = COND_SUB_OPT_LE_LE;
			} else if (token == "") {
				sub_opt = COND_SUB_OPT_UNKNOWN;
			} else {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: For rule_id {1} parse cond_str {2} failed, encountered unexcepted token '{3}'") % rule_id % cond_str % token).str(APPLOCALE));
				return retval;
			}

			sub_opt_flag = true;
		} else if (token == "value") {
			if (level != 2 || value_flag) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: For rule_id {1} parse cond_str {2} failed, duplicate keyword '{3}' is defined.") % rule_id % cond_str % token).str(APPLOCALE));
				return retval;
			}

			token = stoken.next();
			if (token != ":") {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: For rule_id {1} parse cond_str {2} failed, encountered unexcepted token '{3}'") % rule_id % cond_str % token).str(APPLOCALE));
				return retval;
			}

			token = stoken.next();
			if (token.empty()) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: For rule_id {1} parse cond_str {2} failed, encountered unexcepted token '{3}'") % rule_id % cond_str % token).str(APPLOCALE));
				return retval;
			}
			value = token;
			value_flag = true;
		} else if (token == ",") {
			continue;
		} else {
			GPP->write_log(__FILE__, __LINE__, (_("ERROR: For rule_id {1} parse cond_str {2} failed, encountered unexcepted token '{3}'") % rule_id % cond_str % token).str(APPLOCALE));
			return retval;
		}
	}

	if (parms.stage == 3)
		rule_cfg.calc_detail.push_back(calc_detail);
	else
		rule_cfg.flag |= RULE_FLAG_INITED;

	retval = true;
	return retval;
}

bool cal_rule::parse_calc(int rule_id, cal_rule_t& rule_cfg, const string& calc_str)
{
	const cal_parms_t& parms = CALP->_CALP_parms;
	bool retval = false;
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, (_("rule_id={1}") % rule_id).str(APPLOCALE), &retval);
#endif

	if (parms.stage != 3 && rule_cfg.calc_cfg.calc_cycle != -1) {
		retval = true;
		return retval;
	}

	string_token stoken(calc_str);
	int level = 0;
	bool month_flag = false;
	bool value_flag = false;
	bool double_month = false;
	bool s_month_flag = false;
	bool e_month_flag = false;
	int month = -1;
	string month1,month2;
	double value = 0.0;
	vector<cal_rule_calc_detail_t>& calc_detail = rule_cfg.calc_detail;
	map<int, double>& calc_values = calc_detail.rbegin()->calc_values;
	vector<cal_rule_calc_para_t>& interval_calc_para = calc_detail.rbegin()->interval_calc_para;

	while (1) {
		string token = stoken.next();
		if (token.empty()) {
			GPP->write_log(__FILE__, __LINE__, (_("ERROR: For rule_id {1} parse calc_str {2} failed.") % rule_id % calc_str).str(APPLOCALE));
			return retval;
		}

		if (token == "[") {
			if (level > 0) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: For rule_id {1} parse calc_str {2} failed, encountered unexcepted token '{3}'") % rule_id % calc_str % token).str(APPLOCALE));
				return retval;
			}
			level++;
		} else if (token == "]") {
			if (level != 1) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: For rule_id {1} parse calc_str {2} failed, encountered unexcepted token '{3}'") % rule_id % calc_str % token).str(APPLOCALE));
				return retval;
			}
			level--;

			token = stoken.next();
			if (!token.empty()) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: For rule_id {1} parse calc_str {2} failed, encountered unexcepted token '{3}'") % rule_id % calc_str % token).str(APPLOCALE));
				return retval;
			}
			break;
		} else if (token == "{") {
			if (level != 1) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: For rule_id {1} parse calc_str {2} failed, encountered unexcepted token '{3}'") % rule_id % calc_str % token).str(APPLOCALE));
				return retval;
			}
			level++;

			month_flag = false;
			value_flag = false;
			double_month = false;
			s_month_flag = false;
			e_month_flag = false;
		} else if (token == "}") {
			if (level != 2) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: For rule_id {1} parse calc_str {2} failed, encountered unexcepted token '{3}'") % rule_id % calc_str % token).str(APPLOCALE));
				return retval;
			}
			level--;

			if (!month_flag || !value_flag) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: For rule_id {1} parse calc_str {2} failed, keyword month or value is missing.") % rule_id % calc_str).str(APPLOCALE));
				return retval;
			}

			if (parms.stage == 3) {
				if (double_month) {
					cal_rule_calc_para_t  item;
					item.month1 = atol(month1.c_str())-1;
					if (month2.empty())
						item.month2 = std::numeric_limits<long>::max();
					else
						item.month2 = atol(month2.c_str())-1;
					item.value = value;
					interval_calc_para.push_back(item);
				} else {
					calc_values[month - 1] = value;
				}
			}
		} else if (token == "month") {
			if (level != 2 || month_flag) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: For rule_id {1} parse calc_str {2} failed, duplicate keyword {3} is defined.") % rule_id % calc_str % token).str(APPLOCALE));
				return retval;
			}

			token = stoken.next();
			if (token != ":") {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: For rule_id {1} parse calc_str {2} failed, encountered unexcepted token '{3}'") % rule_id % calc_str % token).str(APPLOCALE));
				return retval;
			}

			token = stoken.next();
			if (token.empty()) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: For rule_id {1} parse calc_str {2} failed, encountered unexcepted token '{3}'") % rule_id % calc_str % token).str(APPLOCALE));
				return retval;
			}
			month = ::atoi(token.c_str());
			month_flag = true;
		} else if (token == "start_mth") {
			if (level != 2 || s_month_flag) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: For rule_id {1} parse calc_str {2} failed, duplicate keyword {3} is defined.") % rule_id % calc_str % token).str(APPLOCALE));
				return retval;
			}

			token = stoken.next();
			if (token != ":") {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: For rule_id {1} parse calc_str {2} failed, encountered unexcepted token '{3}'") % rule_id % calc_str % token).str(APPLOCALE));
				return retval;
			}

			token = stoken.next();
			if (token.empty()) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: For rule_id {1} parse calc_str {2} failed, encountered unexcepted token '{3}'") % rule_id % calc_str % token).str(APPLOCALE));
				return retval;
			}
			month1 = token;
			s_month_flag = true;
			double_month = true;
 		} else if (token == "end_mth") {
			if (level != 2 || e_month_flag) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: For rule_id {1} parse calc_str {2} failed, duplicate keyword {3} is defined.") % rule_id % calc_str % token).str(APPLOCALE));
				return retval;
			}

			token = stoken.next();
			if (token != ":") {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: For rule_id {1} parse calc_str {2} failed, encountered unexcepted token '{3}'") % rule_id % calc_str % token).str(APPLOCALE));
				return retval;
			}

			token = stoken.next();
			month2 = token;
			e_month_flag = true;
			double_month = true;
			month_flag = true;
		} else if (token == "value") {
			if (level != 2 || value_flag) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: For rule_id {1} parse calc_str {2} failed, duplicate keyword {3} is defined.") % rule_id % calc_str % token).str(APPLOCALE));
				return retval;
			}

			token = stoken.next();
			if (token != ":") {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: For rule_id {1} parse calc_str {2} failed, encountered unexcepted token '{3}'") % rule_id % calc_str % token).str(APPLOCALE));
				return retval;
			}

			token = stoken.next();
			if (token.empty()) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: For rule_id {1} parse calc_str {2} failed, encountered unexcepted token '{3}'") % rule_id % calc_str % token).str(APPLOCALE));
				return retval;
			}
			value = ::atof(token.c_str());
			value_flag = true;
		} else if (token == ",") {
			continue;
		} else {
			GPP->write_log(__FILE__, __LINE__, (_("ERROR: For rule_id {1} parse calc_str {2} failed, encountered unexcepted token '{3}'") % rule_id % calc_str % token).str(APPLOCALE));
			return retval;
		}
	}

	//长期分成，如果有结束期数，则更新calc_cycle，这样在第一步就过滤不满足计算期数的记录
	if (double_month && !month2.empty())
		rule_cfg.calc_cfg.calc_cycle = ::atoi(month2.c_str());

	retval = true;
	return retval;
}

bool cal_rule::parse_pay_cond(int rule_id, cal_rule_t& rule_cfg, vector<cal_cond_para_t>& pay_cond, const string& cond_str)
{
	bool retval = false;
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, (_("rule_id={1}") % rule_id).str(APPLOCALE), &retval);
#endif

	const cal_parms_t& parms = CALP->_CALP_parms;
	cal_tree& tree_mgr = cal_tree::instance(CALT);
	string_token stoken(cond_str);
	int level = 0;
	bool busi_code_flag = false;
	bool opt_flag = false;
	bool sub_opt_flag = false;
	bool value_flag = false;
	string busi_code;
	cond_opt_enum opt = COND_OPT_UNKNOWN;
	cond_sub_opt_enum sub_opt = COND_SUB_OPT_UNKNOWN;
	string value;
	map<string, cal_busi_t>& busi_map = CALT->_CALT_busi_map;

	while (1) {
		string token = stoken.next();
		if (token.empty()) {
			GPP->write_log(__FILE__, __LINE__, (_("ERROR: For rule_id {1} parse cond_str {2} failed, encountered unexcepted token '{3}'") % rule_id % cond_str % token).str(APPLOCALE));
			return retval;
		}

		if (token == "[") {
			if (level > 0) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: For rule_id {1} parse cond_str {2} failed, encountered unexcepted token '{3}'") % rule_id % cond_str % token).str(APPLOCALE));
				return retval;
			}
			level++;
		} else if (token == "]") {
			if (level != 1) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: For rule_id {1} parse cond_str {2} failed, encountered unexcepted token '{3}'") % rule_id % cond_str % token).str(APPLOCALE));
				return retval;
			}
			level--;

			token = stoken.next();
			if (!token.empty()) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: For rule_id {1} parse cond_str {2} failed, encountered unexcepted token '{3}'") % rule_id % cond_str % token).str(APPLOCALE));
				return retval;
			}
			break;
		} else if (token == "{") {
			if (level != 1) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: For rule_id {1} parse cond_str {2} failed, encountered unexcepted token '{3}'") % rule_id % cond_str % token).str(APPLOCALE));
				return retval;
			}
			level++;

			busi_code_flag = false;
			opt_flag = false;
			sub_opt_flag = false;
			value_flag = false;
		} else if (token == "}") {
			if (level != 2) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: For rule_id {1} parse cond_str {2} failed, encountered unexcepted token '{3}'") % rule_id % cond_str % token).str(APPLOCALE));
				return retval;
			}
			level--;

			if (!busi_code_flag || !opt_flag || !value_flag) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: For rule_id {1} parse cond_str {2} failed, keyword busi_code, opt or value is missing.") % rule_id % cond_str).str(APPLOCALE));
				return retval;
			}

			cal_cond_para_t item;
			item.busi_code = busi_code;
			item.opt = opt;

			if (opt == COND_OPT_COI && sub_opt_flag) {
				switch (sub_opt) {
				case COND_SUB_OPT_LT_LT:
					item.opt = COND_OPT_OOI;
					break;
				case COND_SUB_OPT_LT_LE:
					item.opt = COND_OPT_OCI;
					break;
				case COND_SUB_OPT_LE_LT:
					item.opt = COND_OPT_COI;
					break;
				case COND_SUB_OPT_LE_LE:
					item.opt = COND_OPT_CCI;
					break;
				case COND_SUB_OPT_UNKNOWN:
					break;
				}
			}

			// 如果是包含或不包含，则需要在前后都加逗号，以方便比较
			if (opt == COND_OPT_INC || opt == COND_OPT_EXC) {
				string tmp = value;
				value = ",";
				for (string::const_iterator iter = tmp.begin(); iter != tmp.end(); ++iter) {
					if (isspace(*iter))
						continue;
					value += *iter;
				}
				value += ",";
			}

			if (opt == COND_OPT_COI) {
				string::size_type pos = value.find(',');
				if (pos == string::npos) {
					GPP->write_log(__FILE__, __LINE__, (_("ERROR: For rule_id {1} format for value {2} of cond_str {3} is not correct.") % rule_id % value % cond_str).str(APPLOCALE));
					return retval;
				}
				if (*value.begin() == '[' || *value.begin() == '(')
					item.value = string(value.begin() + 1, value.begin() + pos);
				else
					item.value = string(value.begin(), value.begin() + pos);
				item.value2 = string(value.begin() + pos + 1, value.end());
			} else {
				item.value = value;
			}

			map<string, cal_busi_t>::iterator busi_iter = busi_map.find(busi_code);
			if (busi_iter == busi_map.end()) {
				string str = __FILE__;
				str += ":";
				str += boost::lexical_cast<string>(__LINE__);
				str += ": ";
				str += (_("ERROR: For rule_id {1} busi_code {2} of cond_str {3} is not defined in unical_datasource_cfg.") % rule_id % busi_code % cond_str).str(APPLOCALE);
				CALT->_CALT_busi_errors[rule_id] = str;

				// 指标计算中允许指标未定义
				if (parms.stage != 3)
					continue;

				return retval;
			}

			cal_busi_t& busi_item = busi_iter->second;
			if (!busi_item.valid) {
				if (!tree_mgr.ignored(busi_code)) {
					GPP->write_log(__FILE__, __LINE__, (_("ERROR: For rule_id {1} busi_code {2} of cond_str {3} is not valid.") % rule_id % busi_code % cond_str).str(APPLOCALE));
					return retval;
				}
				continue;
			}

			field_data_t * field_data = busi_item.field_data;

			item.field_name = busi_item.field_name;
			item.field_data = field_data;

			if ((opt == COND_OPT_INC || opt == COND_OPT_EXC)
				&& (field_data->field_type == SQLTYPE_FLOAT
				|| field_data->field_type == SQLTYPE_DOUBLE)) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: field_type {1} is not allowed for opt include/exclude, rule_id {2} busi_code {3} of cond_str {4}") % field_data->field_type % rule_id % busi_code % cond_str).str(APPLOCALE));
				return retval;
			}

			switch (field_data->field_type) {
			case SQLTYPE_CHAR:
				item.numeric_value.long_value = static_cast<long>(item.value[0]);
				if (item.value2.empty())
					item.numeric_value2.long_value = std::numeric_limits<long>::max();
				else
					item.numeric_value2.long_value = static_cast<long>(item.value2[0]);
				item.field_type = SQLTYPE_LONG;
				item.multiplicator = 1;
				break;
			case SQLTYPE_SHORT:
			case SQLTYPE_INT:
			case SQLTYPE_LONG:
			case SQLTYPE_TIME:
				item.numeric_value.long_value = atol(item.value.c_str());
				if (item.value2.empty())
					item.numeric_value2.long_value = std::numeric_limits<long>::max();
				else
					item.numeric_value2.long_value = atol(item.value2.c_str());
				item.field_type = SQLTYPE_LONG;
				item.multiplicator = 1;
				break;
			case SQLTYPE_FLOAT:
			case SQLTYPE_DOUBLE:
				if (field_data->field_type == SQLTYPE_STRING) {
					GPP->write_log(__FILE__, __LINE__, (_("ERROR: field_type string can't be converted to float/double, rule_id {1} busi_code {2} of cond_str {3}") % rule_id % busi_code % cond_str).str(APPLOCALE));
					return retval;
				}

				if (busi_item.multiplicator == -1) {
					item.numeric_value.double_value = ::atof(item.value.c_str());
					if (item.value2.empty())
						item.numeric_value2.double_value = std::numeric_limits<double>::max();
					else
						item.numeric_value2.double_value = ::atof(item.value2.c_str());
					item.field_type = SQLTYPE_DOUBLE;
				} else {
					item.numeric_value.long_value = static_cast<long>(::atof(item.value.c_str()) * busi_item.multiplicator + 0.5);
					if (item.value2.empty())
						item.numeric_value2.long_value = std::numeric_limits<long>::max();
					else
						item.numeric_value2.long_value = static_cast<long>(::atof(item.value2.c_str()) * busi_item.multiplicator + 0.5);

					item.field_type = SQLTYPE_LONG;
					item.multiplicator = busi_item.multiplicator;
				}

				break;
			case SQLTYPE_STRING:
				// 如果为空，增加一个不可能输入的最大字符，方便比较
				if (item.value2.empty())
					item.value2 = static_cast<char>(0xFF);
				item.field_type = SQLTYPE_STRING;
				break;
			}

			pay_cond.push_back(item);

			// 如果支付条件指标是渠道级的，也需要预先计算
			if (busi_item.flag & BUSI_CHANNEL_LEVEL)
				add_accu_cond(rule_cfg, busi_code, busi_item);
			//账户级指标累计
			if (busi_item.flag & BUSI_ACCT_LEVEL)
				add_acct_busi_cond(rule_cfg, busi_code, busi_item);
		} else if (token == "condition") {
			if (level != 2 || busi_code_flag) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: For rule_id {1} parse cond_str {2} failed, duplicate keyword {3} is defined.") % rule_id % cond_str % token).str(APPLOCALE));
				return retval;
			}

			token = stoken.next();
			if (token != ":") {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: For rule_id {1} parse cond_str {2} failed, encountered unexcepted token '{3}'") % rule_id % cond_str % token).str(APPLOCALE));
				return retval;
			}

			token = stoken.next();
			if (token.empty()) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: For rule_id {1} parse cond_str {2} failed, encountered unexcepted token '{3}'") % rule_id % cond_str % token).str(APPLOCALE));
				return retval;
			}
			busi_code = token;
			busi_code_flag = true;
		} else if (memcmp(token.c_str(), "expr", 4) == 0) {
			if (level != 2 || busi_code_flag) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: For rule_id {1} parse cond_str {2} failed, duplicate keyword {3} is defined.") % rule_id % cond_str % token).str(APPLOCALE));
				return retval;
			}

			string alias_name = token;

			token = stoken.next();
			if (token != ":") {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: For rule_id {1} parse cond_str {2} failed, encountered unexcepted token '{3}'") % rule_id % cond_str % token).str(APPLOCALE));
				return retval;
			}

			token = stoken.next();
			if (token.empty()) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: For rule_id {1} parse cond_str {2} failed, encountered unexcepted token '{3}'") % rule_id % cond_str % token).str(APPLOCALE));
				return retval;
			}
			busi_code_flag = true;
			if (!tree_mgr.add_rule_busi(rule_id, alias_name, token, busi_code)) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: For rule_id {1} parse cond_str {2} failed, encountered unexcepted token '{3}'") % rule_id % cond_str % token).str(APPLOCALE));
				return retval;
			}
		} else if (token == "opt") {
			if (level != 2 || opt_flag) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: For rule_id {1} parse cond_str {2} failed, duplicate keyword {3} is defined.") % rule_id % cond_str % token).str(APPLOCALE));
				return retval;
			}

			token = stoken.next();
			if (token != ":") {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: For rule_id {1} parse cond_str {2} failed, encountered unexcepted token '{3}'") % rule_id % cond_str % token).str(APPLOCALE));
				return retval;
			}

			token = stoken.next();
			if (token.empty()) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: For rule_id {1} parse cond_str {2} failed, encountered unexcepted token '{3}'") % rule_id % cond_str % token).str(APPLOCALE));
				return retval;
			}

			if (token == ">") {
				opt = COND_OPT_GT;
			} else if (token == ">=") {
				opt = COND_OPT_GE;
			} else if (token == "<") {
				opt = COND_OPT_LT;
			} else if (token == "<=") {
				opt = COND_OPT_LE;
			} else if (token == "==" || token == "=") {
				opt = COND_OPT_EQ;
			} else if (token == "!=") {
				opt = COND_OPT_NE;
			} else if (token == "[)") {
				opt = COND_OPT_COI;
			} else if (token == "c") {
				opt = COND_OPT_INC;
			} else if (token == "!c") {
				opt = COND_OPT_EXC;
			} else {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: For rule_id {1} parse cond_str {2} failed, encountered unexcepted token '{3}'") % rule_id % cond_str % token).str(APPLOCALE));
				return retval;
			}
			opt_flag = true;
		} else if (token == "sub_opt") {
			if (level != 2 || sub_opt_flag) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: For rule_id {1} parse cond_str {2} failed, duplicate keyword {3} is defined.") % rule_id % cond_str % token).str(APPLOCALE));
				return retval;
			}

			token = stoken.next();
			if (token != ":") {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: For rule_id {1} parse cond_str {2} failed, encountered unexcepted token '{3}'") % rule_id % cond_str % token).str(APPLOCALE));
				return retval;
			}

			token = stoken.next();

			if (token == "<,<") {
				sub_opt = COND_SUB_OPT_LT_LT;
			} else if (token == "<,<=") {
				sub_opt = COND_SUB_OPT_LT_LE;
			} else if (token == "<=,<") {
				sub_opt = COND_SUB_OPT_LE_LT;
			} else if (token == "<=,<=") {
				sub_opt = COND_SUB_OPT_LE_LE;
			} else if (token == "") {
				sub_opt = COND_SUB_OPT_UNKNOWN;
			} else {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: For rule_id {1} parse cond_str {2} failed, encountered unexcepted token '{3}'") % rule_id % cond_str % token).str(APPLOCALE));
				return retval;
			}

			sub_opt_flag = true;
		} else if (token == "value") {
			if (level != 2 || value_flag) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: For rule_id {1} parse cond_str {2} failed, duplicate keyword {3} is defined.") % rule_id % cond_str % token).str(APPLOCALE));
				return retval;
			}

			token = stoken.next();
			if (token != ":") {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: For rule_id {1} parse cond_str {2} failed, encountered unexcepted token '{3}'") % rule_id % cond_str % token).str(APPLOCALE));
				return retval;
			}

			token = stoken.next();
			if (token.empty()) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: For rule_id {1} parse cond_str {2} failed, encountered unexcepted token '{3}'") % rule_id % cond_str % token).str(APPLOCALE));
				return retval;
			}
			value = token;
			value_flag = true;
		} else if (token == ",") {
			continue;
		} else {
			GPP->write_log(__FILE__, __LINE__, (_("ERROR: For rule_id {1} parse cond_str {2} failed, encountered unexcepted token '{3}'") % rule_id % cond_str % token).str(APPLOCALE));
			return retval;
		}
	}

	retval = true;
	return retval;
}

bool cal_rule::parse_unit(const string& attr_value, int& units)
{
	bool retval = false;
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, (_("attr_value={1}") % attr_value).str(APPLOCALE), &retval);
#endif
	string_token stoken(attr_value);

	while (1) {
		string token = stoken.next();
		if (token.empty()) {
			GPP->write_log(__FILE__, __LINE__, (_("ERROR: parse attr_value {1} failed.") % attr_value).str(APPLOCALE));
			return retval;
		}

		if (token == "{" || token == "}")
			continue;

		if (token == "value") {
			token = stoken.next();
			if (token != ":") {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: parse attr_value {1} failed, encountered unexcepted token '{2}'") % attr_value % token).str(APPLOCALE));
				return retval;
			}

			token = stoken.next();
			if (token.empty()) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: parse attr_value {1} failed, encountered unexcepted token '{2}'") % attr_value % token).str(APPLOCALE));
				return retval;
			}

			units = ::atoi(token.c_str());
			retval = true;
			return retval;
		}
	}

	GPP->write_log(__FILE__, __LINE__, (_("ERROR: parse attr_value {1} failed, can't find token value and its content.") % attr_value).str(APPLOCALE));
	return retval;
}

int cal_rule::get_cycle_id(const cal_rule_calc_cfg_t& calc_cfg)
{
	int retval = -1;
#if defined(DEBUG)
	scoped_debug<int> debug(500, __PRETTY_FUNCTION__, "", &retval);
#endif

	const cal_parms_t& parms = CALP->_CALP_parms;
	field_data_t **global_fields = CALT->_CALT_global_fields;
	time_t eff_date = *reinterpret_cast<time_t *>(global_fields[EFF_DATE_GI]->data);
	datetime dt(eff_date);

        if (parms.cycle_type == CYCLE_TYPE_MONTH)
		dt.next_month(calc_cfg.calc_start_time);
	else if (parms.cycle_type == CYCLE_TYPE_DAY)
		dt.next_day(calc_cfg.calc_start_time);

	char *calc_cycle = global_fields[CALC_CYCLE_GI]->data;
	date calc_d(calc_cycle);

	int cycle_id = -1;
	switch (calc_cfg.cycle_time_unit) {
	case CYCLE_TIME_UNIT_MONTH:
		{
			int eff_year = dt.year();
			int eff_month = dt.month();

			int calc_year = calc_d.year();
			int calc_month = calc_d.month();

			int months = ((calc_year - eff_year) * 12 + (calc_month - eff_month));
			if ((months % calc_cfg.cycle_time) == 0)
				cycle_id = months / calc_cfg.cycle_time;
			break;
		}
	case CYCLE_TIME_UNIT_WEEK:
		{
			int days = calc_d.duration() / 86400 - dt.duration() / 86400;
			if ((days % (calc_cfg.cycle_time * 7)) == 0)
				cycle_id = days / (calc_cfg.cycle_time * 7);
			break;
		}
	case CYCLE_TIME_UNIT_DAY:
		{
			int days = calc_d.duration() / 86400 - dt.duration() / 86400;
			if ((days % calc_cfg.cycle_time) == 0)
				cycle_id = days / calc_cfg.cycle_time;
			break;
		}
	}

	//渠道驱动源不判断计算时间点，都会计算
	if (parms.driver_data == "09")
		cycle_id = 0;

	retval = cycle_id;
	*reinterpret_cast<int *>(CALT->_CALT_global_fields[CYCLE_ID_GI]->data) = cycle_id;
	return cycle_id;
}

void cal_rule::add_accu_cond(cal_rule_t& rule_cfg, const string& busi_code, cal_busi_t& busi_item)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, (_("busi_code={1}") % busi_code).str(APPLOCALE), NULL);
#endif
	const cal_parms_t& parms = CALP->_CALP_parms;

	// 渠道级的指标不需累计
	if (parms.channel_level)
		return;

	if (parms.stage == 1
		&& ((CALP->_CALP_busi_type != -1
		&& busi_item.complex_type < 0 && busi_item.busi_type != CALP->_CALP_busi_type)
		|| (CALP->_CALP_busi_type == -1
		&& busi_item.busi_type != 0)))		
		return;

	vector<cal_accu_cond_t>& accu_cond = rule_cfg.accu_cond;

	if (!(busi_item.flag & BUSI_DEAL_FUNCTION)) {
		//  相同的指标只加载一次
		if (!rule_cfg.has_accu_cond(busi_code)) {
			cal_accu_cond_t accu_item;
			if (rule_cfg.calc_cfg.calc_start_type == 1){ //选择开户时间，则使用01累计
				if (parms.driver_data == "01"){
					accu_item.flag = ACCU_FLAG_MATCH_DRIVER;
				}else accu_item.flag = 0;
			}else {
				if (busi_item.driver_data.empty())
					accu_item.flag = ACCU_FLAG_NULL_DRIVER;
				else if (busi_item.driver_data == parms.driver_data)
					accu_item.flag = ACCU_FLAG_MATCH_DRIVER;
				else
					accu_item.flag = 0;
			}
			accu_item.busi_code = busi_code;
			if (busi_item.enable_cycles)
				accu_item.cycles = rule_cfg.cycles;
			else
				accu_item.cycles = 1;
			accu_item.field_data = busi_item.field_data;
			accu_cond.push_back(accu_item);
		}
	} else {
		map<string, cal_busi_t>& busi_map = CALT->_CALT_busi_map;
		vector<field_data_t *>& rela_fields = busi_item.field_data->rela_fields;
		for (vector<field_data_t *>::iterator iter = rela_fields.begin(); iter != rela_fields.end(); ++iter) {
			field_data_t *field_data = *iter;

			// 没有指标的字段引用，不需要累加
			if (field_data->busi_code.empty())
				continue;

			map<string, cal_busi_t>::iterator busi_iter = busi_map.find(field_data->busi_code);
			cal_busi_t& local_busi_item = busi_iter->second;

			if (parms.stage == 1
			&& ((CALP->_CALP_busi_type != -1
			&& local_busi_item.complex_type < 0 && local_busi_item.busi_type != CALP->_CALP_busi_type)
			|| (CALP->_CALP_busi_type == -1
			&& local_busi_item.busi_type != 0)))		
			return;
				
			if (!(local_busi_item.flag & BUSI_CHANNEL_LEVEL) || (local_busi_item.flag & BUSI_USER_CHNL_LEVEL))
				continue;

			//  相同的指标只加载一次
			if (!rule_cfg.has_accu_cond(field_data->busi_code)) {
				cal_accu_cond_t accu_item;

				if (rule_cfg.calc_cfg.calc_start_type == 1){ //选择开户时间，则使用01累计
					if (parms.driver_data == "01"){
						accu_item.flag = ACCU_FLAG_MATCH_DRIVER;
					}else accu_item.flag = 0;
				}else {
					if (local_busi_item.driver_data.empty())
						accu_item.flag = ACCU_FLAG_NULL_DRIVER;
					else if (local_busi_item.driver_data == parms.driver_data)
						accu_item.flag = ACCU_FLAG_MATCH_DRIVER;
					else
						accu_item.flag = 0;
				}
				accu_item.busi_code = field_data->busi_code;
				if (local_busi_item.enable_cycles)
					accu_item.cycles = rule_cfg.cycles;
				else
					accu_item.cycles = 1;
				accu_item.field_data = field_data;
				accu_cond.push_back(accu_item);
			}
		}
	}
}

void cal_rule::add_acct_busi_cond(cal_rule_t& rule_cfg, const string& busi_code, cal_busi_t& busi_item)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, (_("busi_code={1}") % busi_code).str(APPLOCALE), NULL);
#endif
	const cal_parms_t& parms = CALP->_CALP_parms;

	// 渠道级的指标不需累计
	if (parms.channel_level)
		return;

	if (parms.stage == 1
		&& ((CALP->_CALP_busi_type != -1
		&& busi_item.complex_type < 0 && busi_item.busi_type != CALP->_CALP_busi_type)
		|| (CALP->_CALP_busi_type == -1
		&& busi_item.busi_type != 0)))		
		return;

	vector<cal_accu_cond_t>& accu_cond = rule_cfg.acct_busi_cond;

	if (!(busi_item.flag & BUSI_DEAL_FUNCTION)) {
		//  相同的指标只加载一次
		if (!rule_cfg.has_acct_busi_cond(busi_code)) {
			cal_accu_cond_t accu_item;

			if (rule_cfg.calc_cfg.calc_start_type == 1){ //选择开户时间，则使用01累计
				if (parms.driver_data == "01"){
					accu_item.flag = ACCU_FLAG_MATCH_DRIVER;
				}else accu_item.flag = 0;
			}else {
				if (busi_item.driver_data.empty())
					accu_item.flag = ACCU_FLAG_NULL_DRIVER;
				else if (busi_item.driver_data == parms.driver_data)
					accu_item.flag = ACCU_FLAG_MATCH_DRIVER;
				else
					accu_item.flag = 0;
			}
			accu_item.busi_code = busi_code;
			accu_item.field_data = busi_item.field_data;
			if (busi_item.enable_cycles)
				accu_item.cycles = rule_cfg.cycles;
			else
				accu_item.cycles = 1;
			accu_cond.push_back(accu_item);
		}
	} else {
		map<string, cal_busi_t>& busi_map = CALT->_CALT_busi_map;
		vector<field_data_t *>& rela_fields = busi_item.field_data->rela_fields;
		for (vector<field_data_t *>::iterator iter = rela_fields.begin(); iter != rela_fields.end(); ++iter) {
			field_data_t *field_data = *iter;

			// 没有指标的字段引用，不需要累加
			if (field_data->busi_code.empty())
				continue;

			map<string, cal_busi_t>::iterator busi_iter = busi_map.find(field_data->busi_code);
			cal_busi_t& local_busi_item = busi_iter->second;
			
			if (parms.stage == 1
			&& ((CALP->_CALP_busi_type != -1
			&& local_busi_item.complex_type < 0 && local_busi_item.busi_type != CALP->_CALP_busi_type)
			|| (CALP->_CALP_busi_type == -1
			&& local_busi_item.busi_type != 0)))		
			return;
			
			if (!(local_busi_item.flag & BUSI_ACCT_LEVEL))
				continue;

			//  相同的指标只加载一次
			if (!rule_cfg.has_acct_busi_cond(field_data->busi_code)) {
				cal_accu_cond_t accu_item;

				if (rule_cfg.calc_cfg.calc_start_type == 1){ //选择开户时间，则使用01累计
					if (parms.driver_data == "01"){
						accu_item.flag = ACCU_FLAG_MATCH_DRIVER;
					}else accu_item.flag = 0;
				}else {
					if (local_busi_item.driver_data.empty())
						accu_item.flag = ACCU_FLAG_NULL_DRIVER;
					else if (local_busi_item.driver_data == parms.driver_data)
						accu_item.flag = ACCU_FLAG_MATCH_DRIVER;
					else
						accu_item.flag = 0;
				}
				accu_item.busi_code = field_data->busi_code;
				accu_item.field_data = field_data;
				if (local_busi_item.enable_cycles)
					accu_item.cycles = rule_cfg.cycles;
				else
					accu_item.cycles = 1;
				accu_cond.push_back(accu_item);
			}
		}
	}
}

int cal_rule::get_pay_date(const cal_rule_payment_t& pay_cfg)
{
	int retval = -1;
#if defined(DEBUG)
	scoped_debug<int> debug(500, __PRETTY_FUNCTION__, "", &retval);
#endif

	field_data_t **global_fields = CALT->_CALT_global_fields;
	char buf[32];
	char *calc_cycle = global_fields[CALC_CYCLE_GI]->data;
	char *sett_cycle = global_fields[SETT_CYCLE_GI]->data;
	int pay_date;

	switch (pay_cfg.pay_cycle_unit) {
	case PAY_CYCLE_UNIT_MONTH:
		{
			int& calc_months = *reinterpret_cast<int *>(global_fields[CALC_MONTHS_GI]->data);

			memcpy(buf, sett_cycle, 4);
			buf[4] = '\0';
			int sett_year = ::atoi(buf);

			memcpy(buf, sett_cycle + 4, 2);
			buf[2] = '\0';
			int sett_month = ::atoi(buf);

			pay_date = sett_year * 12 + sett_month - calc_months;
			break;
		}
	case PAY_CYCLE_UNIT_WEEK:
		{
			date calc_d(calc_cycle);
			date sett_d(sett_cycle);

			pay_date = (sett_d.duration() - calc_d.duration()) / 86400 / 7;
			break;
		}
	case PAY_CYCLE_UNIT_DAY:
		{
			date calc_d(calc_cycle);
			date sett_d(sett_cycle);

			pay_date = (sett_d.duration() - calc_d.duration()) / 86400;
			break;
		}
	default:
		pay_date = -1;
		break;
	}

	retval = pay_date;
	return retval;
}

void cal_rule::add_track_field(cal_rule_t& rule_cfg, const vector<cal_cond_para_t>& conds, int idx)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, (_("idx={1}") % idx).str(APPLOCALE), NULL);
#endif

	BOOST_FOREACH(const cal_cond_para_t& cond, conds) {
		field_data_t *field_data = cond.field_data;
		bool exist_flag = false;

		BOOST_FOREACH(field_data_t *rela_field, field_data->rela_fields) {
			add_track_field(rule_cfg, rela_field, idx);
		}

		// 如果指标已经加入，则忽略
		for (int i = 0; i < idx; i++) {
			const vector<field_data_t *>& track_fields = rule_cfg.track_fields[i];

			if (std::find(track_fields.begin(), track_fields.end(), field_data) != track_fields.end()) {
				exist_flag = true;
				break;
			}
		}

		if (!exist_flag)
			rule_cfg.track_fields[idx].push_back(field_data);
	}
}

void cal_rule::add_track_field(cal_rule_t& rule_cfg, field_data_t *field_data, int idx)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, (_("idx={1}") % idx).str(APPLOCALE), NULL);
#endif

	// 如果指标已经加入，则忽略
	for (int i = 0; i < idx; i++) {
		const vector<field_data_t *>& track_fields = rule_cfg.track_fields[i];

		if (std::find(track_fields.begin(), track_fields.end(), field_data) != track_fields.end())
			return;
	}

	rule_cfg.track_fields[idx].push_back(field_data);
}

}
}

