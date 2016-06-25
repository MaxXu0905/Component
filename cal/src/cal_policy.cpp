#include "cal_policy.h"
#include "cal_tree.h"

namespace ai
{
namespace app
{

using namespace std;
using namespace ai::sg;
using namespace ai::scci;

bool cal_policy_area_t::operator<(const cal_policy_area_t& rhs) const
{
	if (mod_id < rhs.mod_id)
		return true;
	else if (mod_id > rhs.mod_id)
		return false;

	return city_code < rhs.city_code;
}

cal_policy_detail_t::cal_policy_detail_t()
{
	field_value = NULL;
	field_data = NULL;
	next = false;
}

cal_policy_detail_t::cal_policy_detail_t(const cal_policy_detail_t& rhs)
{
	comm_range_id = rhs.comm_range_id;
	range_type = rhs.range_type;
	comm_range_type = rhs.comm_range_type;
	comm_range_value = rhs.comm_range_value;
	pay_chnl_id  =  rhs.pay_chnl_id;
	field_count = rhs.field_count;
	field_name = rhs.field_name;
	field_data = rhs.field_data;
	field_value = new char[field_data->field_size * field_count];
	memcpy(field_value, rhs.field_value, field_data->field_size * field_count);
	next = rhs.next;
}

cal_policy_detail_t::~cal_policy_detail_t()
{
	delete []field_value;
}

cal_policy_t::cal_policy_t()
{
	flag = 0;
	valid = true;
}

cal_policy_t::~cal_policy_t()
{
}

cal_policy& cal_policy::instance(calt_ctx *CALT)
{
	return *reinterpret_cast<cal_policy *>(CALT->_CALT_mgr_array[CAL_POLICY_MANAGER]);
}

void cal_policy::load()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	load_policy_area();
	load_policy();
	load_policy_detail();
	load_rule();
}

cal_policy::const_iterator cal_policy::begin() const
{
	const map<int, cal_policy_t>& policy_map = CALT->_CALT_policy_map;
	return policy_map.begin();
}

cal_policy::const_iterator cal_policy::end() const
{
	const map<int, cal_policy_t>& policy_map = CALT->_CALT_policy_map;
	return policy_map.end();
}

bool cal_policy::match(int mod_id, const cal_policy_t& policy)
{
	const set<cal_policy_area_t>& policy_area_set = CALT->_CALT_policy_area_set;
	const cal_parms_t& parms = CALP->_CALP_parms;
	cal_tree& tree_mgr = cal_tree::instance(CALT);
	field_data_t **global_fields = CALT->_CALT_global_fields;

	bool retval = false;
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, (_("mid_id={1}") % mod_id).str(APPLOCALE), &retval);
#endif

	/*if (policy.policy_state != "10")
		return retval;*/

	// 检查当前政策的生失效时间是否正确
	time_t eff_date = *reinterpret_cast<time_t *>(global_fields[EFF_DATE_GI]->data);

	//渠道资料驱动源不判断
	if (parms.driver_data != "09") {
		if (eff_date < policy.eff_date || eff_date >= policy.exp_date) {
			dout << "mod_id = " << CALT->_CALT_mod_id << " expired, eff_date = " << eff_date
				<< ", policy.eff_date = " << policy.eff_date
				<< ", policy.exp_date = " << policy.exp_date << endl;
if(CALP->_CALP_inv_out) {			
			CALT->_CALT_policy_error_no = EPOLICY_EXPIRED;
			CALT->_CALT_policy_error_string = (_("mod_id={1} expired, eff_date={1} is not in [{2}, {3})") % eff_date % policy.eff_date % policy.exp_date).str(APPLOCALE);
}
			return retval;
		}
	}

	// 检查当前政策是否适用于当前地市
	cal_policy_area_t key;
	key.mod_id = mod_id;
	key.city_code = "";
	set<cal_policy_area_t>::const_iterator area_iter = policy_area_set.find(key);
	if (area_iter == policy_area_set.end()) {
		key.city_code = global_fields[CITY_CODE_GI]->data;
		area_iter = policy_area_set.find(key);
		if (area_iter == policy_area_set.end()) {
			dout << "mod_id = " << CALT->_CALT_mod_id << " is not in ai_mod_area, city_code = " << key.city_code << endl;
if(CALP->_CALP_inv_out) {
			CALT->_CALT_policy_error_no = ERULE_NOT_AREA;
			CALT->_CALT_policy_error_string = (_("city_code={1}") % key.city_code).str(APPLOCALE);
}
			return retval;
		}
	}

	const vector<cal_policy_detail_t>& details = policy.details;
	string last_busi_code="";
	strcpy(global_fields[ACCU_CHNL_ID_GI]->data, global_fields[CHANNEL_ID_GI]->data);
	for (vector<cal_policy_detail_t>::const_iterator iter = details.begin(); iter != details.end(); ++iter) {
		const char *field_value = iter->field_value;
		field_data_t *field_data = iter->field_data;
		if (field_data->busi_code == last_busi_code)   //同一个指标拆分为多行条件时，关系为或，有一个满足即可
			continue;

		if (!(field_data->flag & FIELD_DATA_GOTTEN)) // 还没有加载
			tree_mgr.get_field_value(field_data);

		if ((field_data->flag & (FIELD_DATA_SET_DEFAULT | FIELD_DATA_USE_DEFAULT)) == FIELD_DATA_SET_DEFAULT) {
			dout << "default data is set, not matched." << endl;
if(CALP->_CALP_inv_out) {
			CALT->_CALT_policy_error_no = EPOLICY_NO_DATA;
			ostringstream fmt;
			fmt << *field_data;
			CALT->_CALT_policy_error_string = fmt.str();
}
			return retval;
		}

		bool found = false;
		const char *data = field_data->data;

		for (int i = 0; i < field_data->data_size; i++) {
			switch (field_data->field_type) {
			case SQLTYPE_CHAR:
				found = search(*reinterpret_cast<const char *>(data), field_value, iter->field_count);
				break;
			case SQLTYPE_SHORT:
				found = search(*reinterpret_cast<const short *>(data), field_value, iter->field_count);
				break;
			case SQLTYPE_INT:
				found = search(*reinterpret_cast<const int *>(data), field_value, iter->field_count);
				break;
			case SQLTYPE_LONG:
				found = search(*reinterpret_cast<const long *>(data), field_value, iter->field_count);
				break;
			case SQLTYPE_FLOAT:
				found = search(*reinterpret_cast<const float *>(data), field_value, iter->field_count);
				break;
			case SQLTYPE_DOUBLE:
				found = search(*reinterpret_cast<const double *>(data), field_value, iter->field_count);
				break;
			case SQLTYPE_STRING:
				found = search(data, field_value, iter->field_count, field_data->field_size);
				break;
			case SQLTYPE_TIME:
				found = search(*reinterpret_cast<const time_t *>(data), field_value, iter->field_count);
				break;
			}

			if (found)
				break;

			data += field_data->field_size;
		}

		if ((found && iter->range_type != RANGE_TYPE_INCLUDE)
			||( !found && iter->range_type == RANGE_TYPE_INCLUDE)) {
			if (iter->range_type == RANGE_TYPE_INCLUDE && iter->next)
				continue;  //还有相同的指标，继续判断

			dout << "mod_id = " << CALT->_CALT_mod_id << " does not match, busi_code(" << *field_data << ")" << endl;
if(CALP->_CALP_inv_out) {
			CALT->_CALT_policy_error_no = EPOLICY_NOT_MATCH;
			ostringstream fmt;
			fmt << *field_data;
			CALT->_CALT_policy_error_string = fmt.str();
}
			return retval;
		}

		//包含关系时，保存已经匹配成功的指标编码
		if (iter->range_type == RANGE_TYPE_INCLUDE) {
			last_busi_code = field_data->busi_code ;
			if (!(iter->pay_chnl_id.empty()) )   { //包含关系匹配上就赋值结算渠道
				strcpy(global_fields[POLICY_CHNL_ID_GI]->data, iter->pay_chnl_id.c_str());
                                //是管理本部条件时才赋值
                                if (iter->comm_range_id=="08" && (iter->comm_range_type=="5" || iter->comm_range_type=="6")
                                        && (policy.access_flag == "2" || policy.access_flag == "3")) {  //汇总到管理本部
					strcpy(global_fields[ACCU_CHNL_ID_GI]->data, data);
				}
			}else {
                                if (iter->comm_range_id=="08" && (iter->comm_range_type=="5" || iter->comm_range_type=="6")
                                        && (policy.access_flag == "2" || policy.access_flag == "3")) {  //汇总到管理本部
					strcpy(global_fields[ACCU_CHNL_ID_GI]->data, data);
					strcpy(global_fields[POLICY_CHNL_ID_GI]->data, data);
				}
			}
		}else {
			if (!(iter->pay_chnl_id.empty()) ) {
					field_data_t *super_chnl = global_fields[SUPER_CHNL_GI];
					if (!(super_chnl->flag & FIELD_DATA_GOTTEN))
						tree_mgr.get_field_value(super_chnl);
					if (super_chnl->data_size>0)
						::qsort(super_chnl->data, super_chnl->data_size, super_chnl->field_size, cal_policy::compare);
					bool pay_chnl_found=search(iter->pay_chnl_id.c_str(), super_chnl->data, super_chnl->data_size, super_chnl->field_size);
					if (pay_chnl_found) {
						last_busi_code = field_data->busi_code ;
				      strcpy(global_fields[POLICY_CHNL_ID_GI]->data, iter->pay_chnl_id.c_str());
				}
					else if (!iter->next) {
						dout << "mod_id = " << CALT->_CALT_mod_id << " does not match, busi_code(" << *field_data << ")" << endl;
if(CALP->_CALP_inv_out) {
						CALT->_CALT_policy_error_no = EPOLICY_NOT_MATCH;
						ostringstream fmt;
						fmt << *field_data;
						CALT->_CALT_policy_error_string = fmt.str();
}
						return retval;
					}
				}
		}
	}

	dout << "matched, mod_id = " << CALT->_CALT_mod_id << endl;
	retval = true;
	return retval;
}

cal_policy_t& cal_policy::get_policy(int mod_id)
{
	map<int, cal_policy_t>& policy_map = CALT->_CALT_policy_map;
	map<int, cal_policy_t>::iterator iter = policy_map.find(mod_id);
	if (iter == policy_map.end())
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Can't find mod_id {1}") % mod_id).str(APPLOCALE));

	return iter->second;
}

cal_policy::cal_policy()
{
}

cal_policy::~cal_policy()
{
}

// 加载政策地域适配表，只加载指定省分的数据
void cal_policy::load_policy_area()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	const cal_parms_t& parms = CALP->_CALP_parms;
	set<cal_policy_area_t>& policy_area_set = CALT->_CALT_policy_area_set;
	policy_area_set.clear();

	string sql_stmt = (boost::format("SELECT mod_id{int}, city_code{char[%1%]} FROM ai_mod_area "
		"WHERE (province_code = :province_code{char[31]} OR province_code IS NULL) "
		"AND mod_id in (SELECT mod_id FROM v_ai_mod_config WHERE cal_start_time_unit = ")
		% CITY_CODE_LEN).str();

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

	if (parms.stage == 1)
		sql_stmt += " AND state = '10'";

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
			cal_policy_area_t item;
			item.mod_id = rset->getInt(1);
			item.city_code = rset->getString(2);
			policy_area_set.insert(item);
		}
	} catch (bad_sql& ex) {
		throw bad_msg(__FILE__, __LINE__, 0, ex.what());
	}
}

// 加载政策配置表，只加载指定省分的数据
void cal_policy::load_policy()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	const cal_parms_t& parms = CALP->_CALP_parms;
	const set<int>& env_mod_ids = CALP->_CALP_env_mod_ids;
	map<int, cal_policy_t>& policy_map = CALT->_CALT_policy_map;
	policy_map.clear();

	string sql_stmt = (boost::format("SELECT a.mod_id{int}, a.mod_type{char[4000]}, a.comm_type{char[%1%]}, "
		"a.dept_id{char[%2%]}, b.driver_data{char[%3%]}, a.eff_date{datetime}, "
		"nvl(a.exp_date, to_date('20371231235959', 'YYYYMMDDHH24MISS')){datetime}, a.state{char[4000]}, "
		"b.access_flag{char[2]}, b.is_limit_flag{char[2]},b.busi_type{char[2]} "
		"FROM v_ai_mod_config a, ai_mod_ext_info b "
		"WHERE a.mod_id = b.mod_id and a.is_model = '1' "
		"AND a.mod_id IN (SELECT mod_id FROM ai_mod_area WHERE province_code = :province_code{char[%4%]} OR province_code IS NULL) "
		"AND a.cal_start_time_unit = ")
		% COMM_TYPE_LEN % DEPT_ID_LEN % DRIVER_DATA_LEN % PROVINCE_CODE_LEN).str();

	if (parms.cycle_type == CYCLE_TYPE_DAY) {
		sql_stmt += "'2'";
	} else if (CALP->_CALP_run_complex) {
		if (parms.busi_limit_month == 1)
			sql_stmt += "'0' AND a.busi_limit_month = '1'";
		else if (parms.busi_limit_month == 2)
			sql_stmt += "'0' AND a.busi_limit_month = '2'";
		else
			throw bad_param(__FILE__, __LINE__, 0, (_("ERROR: invalid value for busi_limit_month")).str(APPLOCALE));
	} else {
		sql_stmt += "'0'";
	}

	if (parms.stage == 1)
		sql_stmt += " AND a.state = '10'";

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
			cal_policy_t item;
			int mod_id = rset->getInt(1);
			if (!env_mod_ids.empty() && env_mod_ids.find(mod_id) == env_mod_ids.end())
				continue;

			item.mod_type = rset->getString(2);
			item.comm_type = rset->getString(3);
			item.dept_id = rset->getString(4);
			string driver_data = rset->getString(5);
			if (driver_data == parms.driver_data)
				item.flag |= POLICY_FLAG_MATCH_DRIVER;
			else if (driver_data.empty())
				item.flag |= POLICY_FLAG_NULL_DRIVER;

			item.eff_date = static_cast<time_t>(rset->getLong(6));
			item.exp_date = static_cast<time_t>(rset->getLong(7));
			item.policy_state = rset->getString(8);
			item.access_flag = rset->getString(9);
			item.is_limit_flag = rset->getString(10);
			item.busi_type = rset->getString(11);

			policy_map[mod_id] = item;
		}
	} catch (bad_sql& ex) {
		throw bad_msg(__FILE__, __LINE__, 0, ex.what());
	}
}

// 加载政策过滤条件，只加载指定省分的数据
void cal_policy::load_policy_detail()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	// 第三步不需加载政策明细，因为计算时不会用这些明细
	if (CALP->_CALP_parms.stage == 3)
		return;

	const cal_parms_t& parms = CALP->_CALP_parms;
	string sql_stmt = (boost::format("SELECT a.mod_id{int}, b.comm_range_id{char[4000]}, b.range_type{char[4000]}, "
		"b.comm_range_type{char[4000]}, b.comm_range_value{char[4000]}, b.pay_chnl_id{char[127]} "
		"FROM ai_mod_item a, v_ai_mod_item_detail b "
		"WHERE a.item_id = b.item_id AND b.range_type in (0, 1) "
		"AND mod_id IN (SELECT mod_id FROM ai_mod_area WHERE (province_code = :province_code{char[%1%]} OR province_code IS NULL)) "
		"AND mod_id IN (SELECT mod_id FROM v_ai_mod_config WHERE (province_code = :province_code{char[%2%]} OR province_code ='09') and cal_start_time_unit = ")
		% PROVINCE_CODE_LEN % PROVINCE_CODE_LEN).str();

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

	if (parms.stage == 1)
		sql_stmt += " AND state = '10'";

	sql_stmt += ") ORDER BY a.mod_id, b.comm_range_id, b.comm_range_type,b.pay_chnl_id";

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
		stmt->setString(2, parms.province_code);

		auto_ptr<Generic_ResultSet> auto_rset(stmt->executeQuery());
		Generic_ResultSet *rset = auto_rset.get();

		int mod_id = -1;
		int cur_mod_id;
		bool first = true;
		cal_policy_detail_t item;
		cal_policy_detail_t cur_item;
		vector<string> value_vector;

		while (rset->next()) {
			cur_mod_id = rset->getInt(1);
			cur_item.comm_range_id = rset->getString(2);
			cur_item.range_type = rset->getInt(3);
			cur_item.comm_range_type = rset->getString(4);
			cur_item.pay_chnl_id = rset->getString(6);

			if (first) {
				mod_id = cur_mod_id;
				item.comm_range_id = cur_item.comm_range_id;
				item.range_type = cur_item.range_type;
				item.comm_range_type = cur_item.comm_range_type;
				item.pay_chnl_id = cur_item.pay_chnl_id;
				item.next = false;
				value_vector.push_back(rset->getString(5));
				first = false;
			} else if (mod_id != cur_mod_id
				|| item.comm_range_id != cur_item.comm_range_id
				|| item.range_type != cur_item.range_type
				|| item.comm_range_type != cur_item.comm_range_type
				|| item.pay_chnl_id != cur_item.pay_chnl_id) {
				//表示政策条件为同一类型，判断时认为是或关系
				if (mod_id == cur_mod_id
					&& item.comm_range_id == cur_item.comm_range_id
					&& item.range_type == cur_item.range_type
					&& item.comm_range_type == cur_item.comm_range_type)
					item.next = true;

				add_item(mod_id, item, value_vector);

				mod_id = cur_mod_id;
				item.comm_range_id = cur_item.comm_range_id;
				item.range_type = cur_item.range_type;
				item.comm_range_type = cur_item.comm_range_type;
				item.pay_chnl_id = cur_item.pay_chnl_id;
				item.next = false;
				value_vector.clear();
				value_vector.push_back(rset->getString(5));
			} else {
				value_vector.push_back(rset->getString(5));
			}
		}

		if (!value_vector.empty())
			add_item(mod_id, item, value_vector);

		// 该内存不需要自动释放
		item.field_value = NULL;
		cur_item.field_value = NULL;
	} catch (bad_sql& ex) {
		throw bad_msg(__FILE__, __LINE__, 0, ex.what());
	}
}

// 加载政策相关的规则，只加载指定省分的数据
void cal_policy::load_rule()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	const cal_parms_t& parms = CALP->_CALP_parms;
	map<int, cal_policy_t>& policy_map = CALT->_CALT_policy_map;

	string sql_stmt = "SELECT /*+RULE*/a.mod_id{int}, b.rule_id{int} "
		"FROM ai_mod_item a, ai_mod_item_rule_relation b "
		"WHERE a.item_id = b.item_id "
		"AND a.mod_id IN (SELECT mod_id FROM ai_mod_area WHERE (province_code = :province_code{char[31]} OR province_code IS NULL)) "
		"AND mod_id IN (SELECT mod_id FROM v_ai_mod_config WHERE  cal_start_time_unit = ";

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

	if (parms.stage == 1)
		sql_stmt += " AND state = '10'";

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
			int mod_id = rset->getInt(1);
			map<int, cal_policy_t>::iterator policy_iter = policy_map.find(mod_id);
			if (policy_iter == policy_map.end()) {
				// 如果只有政策细则，没有配置数据，则记录错误
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: mod_id {1} is defined in ai_mod_item_rule_relation, but missing in v_ai_mod_config/ai_mod_ext_info.") % mod_id).str(APPLOCALE));
				continue;
			}

			int rule_id = rset->getInt(2);
			policy_iter->second.rules.push_back(rule_id);
		}
	} catch (bad_sql& ex) {
		throw bad_msg(__FILE__, __LINE__, 0, ex.what());
	}
}

void cal_policy::add_item(int mod_id, cal_policy_detail_t& item, const vector<string>& value_vector)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, (_("mod_id={1}") % mod_id).str(APPLOCALE), NULL);
#endif

	cal_tree& tree_mgr = cal_tree::instance(CALT);
	field_data_map_t& data_map = CALT->_CALT_data_map;
	map<string, cal_busi_t>& busi_map = CALT->_CALT_busi_map;
	map<int, cal_policy_t>& policy_map = CALT->_CALT_policy_map;

	map<int, cal_policy_t>::iterator policy_iter = policy_map.find(mod_id);
	if (policy_iter == policy_map.end()) {
		// 如果只有政策细则，没有配置数据，则记录错误
		GPP->write_log(__FILE__, __LINE__, (_("ERROR: mod_id {1} is defined in v_ai_mod_item_detail, but missing in v_ai_mod_config/ai_mod_ext_info.") % mod_id).str(APPLOCALE));
		return;
	}

	cal_policy_t& policy = policy_iter->second;
	if (!policy.valid)
		return;

	// 根据comm_range_id + comm_range_type生成指标名称
	string busi_code = string("P") + item.comm_range_id;
	if (!item.comm_range_type.empty()) {
		busi_code += "_";
		busi_code += item.comm_range_type;
	}

	map<string, cal_busi_t>::iterator busi_iter = busi_map.find(busi_code);
	if (busi_iter == busi_map.end()) {
		// 如果找不到细则相关的指标，则该政策不能计算
		if (!tree_mgr.ignored(busi_code)) {
			GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't find busi_code {1} in unical_datasource_cfg, mod_id={2}, comm_range_id={3}, comm_range_type={4}")
				% busi_code % mod_id % item.comm_range_id % item.comm_range_type).str(APPLOCALE));
			policy.valid = false;
		}

		return;
	}

	cal_busi_t& busi_item = busi_iter->second;
	if (!busi_item.valid) {
		// 如果找不到细则相关的指标，则该政策不能计算
		if (!tree_mgr.ignored(busi_code)) {
			GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't find busi_code {1} in unical_datasource_cfg, mod_id={2}, comm_range_id={3}, comm_range_type={4}")
				% busi_code % mod_id % item.comm_range_id % item.comm_range_type).str(APPLOCALE));
			policy.valid = false;
		}

		return;
	}

	int i;
	item.field_count = value_vector.size();
	item.field_name = busi_item.field_name;
	field_data_map_t::iterator data_iter = data_map.find(item.field_name);
	if (data_iter == data_map.end()) {
		// 如果细则相关的指标没有定义，则该政策不能计算
		GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't definition for field_name {1}, mod_id={2}, busi_code={3}")
			% item.field_name % mod_id % busi_code).str(APPLOCALE));
		policy.valid = false;
		return;
	}

	item.field_data = data_iter->second;
	field_data_t *field_data = item.field_data;
	switch (field_data->field_type) {
	case SQLTYPE_CHAR:
		item.field_value = new char[item.field_count];
		for (i = 0; i < item.field_count; i++) {
			if (value_vector[i].size() != 1) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: It's too long for comm_range_value {1}, mod_id={2}, busi_code={3}, expect one character.") % value_vector[i] % mod_id % field_data->busi_code).str(APPLOCALE));
				policy.valid = false;
				return;
			}
			item.field_value[i] = value_vector[i][0];
		}
#if defined(GPLUSPLUS32)
		::qsort(item.field_value, item.field_count, sizeof(char), cal_policy::template compare<char>);
#else
	#if defined(HPUX)
		::qsort(item.field_value, item.field_count, sizeof(char), reinterpret_cast<int (*)(const void *,const void *) >(cal_policy:: compare<char>));
	#else
		::qsort(item.field_value, item.field_count, sizeof(char), cal_policy:: compare<char>);
	#endif
#endif
		break;
	case SQLTYPE_SHORT:
		item.field_value = reinterpret_cast<char *>(new char[item.field_count]);
		for (i = 0; i < item.field_count; i++) {
			if (!is_integer(value_vector[i])) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: It's not an integer for comm_range_value {1}, mod_id={2}, busi_code={3}") % value_vector[i] % mod_id % field_data->busi_code).str(APPLOCALE));
				policy.valid = false;
				return;
			}
			reinterpret_cast<short *>(item.field_value)[i] = static_cast<short>(atoi(value_vector[i].c_str()));
		}
#if defined(GPLUSPLUS32)
		::qsort(item.field_value, item.field_count, sizeof(short), cal_policy::template compare<short>);
#else
	#if defined(HPUX)
		::qsort(item.field_value, item.field_count, sizeof(short), reinterpret_cast<int (*)(const void *,const void *) >(cal_policy:: compare<short>));
	#else
		::qsort(item.field_value, item.field_count, sizeof(short), cal_policy:: compare<short>);
	#endif
#endif
		break;
	case SQLTYPE_INT:
		item.field_value = reinterpret_cast<char *>(new int[item.field_count]);
		for (i = 0; i < item.field_count; i++) {
			if (!is_integer(value_vector[i])) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: It's not an integer for comm_range_value {1}, mod_id={2}, busi_code={3}") % value_vector[i] % mod_id % field_data->busi_code).str(APPLOCALE));
				policy.valid = false;
				return;
			}
			reinterpret_cast<int *>(item.field_value)[i] = static_cast<int>(atoi(value_vector[i].c_str()));
		}
#if defined(GPLUSPLUS32)
		::qsort(item.field_value, item.field_count, sizeof(int), cal_policy::template compare<int>);
#else
	#if defined(HPUX)
		::qsort(item.field_value, item.field_count, sizeof(int), reinterpret_cast<int (*)(const void *,const void *) >(cal_policy:: compare<int>));
	#else
		::qsort(item.field_value, item.field_count, sizeof(int), cal_policy:: compare<int>);
	#endif
#endif
		break;
	case SQLTYPE_LONG:
		item.field_value = reinterpret_cast<char *>(new long[item.field_count]);
		for (i = 0; i < item.field_count; i++) {
			if (!is_integer(value_vector[i])) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: It's not an integer for comm_range_value {1}, mod_id={2}, busi_code={3}") % value_vector[i] % mod_id % field_data->busi_code).str(APPLOCALE));
				policy.valid = false;
				return;
			}
			reinterpret_cast<long *>(item.field_value)[i] = static_cast<long>(atol(value_vector[i].c_str()));
		}
#if defined(GPLUSPLUS32)
		::qsort(item.field_value, item.field_count, sizeof(long), cal_policy::template compare<long>);
#else
	#if defined(HPUX)
		::qsort(item.field_value, item.field_count, sizeof(long), reinterpret_cast<int (*)(const void *,const void *) >(cal_policy:: compare<long>));
	#else
		::qsort(item.field_value, item.field_count, sizeof(long), cal_policy:: compare<long>);
	#endif
#endif
		break;
	case SQLTYPE_FLOAT:
		item.field_value = reinterpret_cast<char *>(new float[item.field_count]);
		for (i = 0; i < item.field_count; i++) {
			if (!is_floating(value_vector[i])) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: It's not a float for comm_range_value {1}, mod_id={2}, busi_code={3}") % value_vector[i] % mod_id % field_data->busi_code).str(APPLOCALE));
				policy.valid = false;
				return;
			}
			reinterpret_cast<float *>(item.field_value)[i] = static_cast<float>(atof(value_vector[i].c_str()));
		}
#if defined(GPLUSPLUS32)
		::qsort(item.field_value, item.field_count, sizeof(float), cal_policy::template compare<float>);
#else
	#if defined(HPUX)
		::qsort(item.field_value, item.field_count, sizeof(float), reinterpret_cast<int (*)(const void *,const void *) >(cal_policy:: compare<float>));
	#else
		::qsort(item.field_value, item.field_count, sizeof(float), cal_policy:: compare<float>);
	#endif
#endif
		break;
	case SQLTYPE_DOUBLE:
		item.field_value = reinterpret_cast<char *>(new double[item.field_count]);
		for (i = 0; i < item.field_count; i++) {
			if (!is_floating(value_vector[i])) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: It's not a float for comm_range_value {1}, mod_id={2}, busi_code={3}") % value_vector[i] % mod_id % field_data->busi_code).str(APPLOCALE));
				policy.valid = false;
				return;
			}
			reinterpret_cast<double *>(item.field_value)[i] = static_cast<double>(atof(value_vector[i].c_str()));
		}
#if defined(GPLUSPLUS32)
		::qsort(item.field_value, item.field_count, sizeof(double), cal_policy::template compare<double>);
#else
	#if defined(HPUX)
		::qsort(item.field_value, item.field_count, sizeof(double), reinterpret_cast<int (*)(const void *,const void *) >(cal_policy:: compare<double>));
	#else
		::qsort(item.field_value, item.field_count, sizeof(double), cal_policy:: compare<double>);
	#endif
#endif
		break;
	case SQLTYPE_STRING:
		item.field_value = new char[item.field_count * field_data->field_size];
		for (i = 0; i < item.field_count; i++) {
			if (value_vector[i].length() >= field_data->field_size) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: It's too long for comm_range_value {1}, mod_id={2}, busi_code={3}, expect maximum {4}") % value_vector[i] % mod_id % field_data->busi_code % (field_data->field_size - 1)).str(APPLOCALE));
				policy.valid = false;
				return;
			}
			memcpy(item.field_value + i * field_data->field_size, value_vector[i].c_str(), value_vector[i].length() + 1);
		}
		::qsort(item.field_value, item.field_count, field_data->field_size, cal_policy::compare);
		break;
	case SQLTYPE_TIME:
		item.field_value = reinterpret_cast<char *>(new time_t[item.field_count]);
		for (i = 0; i < item.field_count; i++) {
			if (!is_integer(value_vector[i])) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: It's not an integer for comm_range_value {1}, mod_id={2}, busi_code={3}") % value_vector[i] % mod_id % field_data->busi_code).str(APPLOCALE));
				policy.valid = false;
				return;
			}
			reinterpret_cast<time_t *>(item.field_value)[i] = static_cast<time_t>(atol(value_vector[i].c_str()));
		}
#if defined(GPLUSPLUS32)
		::qsort(item.field_value, item.field_count, sizeof(time_t), cal_policy::template compare<time_t>);
#else
	#if defined(HPUX)
		::qsort(item.field_value, item.field_count, sizeof(time_t), reinterpret_cast<int (*)(const void *,const void *) >(cal_policy:: compare<time_t>));
	#else
		::qsort(item.field_value, item.field_count, sizeof(time_t), cal_policy:: compare<time_t>);
	#endif
#endif
		break;
	}

	policy.details.push_back(item);
	if (!item.pay_chnl_id.empty())
		policy.pay_chnl_id_set.insert(item.pay_chnl_id);
}

bool cal_policy::is_integer(const string& str)
{
	bool retval = false;
#if defined(DEBUG)
	scoped_debug<bool> debug(100, __PRETTY_FUNCTION__, (_("str={1}") % str).str(APPLOCALE), &retval);
#endif

	if (str.empty())
		return retval;

	BOOST_FOREACH(const char& ch, str) {
		if (!::isdigit(ch))
			return retval;
	}

	retval = true;
	return retval;
}

bool cal_policy::is_floating(const string& str)
{
	bool retval = false;
#if defined(DEBUG)
	scoped_debug<bool> debug(100, __PRETTY_FUNCTION__, (_("str={1}") % str).str(APPLOCALE), &retval);
#endif

	if (str.empty())
		return retval;

	BOOST_FOREACH(const char& ch, str) {
		if (!::isdigit(ch) && ch != '.')
			return retval;
	}

	retval = true;
	return retval;
}

}
}

