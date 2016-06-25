#if !defined(__CAL_RULE_H__)
#define __CAL_RULE_H__

#include "sa_internal.h"
#include "cal_struct.h"
#include "calp_ctx.h"
#include "calt_ctx.h"

namespace ai
{
namespace app
{

using namespace std;
using namespace ai::sg;
using namespace ai::scci;

int const CALC_UNIT_MIN = 0;
int const CALC_UNIT_MONTH = 0;
int const CALC_UNIT_WEEK = 1;
int const CALC_UNIT_DAY = 2;
int const CALC_UNIT_MAX = 2;

int const CYCLE_TIME_UNIT_MIN = 0;
int const CYCLE_TIME_UNIT_MONTH = 0;
int const CYCLE_TIME_UNIT_WEEK = 1;
int const CYCLE_TIME_UNIT_DAY = 2;
int const CYCLE_TIME_UNIT_MAX = 2;

/*
 * 对于单价佣金，需要有效摊分
 * 对于比例佣金，也需要有效摊分
 * 对于定额佣金，要看计算指标是否配置，来决定是否摊分
 * 如果配置了计算指标，则需要摊分，并且要看计算指标的类型
 * 1. 用户级计算指标，则不需摊分，跟单价佣金计算方法一致
 * 2. 渠道级计算指标，则需要摊分
 * 3. 入锅没有配置计算指标，则不需要摊分
 */
int const CALC_METHOD_MIN = 1;
int const CALC_METHOD_PRICE = 1; 	// 单价佣金 (一户给多少佣金)
int const CALC_METHOD_PERCENT = 2;	// 比例佣金 (费用的百分比)
int const CALC_METHOD_FIXED = 3;	// 定额佣金 (总额固定佣金)
int const CALC_METHOD_MAX = 3;

int const PAY_TYPE_MIN = 1;
int const PAY_TYPE_ONCE = 1;	// 一次支付
int const PAY_TYPE_PERCENT = 2;	// 分期支付(百分比) ------ 分期支付，且每次支付条件可以不同
int const PAY_TYPE_ABSOLUTE = 3;	// 分期支付(绝对值)  ------ 分期支付，且每次支付条件可以不同
int const PAY_TYPE_COND_ONCE = 4;	// 条件达到一次性支付
int const PAY_TYPE_COND_CONT = 5;	// 连续条件达到一次性支付
int const PAY_TYPE_MAX = 5;

int const PAY_CYCLE_UNIT_MIN = 0;
int const PAY_CYCLE_UNIT_MONTH = 0;
int const PAY_CYCLE_UNIT_WEEK = 1;
int const PAY_CYCLE_UNIT_DAY = 2;
int const PAY_CYCLE_UNIT_MAX = 2;

int const PAY_RETURN_NORMAL = 0;
int const PAY_RETURN_FREEZE = 1;
int const PAY_RETURN_NORMAL_FREEZE = 2;

struct cal_rule_area_t
{
	int rule_id;
	string city_code;

	bool operator<(const cal_rule_area_t& rhs) const;
};

struct cal_rule_calc_cfg_t
{
	int calc_start_time_unit;	// 计算起始点单位
	int calc_start_time;
	int cycle_time_unit;	// 计算间隔周期单位
	int cycle_time;		// 奖惩类佣金使用(单位为月份) - 多久算一次
	int cycle_type;		// 0:等比例，1:非等比例
	int calc_method;	// 计算方法
	int calc_cycle;		// 计算次数
	int calc_start_type; // 计算起始点类型:0受理时间 1开户时间
	int calc_symbol;     //代办业务是否查重:0不去重 1去重
	int settlement_to_service; //是否结算给服务渠道:1是0否
	int as_operating_costs; //是否作为网络运营成本:1是0否
	double share_ratio; //服务渠道分成比例
};

enum cond_opt_enum
{
	COND_OPT_UNKNOWN,
	COND_OPT_GT,	// 大于
	COND_OPT_GE,	// 大于等于
	COND_OPT_LT,	// 小于
	COND_OPT_LE,	// 小于等于
	COND_OPT_EQ,	// 等于
	COND_OPT_NE,	// 不等于
	COND_OPT_OOI,	// 前开后开区间
	COND_OPT_OCI,	// 前开后闭区间
	COND_OPT_COI,	// 前闭后开区间
	COND_OPT_CCI,	// 前闭后闭区间
	COND_OPT_INC,	// 包含
	COND_OPT_EXC	// 不包含
};

enum cond_sub_opt_enum
{
	COND_SUB_OPT_UNKNOWN,
	COND_SUB_OPT_LT_LT,
	COND_SUB_OPT_LT_LE,
	COND_SUB_OPT_LE_LT,
	COND_SUB_OPT_LE_LE
};

struct cal_cond_para_t
{
	string busi_code;
	cond_opt_enum opt;
	string value;
	string value2;
	string field_name;
	field_data_t *field_data;
	int field_type;
	int multiplicator;
	union {
		long long_value;
		double double_value;
	} numeric_value;
	union {
		long long_value;
		double double_value;
	} numeric_value2;
};

int const ACCU_FLAG_MATCH_DRIVER = 0x1;	// 当前指标与驱动源一致
int const ACCU_FLAG_NULL_DRIVER = 0x2;	// 当前没有指定驱动源

struct cal_accu_cond_t
{
	int flag;
	string busi_code;
	int cycles;
	field_data_t *field_data;
};

struct cal_rule_payment_detail_t
{
	int pay_date;
	vector<cal_cond_para_t> pay_cond;	// 支付条件
	double pay_value;	// 支付金额
};

struct cal_rule_calc_para_t
{
	long month1;
	long month2;
	double value;
};
struct cal_rule_calc_detail_t
{
	// 阶梯计算的下限
	double lower_value;
	// 阶梯计算的上限
	double upper_value;
	// 累计指标条件
	vector<cal_cond_para_t> ref_cond;
	// 计算规则，月份与佣金的映射表
	map<int, double> calc_values;
	vector<cal_rule_calc_para_t> interval_calc_para;    //长期分成方案，多个月份区间

	bool operator<(const cal_rule_calc_detail_t& rhs) const;
};

struct cal_rule_payment_t
{
	int pay_type;
	int pay_cycle_unit;
	int pay_cycle;
	vector<cal_rule_payment_detail_t> pay_detail;
};

struct cal_complex_busi_attr_t
{
	string busi_code;
	int units;
	int complex_type;
};

struct cal_kpi_busi_attr_t
{
	string busi_code;
	int kpi_unit;
};

int const RULE_FLAG_STEP = 0x1;						// 阶梯费率
int const RULE_FLAG_MATCH_DRIVER = 0x8;			// 当前规则与驱动源一致
int const RULE_FLAG_NULL_DRIVER = 0x10;			// 当前没有指定驱动源
int const RULE_FLAG_NO_CALC_BUSI = 0x20;			// 没有指定计算指标
int const RULE_FLAG_UNKNOWN_CALC_BUSI = 0x40;	// 不认识的计算指标
int const RULE_FLAG_CONFLICT_DRIVER = 0x80;		// 驱动源冲突
int const RULE_FLAG_ALLOCATABLE = 0x100;			// 可以对当前驱动源进行有效摊分
int const RULE_FLAG_CHANNEL = 0x200;				// 渠道级指标
int const RULE_FLAG_INITED = 0x400;					// 是否已经初始化
int const RULE_FLAG_ACCT = 0x800;					// 账户级计算指标，要输出账户id
int const RULE_FLAG_OPEN_TIME = 0x1000;			// 计算起始点类型为开户时间
int const RULE_FLAG_MATCH_ONE_DRIVER = 0x2000;		// 表达式中至少有一个指标匹配驱动源
int const RULE_FLAG_CAL_EXPR_SEPCIFY_DRIVER = 0x4000;		// 计算指标是表达式,并且包含政策指定驱动源的指标


struct cal_rule_t
{
	// 该规则计算时是否为阶梯费率
	int flag;
	// 规则生效时间
	time_t eff_date;
	// 规则失效时间
	time_t exp_date;
	// 指标作用的范围
	int cycles;
	// 计算时间限制条件
	cal_rule_calc_cfg_t calc_cfg;
	// 在第一阶段，设置为空
	// 在第三阶段，汇总的源指标
	string calc_busi_code;
	string calc_field_name;
	field_data_t *calc_field_data;
	// 在第一阶段，为汇总的源指标
	// 在第三阶段，为汇总指标
	string sum_busi_code;
	string sum_field_name;
	field_data_t *sum_field_data;
	// 参考的指标累计条件
	vector<cal_cond_para_t> ref_cond;
	// 累计指标条件
	vector<cal_accu_cond_t> accu_cond;
	vector<cal_accu_cond_t> acct_busi_cond;    //账户级指标条件
	// 计算规则
	vector<cal_rule_calc_detail_t> calc_detail;
	// 支付规则
	cal_rule_payment_t pay_cfg;
	bool valid;
        // 引用复合指标的属性
	vector<cal_complex_busi_attr_t> complex_attr;
	//渠道承诺绩效指标的属性
	vector<cal_kpi_busi_attr_t> kpi_attr;
	// 需要在轨迹上体现的指标数组，去除了重复指标
	vector<field_data_t *> track_fields[3];

	cal_rule_t();
	~cal_rule_t();

	bool has_accu_cond(const string& busi_code);
	bool has_acct_busi_cond(const string& busi_code);
};

struct cal_rule_complex_t
{
	int rule_id;
	string busi_code;
	int units;
	int complex_type;

	bool operator<(const cal_rule_complex_t& rhs) const;
};

class string_token
{
public:
	string_token(const string& source_);
	~string_token();

	string next();

private:
	const string& source;
	string::const_iterator iter;
};

class cal_rule : public cal_manager
{
public:
	static cal_rule& instance(calt_ctx *CALT);

	void load();
	int do_calculate(const cal_rule_t& rule_cfg);
	int do_pay(const cal_rule_t& rule_cfg);
	bool check_rule(int rule_id, const cal_rule_t& rule_cfg);
	bool check_cond(const vector<cal_cond_para_t>& conds);
	cal_rule_t& get_rule(int rule_id);

private:
	cal_rule();
	virtual ~cal_rule();

	void load_rule_area();
	void load_rule();
	void load_rule_detail();
	void load_rule_detail_expr();
	void load_rule_calc_detail();
	void load_rule_payment();
	void load_rule_payment_detail();
	void load_rule_complex();
	void load_rule_kpi();
	int do_user_calculate(const cal_rule_t& rule_cfg);
	int do_channel_calculate(const cal_rule_t& rule_cfg);
	bool parse_cond(int rule_id, cal_rule_t& rule_cfg, const string& cond_str);
	bool parse_calc(int rule_id, cal_rule_t& rule_cfg, const string& calc_str);
	bool parse_pay_cond(int rule_id, cal_rule_t& rule_cfg, vector<cal_cond_para_t>& pay_cond, const string& cond_str);
	bool parse_unit(const string& attr_value, int& units);
	int get_cycle_id(const cal_rule_calc_cfg_t& calc_cfg);
	void add_accu_cond(cal_rule_t& rule_cfg, const string& busi_code, cal_busi_t& busi_item);
	void add_acct_busi_cond(cal_rule_t& rule_cfg, const string& busi_code, cal_busi_t& busi_item);
	int get_pay_date(const cal_rule_payment_t& pay_cfg);
	void add_track_field(cal_rule_t& rule_cfg, const vector<cal_cond_para_t>& conds, int idx);
	void add_track_field(cal_rule_t& rule_cfg, field_data_t *field_data, int idx);

	friend class calt_ctx;
};

}
}

#endif

