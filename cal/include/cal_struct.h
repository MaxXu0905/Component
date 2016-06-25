#if !defined(__CAL_STRUCT_H__)
#define __CAL_STRUCT_H__

#include "sa_internal.h"
#include "dstream.h"

namespace ai
{
namespace app
{

using namespace std;
using namespace ai::sg;
using namespace ai::scci;

int const DEAL_TYPE_MIN = 1;
int const DEAL_TYPE_DIRECT = 1;		// 直接取值(包括从输入或表取值)
int const DEAL_TYPE_SUM = 2;			// 按条件累加
int const DEAL_TYPE_COUNT = 3;			// 按条件累计
int const DEAL_TYPE_ATTR = 4;			// 取用户属性值
int const DEAL_TYPE_BOOL = 5;			// 是否存在记录
int const DEAL_TYPE_MAX = 5;
int const DEAL_TYPE_FUNCTION = DEAL_TYPE_MAX + 1;
int const DEAL_TYPE_FILTER = DEAL_TYPE_MAX + 2;

int const SUBJECT_REL_TYPE_MIN = 0;
int const SUBJECT_REL_TYPE_EXCLUDE = 0;
int const SUBJECT_REL_TYPE_INCLUDE = 1;
int const SUBJECT_REL_TYPE_ALL = 2;
int const SUBJECT_REL_TYPE_MAX = 2;

int const COMPLEX_TYPE_MIN = -1;
int const COMPLEX_TYPE_SPECIAL= -1;
int const COMPLEX_TYPE_NONE = 0;		// 不是复合指标
int const COMPLEX_TYPE_GROWTH = 1;	// 区间增量
int const COMPLEX_TYPE_SUM = 2;		// 区间累计
int const COMPLEX_TYPE_SGR = 3;		// 环比增长率
int const COMPLEX_TYPE_GR = 4;		// 有效增长率
int const COMPLEX_TYPE_AVG = 5;		// 平均值
int const COMPLEX_TYPE_MAX = 5;
int const COMPLEX_TYPE_FUNC = COMPLEX_TYPE_MAX + 1;

int const KPI_DATE_TYPE_MONTH = 1;	// 月
int const KPI_DATE_TYPE_QUARTER = 3;	// 季度
int const KPI_DATE_TYPE_HALF = 6;	// 半年
int const KPI_DATE_TYPE_YEAR = 12;		// 年

int const INIT_MAX_RESULT_RECORDS = 256;

const char SVC_TYPE_01[] = "01";      // 业务类型2G
const char SVC_TYPE_02[] = "02";      // 业务类型3G
const char SVC_TYPE_06[] = "03";     //业务类型 4G

struct global_busi_t
{
	const char *field_name;
	int field_index;
	int field_type;
	int field_size;
	int flag;
};

int const BUSI_FLAG_FROM_SOURCE = 0x1;
int const BUSI_FLAG_FROM_TABLE = 0x2;
int const BUSI_FLAG_RESET = 0x4;
int const BUSI_FLAG_NO_CREATE = 0x8;
int const BUSI_FLAG_NULL_CREATE = 0x10;
int const BUSI_FLAG_NULL_OK = 0x20;

// 全局指标定义
const char CHANNEL_ID[] = "channel_id";
const char CITY_CODE[] = "city_code";
const char DEAL_DATE[] = "deal_date";
const char SUBJECT_ID[] = "subject_id";
const char EFF_DATE[] = "eff_date";
const char MOD_ID[] = "mod_id";
const char RULE_ID[] = "rule_id";
const char COMM_FEE[] = "comm_fee";
const char COMM_TRACK[] = "comm_track";
const char BUSI_CODE[] = "busi_code";
const char BUSI_VALUE[] = "busi_value";
const char ACCT_BUSI_VALUE[] = "acct_busi_value";
const char FEE[] = "fee";
const char CALC_CYCLE[] = "calc_cycle";
const char SETT_CYCLE[] = "sett_cycle";
const char DEPT_ID[] = "dept_id";
const char COMM_TYPE[] = "comm_type";
const char FREEZE_EXPIRE[] = "freeze_expire";
const char CALC_MONTHS[] = "calc_months";
const char PAY_CHNL_ID[] = "pay_chnl_id";
const char POLICY_CHNL_ID[] = "policy_chnl_id";
const char CALC_TRACK[] = "calc_track";
const char INV_BUSI[] = "inv_busi#";
const char SUPER_CHNL[]="super_chnl";
const char ACCT_NO[]="acct_no";
const char RECORD_LEVEL[] = "record_level";
const char REAL_PAYCHNL[] = "real_paychnl";
const char ARRE_CYCLE[] = "arre_cycle";
const char CALC_TYPE[] = "calc_type";
const char SETT_FEE[] = "sett_fee";
const char EXPIRE_FEE[] = "expire_fee";
const char VAT_PRICE[] = "vat_price";
const char VAT_TAX[] = "vat_tax";
const char TAXPAYER_TYPE[] = "taxpayer_type";
const char KPI_DATE_TYPE[] = "kpi_date_type";
const char ACCU_CHNL_ID[] = "accu_chnl_id";
const char ACCESS_FLAG[] = "access_flag";
const char IS_LIMIT_FLAG[] = "is_limit_flag";
const char CYCLE_ID[] = "cycle_id";
const char SERV_CHNL_ID[] = "serv_chnl_id";
const char CHNL_SEPARATE[] = "chnl_separate";
const char SERV_FLAG[] = "serv_flag";
const char RELA_CHNL[] = "rela_chnl";
const char COST_FLAG[] = "cost_flag";
const char SVC_TYPE[] = "svc_type";
const char SUBS_STATUS[] = "subs_status";
const char UPDATE_TIME[] = "update_time";
const char PRODUCT_TYPE[] = "product_type";
const char PAY_COMM_FLAG[] = "pay_comm_flag";
const char MOD_TYPE[] = "mod_type";



int const CHANNEL_ID_GI = 0;
int const CITY_CODE_GI = 1;
int const DEAL_DATE_GI = 2;
int const SUBJECT_ID_GI = 3;
int const EFF_DATE_GI = 4;
int const MOD_ID_GI = 5;
int const RULE_ID_GI = 6;
int const COMM_FEE_GI = 7;
int const COMM_TRACK_GI = 8;
int const BUSI_CODE_GI = 9;
int const BUSI_VALUE_GI = 10;
int const FEE_GI = 11;
int const CALC_CYCLE_GI = 12;
int const SETT_CYCLE_GI = 13;
int const DEPT_ID_GI = 14;
int const COMM_TYPE_GI = 15;
int const FREEZE_EXPIRE_GI = 16;
int const CALC_MONTHS_GI = 17;
int const PAY_CHNL_ID_GI = 18;
int const POLICY_CHNL_ID_GI = 19;
int const CALC_TRACK_GI = 20;
int const INV_BUSI_GI = 21;
int const SUPER_CHNL_GI = 22;
int const ACCT_NO_GI = 23;
int const RECORD_LEVEL_GI = 24;
int const ACCT_BUSI_VALUE_GI = 25;
int const ARRE_CYCLE_GI = 26;
int const REAL_PAYCHNL_GI = 27;
int const CALC_TYPE_GI = 28;
int const SETT_FEE_GI = 29;
int const EXPIRE_FEE_GI = 30;
int const VAT_PRICE_GI = 31;
int const VAT_TAX_GI = 32;
int const TAXPAYER_TYPE_GI = 33;
int const KPI_DATE_TYPE_GI = 34;
int const ACCU_CHNL_ID_GI = 35;
int const ACCESS_FLAG_GI = 36;
int const IS_LIMIT_FLAG_GI = 37;
int const CYCLE_ID_GI = 38;
int const SERV_CHNL_ID_GI = 39;
int const CHNL_SEPARATE_GI = 40;
int const SERV_FLAG_GI = 41;
int const RELA_CHNL_GI = 42;
int const COST_FLAG_GI= 43;
int const SVC_TYPE_GI = 44;
int const SUBS_STATUS_GI = 45;
int const UPDATE_TIME_GI = 46;
int const PRODUCT_TYPE_GI = 47;
int const PAY_COMM_FLAG_GI = 48;
int const MOD_TYPE_GI = 49;



int const GLOBAL_BUSI_DATA_SIZE = 50;

int const CHANNEL_ID_LEN = 20;
int const CITY_CODE_LEN = 31;
int const SUBJECT_ID_LEN = 31;
int const BUSI_CODE_LEN = 12;
int const CALC_CYCLE_LEN = 8;
int const SETT_CYCLE_LEN = 8;
int const DEPT_ID_LEN = 8;
int const COMM_TYPE_LEN = 2;
int const MOD_ID_LEN = 9;
int const RULE_ID_LEN = 9;
int const BUSI_VALUE_LEN = 20;
int const ACCT_NO_LEN = 16;
int const ARRE_CYCLE_LEN = 6;
int const CALC_TYPE_LEN = 2;
int const TAXPAYER_TYPE_LEN = 1;
int const KPI_DATE_TYPE_LEN = 6;
int const CYCLE_ID_LEN = 6;
int const SERV_FLAG_LEN = 1;
int const RELA_CHNL_LEN = 7;
int const COST_FLAG_LEN= 1;
int const MOD_TYPE_LEN = 4;


int const DRIVER_DATA_LEN = 2;
int const DEFAULT_VALUE_LEN = 127;
int const PROVINCE_CODE_LEN = 2;

int const CHANNEL_ID_SIZE = CHANNEL_ID_LEN + 1;
int const SUBJECT_ID_SIZE = SUBJECT_ID_LEN + 1;
int const BUSI_CODE_SIZE = BUSI_CODE_LEN + 1;
int const CALC_CYCLE_SIZE = CALC_CYCLE_LEN + 1;
int const SETT_CYCLE_SIZE = SETT_CYCLE_LEN + 1;
int const DEPT_ID_SIZE = DEPT_ID_LEN + 1;
int const COMM_TYPE_SIZE = COMM_TYPE_LEN + 1;
int const ARRE_CYCLE_SIZE = ARRE_CYCLE_LEN + 1;
int const CALC_TYPE_SIZE = CALC_TYPE_LEN + 1;
int const TAXPAYER_TYPE_SIZE = TAXPAYER_TYPE_LEN + 1;
int const KPI_DATE_TYPE_SIZE = KPI_DATE_TYPE_LEN + 1;
int const SERV_FLAG_SIZE = SERV_FLAG_LEN + 1;
int const RELA_CHNL_SIZE = RELA_CHNL_LEN + 1;
int const COST_FLAG_SIZE= COST_FLAG_LEN + 1;
int const MOD_TYPE_SIZE = MOD_TYPE_LEN + 1;

const global_busi_t GLOBAL_BUSI_CODES_STAGE1[] = {
	{ CHANNEL_ID, CHANNEL_ID_GI, SQLTYPE_STRING, -1, BUSI_FLAG_FROM_SOURCE | BUSI_FLAG_NO_CREATE | BUSI_FLAG_NULL_OK },
	{ CHANNEL_ID, CHANNEL_ID_GI, SQLTYPE_STRING, -1, BUSI_FLAG_FROM_TABLE | BUSI_FLAG_RESET | BUSI_FLAG_NO_CREATE },
	{ CITY_CODE, CITY_CODE_GI, SQLTYPE_STRING, -1, BUSI_FLAG_FROM_SOURCE | BUSI_FLAG_NO_CREATE },
	{ DEAL_DATE, DEAL_DATE_GI, SQLTYPE_STRING, -1, BUSI_FLAG_FROM_SOURCE | BUSI_FLAG_NO_CREATE },
	{ SUBJECT_ID, SUBJECT_ID_GI, SQLTYPE_STRING, -1, BUSI_FLAG_FROM_TABLE | BUSI_FLAG_RESET | BUSI_FLAG_NO_CREATE | BUSI_FLAG_NULL_OK },
	{ EFF_DATE, EFF_DATE_GI, SQLTYPE_DATETIME, sizeof(time_t), 0 },
	{ MOD_ID, MOD_ID_GI, SQLTYPE_INT, sizeof(int), 0 },
	{ RULE_ID, RULE_ID_GI, SQLTYPE_INT, sizeof(int), BUSI_FLAG_NULL_CREATE },
	{ COMM_FEE, COMM_FEE_GI, SQLTYPE_DOUBLE, sizeof(double), 0 },
	{ COMM_TRACK, COMM_TRACK_GI, SQLTYPE_STRING, 1024, 0 },
	{ BUSI_CODE, BUSI_CODE_GI, SQLTYPE_STRING, BUSI_CODE_SIZE, 0 },
	{ BUSI_VALUE, BUSI_VALUE_GI, SQLTYPE_DOUBLE, -1, BUSI_FLAG_FROM_TABLE | BUSI_FLAG_RESET | BUSI_FLAG_NO_CREATE },
	{ ACCT_BUSI_VALUE, ACCT_BUSI_VALUE_GI, SQLTYPE_DOUBLE, -1, BUSI_FLAG_FROM_TABLE | BUSI_FLAG_RESET | BUSI_FLAG_NO_CREATE },
	{ FEE, FEE_GI, SQLTYPE_DOUBLE, sizeof(double), BUSI_FLAG_NULL_CREATE },
	{ CALC_CYCLE, CALC_CYCLE_GI, SQLTYPE_STRING, CALC_CYCLE_SIZE, 0 },
	{ SETT_CYCLE, SETT_CYCLE_GI, SQLTYPE_STRING, SETT_CYCLE_SIZE, 0 },
	{ DEPT_ID, DEPT_ID_GI, SQLTYPE_STRING, DEPT_ID_SIZE, 0 },
	{ COMM_TYPE, COMM_TYPE_GI, SQLTYPE_STRING, COMM_TYPE_SIZE, 0 },
	{ CALC_MONTHS, CALC_MONTHS_GI, SQLTYPE_INT, sizeof(int), 0 },
	{ PAY_CHNL_ID, PAY_CHNL_ID_GI, SQLTYPE_STRING, -1, BUSI_FLAG_FROM_TABLE | BUSI_FLAG_RESET | BUSI_FLAG_NO_CREATE },
	{ POLICY_CHNL_ID, POLICY_CHNL_ID_GI, SQLTYPE_STRING, CHANNEL_ID_SIZE, 0 },
	{ INV_BUSI, INV_BUSI_GI, SQLTYPE_INT, sizeof(int), BUSI_FLAG_FROM_TABLE | BUSI_FLAG_NO_CREATE | BUSI_FLAG_NULL_OK|BUSI_FLAG_RESET },
	{ SUPER_CHNL, SUPER_CHNL_GI, SQLTYPE_STRING, -1, BUSI_FLAG_FROM_TABLE | BUSI_FLAG_RESET | BUSI_FLAG_NO_CREATE },
	{ ACCT_NO, ACCT_NO_GI, SQLTYPE_STRING, -1, BUSI_FLAG_FROM_TABLE | BUSI_FLAG_RESET | BUSI_FLAG_NO_CREATE },
	{ RECORD_LEVEL, RECORD_LEVEL_GI, SQLTYPE_STRING, 5, 0},
	{ REAL_PAYCHNL, REAL_PAYCHNL_GI, SQLTYPE_STRING, CHANNEL_ID_LEN, 0 },
	{ ARRE_CYCLE, ARRE_CYCLE_GI, SQLTYPE_STRING, ARRE_CYCLE_SIZE, 0 },
	{ CALC_TYPE, CALC_TYPE_GI, SQLTYPE_STRING, CALC_TYPE_SIZE, 0 },
	{ SETT_FEE, SETT_FEE_GI, SQLTYPE_DOUBLE, sizeof(double), 0 },
	{ EXPIRE_FEE, EXPIRE_FEE_GI, SQLTYPE_DOUBLE, sizeof(double), 0 },
	{ VAT_PRICE, VAT_PRICE_GI, SQLTYPE_DOUBLE, sizeof(double), 0 },
	{ VAT_TAX, VAT_TAX_GI, SQLTYPE_DOUBLE, sizeof(double), 0 },
	{ KPI_DATE_TYPE, KPI_DATE_TYPE_GI, SQLTYPE_STRING, KPI_DATE_TYPE_SIZE, 0 },
	{ ACCU_CHNL_ID, ACCU_CHNL_ID_GI, SQLTYPE_STRING, CHANNEL_ID_LEN, 0 },
	{ ACCESS_FLAG, ACCESS_FLAG_GI, SQLTYPE_STRING, 2, 0 },
	{ CYCLE_ID, CYCLE_ID_GI, SQLTYPE_INT, sizeof(int), 0 },
	{ SERV_CHNL_ID, SERV_CHNL_ID_GI, SQLTYPE_STRING, -1, BUSI_FLAG_FROM_TABLE | BUSI_FLAG_RESET | BUSI_FLAG_NO_CREATE },
	{ CHNL_SEPARATE, CHNL_SEPARATE_GI, SQLTYPE_DOUBLE, sizeof(double), 0 },
	{ SERV_FLAG, SERV_FLAG_GI, SQLTYPE_INT, SERV_FLAG_SIZE, 0 },
	{ RELA_CHNL, RELA_CHNL_GI, SQLTYPE_STRING, RELA_CHNL_SIZE, 0 },
	{ COST_FLAG, COST_FLAG_GI, SQLTYPE_INT, COST_FLAG_SIZE, 0 },
	{ SVC_TYPE, SVC_TYPE_GI, SQLTYPE_STRING, -1, BUSI_FLAG_FROM_TABLE | BUSI_FLAG_RESET | BUSI_FLAG_NO_CREATE },
	{ SUBS_STATUS, SUBS_STATUS_GI, SQLTYPE_STRING, -1, BUSI_FLAG_FROM_TABLE | BUSI_FLAG_RESET | BUSI_FLAG_NO_CREATE },
	{ UPDATE_TIME, UPDATE_TIME_GI, SQLTYPE_STRING, -1, BUSI_FLAG_FROM_TABLE | BUSI_FLAG_RESET | BUSI_FLAG_NO_CREATE },
	{ PRODUCT_TYPE, PRODUCT_TYPE_GI, SQLTYPE_STRING, -1, BUSI_FLAG_FROM_SOURCE | BUSI_FLAG_NO_CREATE },
	{ PAY_COMM_FLAG, PAY_COMM_FLAG_GI, SQLTYPE_STRING, -1, BUSI_FLAG_FROM_SOURCE | BUSI_FLAG_NO_CREATE },
	{ MOD_TYPE, MOD_TYPE_GI, SQLTYPE_STRING, MOD_TYPE_SIZE, 0 }
};

const global_busi_t GLOBAL_BUSI_CODES_STAGE2[] = {
	{ CHANNEL_ID, CHANNEL_ID_GI, SQLTYPE_STRING, -1, BUSI_FLAG_FROM_SOURCE | BUSI_FLAG_NO_CREATE | BUSI_FLAG_NULL_OK },
	{ CHANNEL_ID, CHANNEL_ID_GI, SQLTYPE_STRING, -1, BUSI_FLAG_FROM_TABLE | BUSI_FLAG_RESET | BUSI_FLAG_NO_CREATE },
	{ CITY_CODE, CITY_CODE_GI, SQLTYPE_STRING, -1, BUSI_FLAG_FROM_SOURCE | BUSI_FLAG_NO_CREATE },
	{ DEAL_DATE, DEAL_DATE_GI, SQLTYPE_STRING, -1, BUSI_FLAG_FROM_SOURCE | BUSI_FLAG_NO_CREATE },
	{ SUBJECT_ID, SUBJECT_ID_GI, SQLTYPE_STRING, -1, BUSI_FLAG_FROM_TABLE | BUSI_FLAG_RESET | BUSI_FLAG_NO_CREATE | BUSI_FLAG_NULL_OK },
	{ EFF_DATE, EFF_DATE_GI, SQLTYPE_DATETIME, sizeof(time_t), 0 },
	{ MOD_ID, MOD_ID_GI, SQLTYPE_INT, sizeof(int), BUSI_FLAG_FROM_SOURCE | BUSI_FLAG_NO_CREATE },
	{ RULE_ID, RULE_ID_GI, SQLTYPE_INT, sizeof(int), BUSI_FLAG_FROM_SOURCE | BUSI_FLAG_NO_CREATE },
	{ COMM_FEE, COMM_FEE_GI, SQLTYPE_DOUBLE, sizeof(double), BUSI_FLAG_FROM_SOURCE | BUSI_FLAG_NO_CREATE },
	{ COMM_TRACK, COMM_TRACK_GI, SQLTYPE_STRING, 1024, BUSI_FLAG_FROM_SOURCE | BUSI_FLAG_NO_CREATE },
	{ BUSI_CODE, BUSI_CODE_GI, SQLTYPE_STRING, BUSI_CODE_SIZE, 0 },
	{ BUSI_VALUE, BUSI_VALUE_GI, SQLTYPE_DOUBLE, -1, BUSI_FLAG_FROM_TABLE | BUSI_FLAG_RESET | BUSI_FLAG_NO_CREATE },
	{ ACCT_BUSI_VALUE, ACCT_BUSI_VALUE_GI, SQLTYPE_DOUBLE, -1, BUSI_FLAG_FROM_TABLE | BUSI_FLAG_RESET | BUSI_FLAG_NO_CREATE },
	{ FEE, FEE_GI, SQLTYPE_DOUBLE, sizeof(double), BUSI_FLAG_FROM_SOURCE | BUSI_FLAG_NO_CREATE },
	{ CALC_CYCLE, CALC_CYCLE_GI, SQLTYPE_STRING, -1, BUSI_FLAG_FROM_SOURCE | BUSI_FLAG_NO_CREATE },
	{ SETT_CYCLE, SETT_CYCLE_GI, SQLTYPE_STRING, -1, BUSI_FLAG_FROM_SOURCE | BUSI_FLAG_NO_CREATE },
	{ COMM_TYPE, COMM_TYPE_GI, SQLTYPE_STRING, -1, BUSI_FLAG_FROM_SOURCE | BUSI_FLAG_NO_CREATE },
	{ FREEZE_EXPIRE, FREEZE_EXPIRE_GI, SQLTYPE_INT, sizeof(int), 0 },
	{ CALC_MONTHS, CALC_MONTHS_GI, SQLTYPE_INT, sizeof(int), 0 },
	{ PAY_CHNL_ID, PAY_CHNL_ID_GI, SQLTYPE_STRING, -1, BUSI_FLAG_FROM_SOURCE | BUSI_FLAG_NO_CREATE },
	{ POLICY_CHNL_ID, POLICY_CHNL_ID_GI, SQLTYPE_STRING, CHANNEL_ID_SIZE, 0 },
	{ CALC_TRACK, CALC_TRACK_GI, SQLTYPE_STRING, 1024, 0 },
	{ INV_BUSI, INV_BUSI_GI, SQLTYPE_INT, sizeof(int), BUSI_FLAG_FROM_TABLE | BUSI_FLAG_NO_CREATE | BUSI_FLAG_NULL_OK|BUSI_FLAG_RESET },
	{ ARRE_CYCLE, ARRE_CYCLE_GI, SQLTYPE_STRING, ARRE_CYCLE_SIZE, 0 },
	{ SETT_FEE, SETT_FEE_GI, SQLTYPE_DOUBLE, sizeof(double), BUSI_FLAG_FROM_SOURCE | BUSI_FLAG_NO_CREATE },
	{ EXPIRE_FEE, EXPIRE_FEE_GI, SQLTYPE_DOUBLE, sizeof(double), BUSI_FLAG_FROM_SOURCE | BUSI_FLAG_NO_CREATE },
	{ VAT_PRICE, VAT_PRICE_GI, SQLTYPE_DOUBLE, sizeof(double), BUSI_FLAG_FROM_SOURCE | BUSI_FLAG_NO_CREATE },
	{ VAT_TAX, VAT_TAX_GI, SQLTYPE_DOUBLE, sizeof(double), BUSI_FLAG_FROM_SOURCE | BUSI_FLAG_NO_CREATE },
	{ TAXPAYER_TYPE, TAXPAYER_TYPE_GI, SQLTYPE_STRING, TAXPAYER_TYPE_SIZE, BUSI_FLAG_FROM_TABLE | BUSI_FLAG_RESET | BUSI_FLAG_NO_CREATE },
	{ KPI_DATE_TYPE, KPI_DATE_TYPE_GI, SQLTYPE_STRING, KPI_DATE_TYPE_SIZE, 0 },
	{ ACCU_CHNL_ID, ACCU_CHNL_ID_GI, SQLTYPE_STRING, CHANNEL_ID_LEN, BUSI_FLAG_FROM_SOURCE | BUSI_FLAG_NO_CREATE },
	{ ACCESS_FLAG, ACCESS_FLAG_GI, SQLTYPE_STRING, 2, BUSI_FLAG_FROM_SOURCE | BUSI_FLAG_NO_CREATE },
	{ IS_LIMIT_FLAG, IS_LIMIT_FLAG_GI, SQLTYPE_STRING, 2, 0 },
	{ CYCLE_ID, CYCLE_ID_GI, SQLTYPE_INT, sizeof(int), 0 },
	{ CHNL_SEPARATE, CHNL_SEPARATE_GI, SQLTYPE_DOUBLE, sizeof(double), BUSI_FLAG_FROM_SOURCE | BUSI_FLAG_NO_CREATE },
	{ SERV_FLAG, SERV_FLAG_GI, SQLTYPE_INT, SERV_FLAG_SIZE, BUSI_FLAG_FROM_SOURCE | BUSI_FLAG_NO_CREATE },
	{ RELA_CHNL, RELA_CHNL_GI, SQLTYPE_STRING, RELA_CHNL_SIZE, BUSI_FLAG_FROM_SOURCE | BUSI_FLAG_NO_CREATE },
	{ COST_FLAG, COST_FLAG_GI, SQLTYPE_INT, COST_FLAG_SIZE, BUSI_FLAG_FROM_SOURCE | BUSI_FLAG_NO_CREATE },
	{ SVC_TYPE, SVC_TYPE_GI, SQLTYPE_STRING, -1, BUSI_FLAG_FROM_TABLE | BUSI_FLAG_RESET | BUSI_FLAG_NO_CREATE },
	{ MOD_TYPE, MOD_TYPE_GI, SQLTYPE_STRING, -1, BUSI_FLAG_FROM_SOURCE | BUSI_FLAG_NO_CREATE }
};

int const GLOBAL_BUSI_DATA_SIZE1 = sizeof(GLOBAL_BUSI_CODES_STAGE1) / sizeof(global_busi_t);
int const GLOBAL_BUSI_DATA_SIZE2 = sizeof(GLOBAL_BUSI_CODES_STAGE2) / sizeof(global_busi_t);

int const FIELD_DATA_RESET = 0x1;			// 在处理记录前需要初始化
int const FIELD_DATA_NO_DEFAULT = 0x2;		// 没有初始化值
int const FIELD_DATA_SET_DEFAULT = 0x4;		// 设置了缺省值
int const FIELD_DATA_GOTTEN = 0x8;			// 是否已经查找
int const FIELD_DATA_BELONG_BUSI = 0x10;	// 是否关联指标
int const FIELD_DATA_POLICY_RESET = 0x20;	// 切换政策时需要重新初始化
int const FIELD_DATA_USE_DEFAULT = 0x40;	// 是否使用缺省值
int const FIELD_DATA_HAS_ALIAS = 0x80;		// 指标有别名

// 话单的受理时间不在政策的生效范围内
int const EPOLICY_EXPIRED = 101;
// 话单的地市不在政策的适用地市范围内
int const EPOLICY_NOT_AREA = 102;
// 指定指标的值使用了缺省值，但不允许使用缺省值
int const EPOLICY_NO_DATA = 103;
// 指定指标的值不符合政策的生效条件
int const EPOLICY_NOT_MATCH = 104;
// 指标INV_BUSI的返回值为1
int const EPOLICY_FILTER = 105;
//业务受理时间小于最小政策生效日期
int const EPOLICY_LESS_EFF_DATE = 106;

// 话单的受理时间不在规则的生效范围内
int const ERULE_EXPIRED = 201;
// 话单的地市不在规则的适用地市范围内
int const ERULE_NOT_AREA = 202;
// 话单的计算周期超出范围
int const ERULE_OUT_OF_CYCLE = 203;
// 指定指标的值使用了缺省值，但不允许使用缺省值
int const ERULE_NO_DATA = 204;
// 指定指标的值期望获得一个，但返回了多个值
int const ERULE_TOO_MANY_VALUES = 205;
// 指定指标的值不符合规则的生效条件
int const ERULE_NOT_MATCH = 206;
// 话单不符合驱动源条件
int const ERULE_NOT_DRIVER = 207;
// 计算指标的值使用了缺省值，但不允许使用缺省值
int const ERULE_CALC_NO_DATA = 208;
// 结算给服务渠道的规则不适用非主产品订购
int const ERULE_ONLY_MATCH_MAIN_PRODUCT = 209;

// 没有规则匹配
int const ECOMM_NO_MATCH = 301;
// 计算出错
int const ECOMM_CALCULATION = 302;

// 下面是指标相关的定义
struct field_data_t
{
	int flag;		// 处理记录的标识
	string busi_code;	// 指标名称
	string ref_busi_code;	// 在科目查找时使用
	int field_type;	// 字段类型
	int field_size;	// 字段长度
	int max_size;	// 最大可以容纳的字段个数
	int data_size;	// 当前的字段个数
	char *data;		// 字段值指针
	int table_id;	// 数据来源表
	// 字段在表中的索引，表中的字段表示在values数组中的位置，
	// 在输入中时表示字段的对应位置，subject_index表示科目的对应位置
	int value_index;
	int subject_index;
	int deal_type;	// 处理方式
	int orig_deal_type;	//原始处理方式
	string default_value;	// 初始值
	vector<field_data_t *> rela_fields;	// 通过规则获取的指标相关的字段
	const char **in_array;	// 函数调用时的输入数组
	int func_idx;
	compiler::func_type func;

	field_data_t();
	field_data_t(int flag_, const string& busi_code_, const string& ref_busi_code_, int field_type_,
		int field_size_, int table_id_, int value_index_, int subject_index_, int deal_type_, bool use_default = false,
		const string& default_value_ = "",int orig_deal_type_=-1);
	~field_data_t();

	double get_double() const;
	void set_data(const string& value);
	friend ostream& operator<<(ostream& os, const field_data_t& field_data);
	friend dstream& operator<<(dstream& os, const field_data_t& field_data);
};

typedef map<string, field_data_t *> field_data_map_t;

}
}

#endif

