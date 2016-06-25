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
 * ���ڵ���Ӷ����Ҫ��Ч̯��
 * ���ڱ���Ӷ��Ҳ��Ҫ��Ч̯��
 * ���ڶ���Ӷ��Ҫ������ָ���Ƿ����ã��������Ƿ�̯��
 * ��������˼���ָ�꣬����Ҫ̯�֣�����Ҫ������ָ�������
 * 1. �û�������ָ�꣬����̯�֣�������Ӷ����㷽��һ��
 * 2. ����������ָ�꣬����Ҫ̯��
 * 3. ���û�����ü���ָ�꣬����Ҫ̯��
 */
int const CALC_METHOD_MIN = 1;
int const CALC_METHOD_PRICE = 1; 	// ����Ӷ�� (һ��������Ӷ��)
int const CALC_METHOD_PERCENT = 2;	// ����Ӷ�� (���õİٷֱ�)
int const CALC_METHOD_FIXED = 3;	// ����Ӷ�� (�ܶ�̶�Ӷ��)
int const CALC_METHOD_MAX = 3;

int const PAY_TYPE_MIN = 1;
int const PAY_TYPE_ONCE = 1;	// һ��֧��
int const PAY_TYPE_PERCENT = 2;	// ����֧��(�ٷֱ�) ------ ����֧������ÿ��֧���������Բ�ͬ
int const PAY_TYPE_ABSOLUTE = 3;	// ����֧��(����ֵ)  ------ ����֧������ÿ��֧���������Բ�ͬ
int const PAY_TYPE_COND_ONCE = 4;	// �����ﵽһ����֧��
int const PAY_TYPE_COND_CONT = 5;	// ���������ﵽһ����֧��
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
	int calc_start_time_unit;	// ������ʼ�㵥λ
	int calc_start_time;
	int cycle_time_unit;	// ���������ڵ�λ
	int cycle_time;		// ������Ӷ��ʹ��(��λΪ�·�) - �����һ��
	int cycle_type;		// 0:�ȱ�����1:�ǵȱ���
	int calc_method;	// ���㷽��
	int calc_cycle;		// �������
	int calc_start_type; // ������ʼ������:0����ʱ�� 1����ʱ��
	int calc_symbol;     //����ҵ���Ƿ����:0��ȥ�� 1ȥ��
	int settlement_to_service; //�Ƿ�������������:1��0��
	int as_operating_costs; //�Ƿ���Ϊ������Ӫ�ɱ�:1��0��
	double share_ratio; //���������ֳɱ���
};

enum cond_opt_enum
{
	COND_OPT_UNKNOWN,
	COND_OPT_GT,	// ����
	COND_OPT_GE,	// ���ڵ���
	COND_OPT_LT,	// С��
	COND_OPT_LE,	// С�ڵ���
	COND_OPT_EQ,	// ����
	COND_OPT_NE,	// ������
	COND_OPT_OOI,	// ǰ��������
	COND_OPT_OCI,	// ǰ���������
	COND_OPT_COI,	// ǰ�պ�����
	COND_OPT_CCI,	// ǰ�պ������
	COND_OPT_INC,	// ����
	COND_OPT_EXC	// ������
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

int const ACCU_FLAG_MATCH_DRIVER = 0x1;	// ��ǰָ��������Դһ��
int const ACCU_FLAG_NULL_DRIVER = 0x2;	// ��ǰû��ָ������Դ

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
	vector<cal_cond_para_t> pay_cond;	// ֧������
	double pay_value;	// ֧�����
};

struct cal_rule_calc_para_t
{
	long month1;
	long month2;
	double value;
};
struct cal_rule_calc_detail_t
{
	// ���ݼ��������
	double lower_value;
	// ���ݼ��������
	double upper_value;
	// �ۼ�ָ������
	vector<cal_cond_para_t> ref_cond;
	// ��������·���Ӷ���ӳ���
	map<int, double> calc_values;
	vector<cal_rule_calc_para_t> interval_calc_para;    //���ڷֳɷ���������·�����

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

int const RULE_FLAG_STEP = 0x1;						// ���ݷ���
int const RULE_FLAG_MATCH_DRIVER = 0x8;			// ��ǰ����������Դһ��
int const RULE_FLAG_NULL_DRIVER = 0x10;			// ��ǰû��ָ������Դ
int const RULE_FLAG_NO_CALC_BUSI = 0x20;			// û��ָ������ָ��
int const RULE_FLAG_UNKNOWN_CALC_BUSI = 0x40;	// ����ʶ�ļ���ָ��
int const RULE_FLAG_CONFLICT_DRIVER = 0x80;		// ����Դ��ͻ
int const RULE_FLAG_ALLOCATABLE = 0x100;			// ���ԶԵ�ǰ����Դ������Ч̯��
int const RULE_FLAG_CHANNEL = 0x200;				// ������ָ��
int const RULE_FLAG_INITED = 0x400;					// �Ƿ��Ѿ���ʼ��
int const RULE_FLAG_ACCT = 0x800;					// �˻�������ָ�꣬Ҫ����˻�id
int const RULE_FLAG_OPEN_TIME = 0x1000;			// ������ʼ������Ϊ����ʱ��
int const RULE_FLAG_MATCH_ONE_DRIVER = 0x2000;		// ���ʽ��������һ��ָ��ƥ������Դ
int const RULE_FLAG_CAL_EXPR_SEPCIFY_DRIVER = 0x4000;		// ����ָ���Ǳ��ʽ,���Ұ�������ָ������Դ��ָ��


struct cal_rule_t
{
	// �ù������ʱ�Ƿ�Ϊ���ݷ���
	int flag;
	// ������Чʱ��
	time_t eff_date;
	// ����ʧЧʱ��
	time_t exp_date;
	// ָ�����õķ�Χ
	int cycles;
	// ����ʱ����������
	cal_rule_calc_cfg_t calc_cfg;
	// �ڵ�һ�׶Σ�����Ϊ��
	// �ڵ����׶Σ����ܵ�Դָ��
	string calc_busi_code;
	string calc_field_name;
	field_data_t *calc_field_data;
	// �ڵ�һ�׶Σ�Ϊ���ܵ�Դָ��
	// �ڵ����׶Σ�Ϊ����ָ��
	string sum_busi_code;
	string sum_field_name;
	field_data_t *sum_field_data;
	// �ο���ָ���ۼ�����
	vector<cal_cond_para_t> ref_cond;
	// �ۼ�ָ������
	vector<cal_accu_cond_t> accu_cond;
	vector<cal_accu_cond_t> acct_busi_cond;    //�˻���ָ������
	// �������
	vector<cal_rule_calc_detail_t> calc_detail;
	// ֧������
	cal_rule_payment_t pay_cfg;
	bool valid;
        // ���ø���ָ�������
	vector<cal_complex_busi_attr_t> complex_attr;
	//������ŵ��Чָ�������
	vector<cal_kpi_busi_attr_t> kpi_attr;
	// ��Ҫ�ڹ켣�����ֵ�ָ�����飬ȥ�����ظ�ָ��
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

