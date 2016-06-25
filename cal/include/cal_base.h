#if !defined(__CAL_H__)
#define __CAL_H__

#include "sa_internal.h"
#include "cal_tree.h"
#include "cal_policy.h"
#include "cal_rule.h"

namespace ai
{
namespace app
{

using namespace std;
using namespace ai::sg;
using namespace ai::scci;

int const TRACK_STAGE_COND = 1;
int const TRACK_STAGE_CALC = 2;
int const TRACK_STAGE_PAY = 3;

int const DUP_ACCT_LEVEL = 1;
int const DUP_CRBO_LEVEL = 2;

typedef vector<string> cal_busi_key_t;
struct cal_busi_value_t
{
	double busi_value;
	int busi_count;

	cal_busi_value_t(double _busi_value, int _busi_count)
	{
		busi_value = _busi_value;
		busi_count = _busi_count;
	}
};

typedef vector<string> cal_accu_key_t;
typedef vector<double> cal_accu_value_t;

struct cal_mod_rule_complex_t
{
	int mod_id;
	int rule_id;
	string busi_code;

	bool operator<(const cal_mod_rule_complex_t& rhs) const{
		if (mod_id < rhs.mod_id)
			return true;
		else if (mod_id > rhs.mod_id)
			return false;
	
		if (rule_id < rhs.rule_id)
			return true;
		else if (rule_id > rhs.rule_id)
			return false;

		return busi_code < rhs.busi_code;
	}
};

class cal_base : public sa_base
{
public:
	cal_base(int sa_id_);
	virtual ~cal_base();

private:
	virtual bool support_batch() const;
	virtual bool support_concurrency() const;
	virtual bool call();
	virtual void pre_init();
	virtual void post_init();
	virtual void post_init2();
	virtual void post_open();
	virtual void post_run();
	virtual void pre_rollback();

	void load_channel();
	void load_strategic_channel();
	void load_finish();
	void create_global_fields(bool table_loaded);
	void create_input_fields();
	void create_output_fields();
	void set_output_fields();
	void reset_fields();
	void set_input_fields();
	void do_calculation();
	bool do_rule(const cal_policy_t& policy);
	void do_policy();
	bool accumulate(const cal_policy_t& policy, const cal_rule_t& rule_cfg);
	void set_track(int track_stage);
	void reset_svc_chnl_fields();
	void pre_write();
	int get_calc_cycle();
	void set_calc_months();
	bool match_driver(int policy_flag, int rule_flag);
	bool match_busi(int policy_flag, int rule_flag, int accu_flag);
	void collect_driver();
	bool insert_dup(int dup_flag);
	void packup_dup();
	void extract_dup();
	void do_merge();
	void write_busi();
	void write_acct_busi();
	void write_accu();
	void set_kpi_date_type(int rule_id);

	calp_ctx *CALP;
	calt_ctx *CALT;

	int dup_idx;
	compiler::func_type dup_func;
	int dup_crbo_idx;
	compiler::func_type dup_crbo_func;
	char *dup_output[3];

	map<cal_busi_key_t, cal_busi_value_t> merge_map;
	map<cal_accu_key_t, cal_accu_value_t> accu_map;
	bool is_busi;	// 指标汇总
	int accu_keys;
	time_t min_eff_date;

	set<string> channel_set;
	set<string> strategic_channel_set;
	set<cal_mod_rule_complex_t> mod_rule_complex_set;
	bool is_write_inv;
	string inv_line;   //未出账无效记录字段
	char input_delimiter;
	char svc_type[3];
};

}
}

#endif

