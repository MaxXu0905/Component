#if !defined(__CALT_CTX_H__)
#define __CALT_CTX_H__

#include "sa_struct.h"
#include "cal_manager.h"
#include "cal_struct.h"

namespace ai
{
namespace app
{

using namespace std;
using namespace ai::sg;
using namespace ai::scci;

class cal_policy_area_t;
class cal_policy_t;
class cal_rule_area_t;
class cal_rule_t;
class cal_rule_complex_t;
class cal_busi_t;

struct cal_busi_alias_t
{
	int rule_id;
	string busi_code;

	bool operator<(const cal_busi_alias_t& rhs) const {
		if (rule_id < rhs.rule_id)
			return true;
		else if (rule_id > rhs.rule_id)
			return false;

		return busi_code < rhs.busi_code;
	}
};

class calt_ctx
{
public:
	static calt_ctx * instance();
	~calt_ctx();

	// ���ߵ��������
	set<cal_policy_area_t> _CALT_policy_area_set;
	// �������ã�ϸ���
	map<int, cal_policy_t> _CALT_policy_map;

	// ������������
	set<cal_rule_area_t> _CALT_rule_area_set;
	// �涨�����
	map<int, cal_rule_t> _CALT_rule_map;
	// ����ָ�궨���
	set<cal_rule_complex_t> _CALT_rule_complex_set;

	// ָ��δ����Ĺ��������Ϣ��
	map<int, string> _CALT_busi_errors;
	// ָ�궨���
	map<string, cal_busi_t> _CALT_busi_map;
	// ��˰��������˰��ӳ���
	map<string, double> _CALT_chl_taxtype_map;
	// ��Ҫ����ָ������Դ��ָ��
	set<string> _CALT_specify_driver_busi_set;

	field_data_map_t _CALT_data_map;
	field_data_t *_CALT_global_fields[GLOBAL_BUSI_DATA_SIZE];
	field_data_t **_CALT_input_fields;
	field_data_t **_CALT_output_fields;

	char *_CALT_global_pay_return;
	char *_CALT_global_busi_code;
	char *_CALT_global_busi_value;
	char *_CALT_global_is_accumulate;
	char *_CALT_global_is_limit_flag;

	int _CALT_mod_id;
	int _CALT_rule_id;
	Generic_Database *_CALT_db;

	cal_manager *_CALT_mgr_array[CAL_TOTAL_MANAGER];

	// ����ָ�������
	int _CALT_gen_busi_idx;
	// ָ������������ӳ��
	map<string, string> _CALT_gen_busi;
	// ������ָ�����Ƶ�ӳ��
	map<string, string> _CALT_expr_map;
	// ָ�����ض������ĵ�����
	map<cal_busi_alias_t, string> _CALT_busi_alias;


	int _CALT_policy_error_no;
	string _CALT_policy_error_string;
	int _CALT_rule_error_no;
	string _CALT_rule_error_string;


private:
	calt_ctx();

	static boost::thread_specific_ptr<calt_ctx> instance_;
};

}
}

#endif

