#if !defined(__CAL_CTX_H__)
#define __CAL_CTX_H__

#include "sg_internal.h"
#include "svcp_ctx.h"

namespace ai
{
namespace app
{

using namespace ai::sg;
using namespace ai::scci;

enum accu_flag_mode
{
	ACCU_FLAG_NONE = 0,
	ACCU_FLAG_POLICY = 1,
	ACCU_FLAG_RULE = 2
};

enum collision_level_mode
{
	COLLISION_LEVEL_NONE = 0,
	COLLISION_LEVEL_SOURCE = 1,
	COLLISION_LEVEL_ALL = 2
};

enum cycle_type_mode
{
	CYCLE_TYPE_DAY = 0,
	CYCLE_TYPE_MONTH = 1
};

// ������Ҫʹ�õı��б�
struct cal_dbc_table_t
{
	string table_name;		// �����ı�������ת����Сд
	int index_id;			// ����������ID

	bool operator==(const cal_dbc_table_t& rhs) const {
		return (table_name == rhs.table_name && index_id == rhs.index_id);
	}

	bool operator<(const cal_dbc_table_t& rhs) const {
		if (table_name < rhs.table_name)
			return true;
		else if (table_name > rhs.table_name)
			return false;

		return (index_id < rhs.index_id);
	}
};

// ������Ҫʹ�õı�����б�
struct cal_dbc_alias_t
{
	string table_name;					// ��������ת����Сд
	string orig_table_name;				// ������Ŀ�����ת����Сд
	int orig_index_id;					// ������Ŀ������
	map<string, string> field_map;
};

struct cal_parms_t
{
	string dbname; 						// database name to load configuration
	string openinfo;						// information to open database
	string province_code;					// province_code that we are calculating.
	string driver_data;						// driver data for data source from policy
	int stage;								// identify current handling stage.
	collision_level_mode collision_level;		// �ֶγ�ͻ����
	bool channel_level;					// ��������������
	cycle_type_mode cycle_type;				// ��������M: �¼��½� D:�ռ��ս�
	int busi_limit_month;					// 1:������2:��ʷ��
	string rule_dup;						// ���ع���
	string rule_dup_crbo;					// ����ҵ����ع���
	string dup_svc_key;					// ���ط�����
	int dup_version;						// ���ط���汾
	int dup_all_partitions;					// ���ص����з�����
	int dup_partitions;						// ���ڵ��ϵķ�����
};

class calp_ctx
{
public:
	static calp_ctx * instance();
	~calp_ctx();

	bool get_config(const string& svc_key, int version);

	bool _CALP_run_complex;					// �Ƿ���㸴��ָ��
	bool _CALP_inv_out;                                             //�Ƿ����δ����ԭ��
	bool _CALP_collect_driver;					// ֻ�ռ�����Դ��Ϣ����ִ��
	set<int> _CALP_env_mod_ids;				// ��Ҫ���ص�����
	bool _CALP_debug_inv;
	int _CALP_busi_type;					// ��Ҫ���ܵ�ָ������

	cal_parms_t _CALP_parms;					// ��Դ����
	vector<cal_dbc_table_t> _CALP_dbc_tables;	// ��ѯDBCʱʹ�õı��б�
	vector<cal_dbc_alias_t> _CALP_dbc_aliases;	// ��ѯDBCʱʹ�õı����б�
	map<string, sqltype_enum> _CALP_field_types;	// �����ֶε����Ͷ���
	map<string, string> _CALP_default_values;		// �ֶ�Ĭ��ֵ�б�

private:
	calp_ctx();

	void load_config(const string& config);
	void load_parms(const bpt::iptree& pt);
	void load_tables(const bpt::iptree& pt);
	void load_aliases(const bpt::iptree& pt);
	void load_input(const bpt::iptree& pt);
	void load_output(const bpt::iptree& pt);
	bool get_partitions();

	static void init_once();

	static boost::once_flag once_flag;
	static calp_ctx *_instance;
};

}
}

#endif

