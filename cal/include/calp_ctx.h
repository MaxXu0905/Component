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

// 计算需要使用的表列表
struct cal_dbc_table_t
{
	string table_name;		// 关联的表名，需转换成小写
	int index_id;			// 关联的索引ID

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

// 计算需要使用的表别名列表
struct cal_dbc_alias_t
{
	string table_name;					// 别名表，需转换成小写
	string orig_table_name;				// 别名的目标表，需转换成小写
	int orig_index_id;					// 别名的目标索引
	map<string, string> field_map;
};

struct cal_parms_t
{
	string dbname; 						// database name to load configuration
	string openinfo;						// information to open database
	string province_code;					// province_code that we are calculating.
	string driver_data;						// driver data for data source from policy
	int stage;								// identify current handling stage.
	collision_level_mode collision_level;		// 字段冲突级别
	bool channel_level;					// 渠道级计算流程
	cycle_type_mode cycle_type;				// 账期类型M: 月计月结 D:日计日结
	int busi_limit_month;					// 1:计算月2:历史月
	string rule_dup;						// 查重规则
	string rule_dup_crbo;					// 代办业务查重规则
	string dup_svc_key;					// 查重服务名
	int dup_version;						// 查重服务版本
	int dup_all_partitions;					// 查重的所有分区数
	int dup_partitions;						// 单节点上的分区数
};

class calp_ctx
{
public:
	static calp_ctx * instance();
	~calp_ctx();

	bool get_config(const string& svc_key, int version);

	bool _CALP_run_complex;					// 是否计算复合指标
	bool _CALP_inv_out;                                             //是否输出未出帐原因
	bool _CALP_collect_driver;					// 只收集驱动源信息，不执行
	set<int> _CALP_env_mod_ids;				// 需要加载的政策
	bool _CALP_debug_inv;
	int _CALP_busi_type;					// 需要汇总的指标类型

	cal_parms_t _CALP_parms;					// 资源参数
	vector<cal_dbc_table_t> _CALP_dbc_tables;	// 查询DBC时使用的表列表
	vector<cal_dbc_alias_t> _CALP_dbc_aliases;	// 查询DBC时使用的别名列表
	map<string, sqltype_enum> _CALP_field_types;	// 输入字段的类型定义
	map<string, string> _CALP_default_values;		// 字段默认值列表

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

