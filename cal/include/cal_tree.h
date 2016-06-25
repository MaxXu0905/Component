#if !defined(__CAL_TREE_H__)
#define __CAL_TREE_H__

#include "sa_internal.h"
#include "sdc_api.h"
#include "cal_struct.h"
#include "cal_manager.h"
#include "calp_ctx.h"
#include "calt_ctx.h"

namespace ai
{
namespace app
{

using namespace std;
using namespace ai::sg;
using namespace ai::scci;

int const DBC_FLAG_BUSI_TABLE = 0x1;
int const DBC_FLAG_PAY_CHNL = 0x2;
int const DBC_FLAG_POLICY_LEVEL = 0x4;

int const CHNL_LEVEL_NONE = 0;
int const CHNL_LEVEL_CHNL = 1;
int const CHNL_LEVEL_ACCT = 2;
int const CHNL_LEVEL_USER_CHNL = 3;

int const BUSI_IS_MIXED = 0x1;			// 复合指标
int const BUSI_DYNAMIC = 0x2;			// 动态指标
int const BUSI_USER_DEFINED = 0x4;		// 自定义指标，需要科目过滤
int const BUSI_FROM_SOURCE = 0x8;		// 来源于文件的指标
int const BUSI_FROM_TABLE = 0x10;		// 来源于表的指标
int const BUSI_CHANNEL_LEVEL = 0x20;	// 是否按渠道级汇总，不包括其他源的汇总指标
int const BUSI_DEAL_DIRECT = 0x40;		// 直接查找
int const BUSI_DEAL_SUM = 0x80;			// 需要累加
int const BUSI_DEAL_COUNT = 0x100;		// 需要累计
int const BUSI_DEAL_ATTR = 0x200;		// 用户属性指标
int const BUSI_DEAL_BOOL = 0x400;		// 是否存在记录
int const BUSI_DEAL_ELAPSED = 0x800;	// 账龄
int const BUSI_DEAL_FUNCTION = 0x1000;	// 通过函数
int const BUSI_ALLOCATABLE = 0x2000;	// 复合指标来源于输入，可摊分
int const BUSI_ACCT_LEVEL = 0x4000;  //账户级汇总
int const BUSI_USER_CHNL_LEVEL = 0x8000;  //用户级的渠道级指标


struct dbc_field_t
{
	string field_name;			// 字段名称
	sqltype_enum field_type;	// 字段类型
	int field_size;				// 字段长度
	int field_offset;			// 字段在数据结构中的偏移量
	field_data_t *field_data;	// 指标数据

	dbc_field_t();
	~dbc_field_t();
};

struct dbc_info_t
{
	int real_table_id;				// 查找DBC时使用的表ID
	int real_index_id;				// 查找DBC时使用的索引ID
	int dbc_flag;					// 特殊标记
	string table_name;				// 查找共享内存时使用的表名，需转换成小写
	int struct_size;				// 数据结构字节数
	vector<dbc_field_t> keys;
	vector<dbc_field_t> values;
	int subject_index;
	bool valid;

	dbc_info_t();
	~dbc_info_t();
};

// 数据来源
struct cal_data_source_t
{
	string driver_data;
	string busi_code;
	string ref_busi_code;
	int chnl_level;
	string table_name;
	string column_name;
	int deal_type;
	bool use_default;
	string default_value;
	string subject_name;
	int complex_type;
	int multiplicator;
	int busi_type;
};

// 指标定义
struct cal_busi_cfg_t
{
	string busi_code;
	string src_busi_code;
	int statistics_object;
	bool is_mix_flag;
	bool busi_local_flag;
	bool enable_cycles;
};

// 指标数据来源临时表
struct cal_orign_data_source_t
{
	string busi_code;
	int chnl_level;
	string driver_data;
	string table_name;
	string field_name;
	int deal_type;
	bool use_default;
	string default_value;
	string subject_name;
	bool is_mix_flag;
	bool enable_cycles;
	int complex_type;
	int multiplicator;
	int busi_type;
	bool is_user_chnl_level;

	bool operator<(const cal_orign_data_source_t& rhs) const;
};

struct cal_rule_busi_t
{
	string rule_busi;
	int field_type;
	int field_size;
	int precision;
	bool busi_local_flag;
};

struct cal_rule_busi_source_t
{
	string busi_code;
	string ref_busi_code;
	cal_rule_busi_t rule_busi;
};

struct cal_busi_subject_t
{
	int rel_type;
	vector<string> items;
};

struct cal_busi_t
{
	int flag;
	string driver_data;			// 驱动源
	int complex_type;			// 复合指标处理方式
	string field_name;			// 字段名称
	field_data_t *field_data;	// 指标数据
	cal_busi_subject_t subject;	// 科目
	int multiplicator;			// -1: 没有精度，>=0 指定精度转化成乘数
	string sub_busi_code;		// 渠道级指标如果可摊分，则记录其明细指标
	int busi_type;
	bool enable_cycles;
	bool valid;

	cal_busi_t();
	~cal_busi_t();
};

struct table_index_t
{
    int current_index; // 表索引当前值
    int total_size; // 表索引总值
};

class table_iterator
{
public:
    // 表迭代构造函数
    table_iterator();
    table_iterator(map<int, table_index_t> *table_indexes_);
    // 析构函数，释放资源
    ~table_iterator();
    // 设置关联参数
    void set(map<int, table_index_t> *table_indexes_);

    // 下一个记录
    bool next();
    // 结束
    bool eof();
    // 获取指定表的索引
    int get_index(int table_id);

private:
    map<int, table_index_t> *table_indexes; // 表索引数组
    bool eof_; // 结束标识
};

struct cal_rule_busi_item_t
{
	string busi_code;
	string rule_busi;
	int field_type;
	int field_size;
	int precision;
	bool busi_local_flag;
};

class cal_tree : public cal_manager
{
public:
	static cal_tree& instance(calt_ctx *CALT);

	void load();
	void get_field_value(field_data_t *field_data);
	bool in_subject(const string& busi_code, const string& subject_id);
	bool in_attr(const cal_busi_subject_t& subject, const string& subject_id);
	bool ignored(const string& busi_code);
	void reset_fields();
	bool add_rule_busi(int rule_id, const string& alias_name, const string& rule_busi, string& busi_code);

private:
	cal_tree();
	virtual ~cal_tree();

	void load_dbc_table();
	void load_dbc_alias();
	void make_storage();
	void load_busi_driver();
	void load_datasource_cfg();
	void load_busi_cfg();
	void init_datasource();
	void load_busi_special();
	void load_busi_ignore();
	void load_rule_busi();
	bool add_rule_busi(const cal_rule_busi_item_t& busi_item);
	void init_busi_cfg();
	void load_subject();
	void load_chl_taxtype();
	void set_null_fields(vector<dbc_field_t>& values);
	void set_data_source(const string& busi_code, const string& ref_busi_code, const cal_rule_busi_t& rule_busi_item);
	void set_data_source(const string& busi_code, const string& ref_busi_code, const cal_orign_data_source_t& orign_ds);
	void set_from_source(const string& busi_code, const string& ref_busi_code, const cal_orign_data_source_t& orign_ds);
	void set_from_table(const string& busi_code, const string& ref_busi_code, const cal_orign_data_source_t& orign_ds);
	void set_busi_cfg(const string& busi_code, const string& src_busi_code, const string& ref_busi_code,
		int chnl_level, bool is_mix_flag, bool enable_cycles, vector<cal_rule_busi_source_t>& todo_vector);
	bool has_orign(const dbc_field_t& field, bool enlarge);
	bool is_global(const string& field_name);
	bool parse_rule_busi(const string& busi_code, const string& rule_busi, int field_type,
		vector<field_data_t *>& rela_fields, vector<const char *>& in_array, string& dst_code, int& busi_flag,
		int& busi_type, bool local_flag);
	void check_dbc(bool enlarge);

	static void dump(const string& field_name, int field_type, const char *data);

	// DBC加载表信息映射
	map<int, dbc_info_t> table_info_map;
	// 数据源数组
	vector<cal_data_source_t> data_source;
	// 指标定义数组
	vector<cal_busi_cfg_t> busi_cfg;
	// 指标名称与指标来源映射表
	set<cal_orign_data_source_t> orign_ds_set;
	// 表名与索引的映射关系
	map<string, int> table_name_map;
	// 指标与其驱动源的映射表
	map<string, string> busi_driver_map;
	// 渠道级驱动源下，需要忽略的指标集
	set<string> busi_ignore_set;
	// 公式计算的指标映射表
	map<string, cal_rule_busi_t> rule_busi_map;
	//渠道特征本地化后的指标
	set<string> chnl_feature_busi_set;
	// 查询需要的缓存
	int max_record_size;
	int max_result_records;
	char *src_buf;
	char *result_buf;

	int busi_table_id;
	int acct_busi_table_id;
	int busi_value_index;
	int acct_busi_value_index;
	vector<field_data_t *> policy_field_data;

	sdc_api& sdc_mgr;

	friend class calt_ctx;
};

}
}

#endif

