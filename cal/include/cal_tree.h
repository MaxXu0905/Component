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

int const BUSI_IS_MIXED = 0x1;			// ����ָ��
int const BUSI_DYNAMIC = 0x2;			// ��ָ̬��
int const BUSI_USER_DEFINED = 0x4;		// �Զ���ָ�꣬��Ҫ��Ŀ����
int const BUSI_FROM_SOURCE = 0x8;		// ��Դ���ļ���ָ��
int const BUSI_FROM_TABLE = 0x10;		// ��Դ�ڱ��ָ��
int const BUSI_CHANNEL_LEVEL = 0x20;	// �Ƿ����������ܣ�����������Դ�Ļ���ָ��
int const BUSI_DEAL_DIRECT = 0x40;		// ֱ�Ӳ���
int const BUSI_DEAL_SUM = 0x80;			// ��Ҫ�ۼ�
int const BUSI_DEAL_COUNT = 0x100;		// ��Ҫ�ۼ�
int const BUSI_DEAL_ATTR = 0x200;		// �û�����ָ��
int const BUSI_DEAL_BOOL = 0x400;		// �Ƿ���ڼ�¼
int const BUSI_DEAL_ELAPSED = 0x800;	// ����
int const BUSI_DEAL_FUNCTION = 0x1000;	// ͨ������
int const BUSI_ALLOCATABLE = 0x2000;	// ����ָ����Դ�����룬��̯��
int const BUSI_ACCT_LEVEL = 0x4000;  //�˻�������
int const BUSI_USER_CHNL_LEVEL = 0x8000;  //�û�����������ָ��


struct dbc_field_t
{
	string field_name;			// �ֶ�����
	sqltype_enum field_type;	// �ֶ�����
	int field_size;				// �ֶγ���
	int field_offset;			// �ֶ������ݽṹ�е�ƫ����
	field_data_t *field_data;	// ָ������

	dbc_field_t();
	~dbc_field_t();
};

struct dbc_info_t
{
	int real_table_id;				// ����DBCʱʹ�õı�ID
	int real_index_id;				// ����DBCʱʹ�õ�����ID
	int dbc_flag;					// ������
	string table_name;				// ���ҹ����ڴ�ʱʹ�õı�������ת����Сд
	int struct_size;				// ���ݽṹ�ֽ���
	vector<dbc_field_t> keys;
	vector<dbc_field_t> values;
	int subject_index;
	bool valid;

	dbc_info_t();
	~dbc_info_t();
};

// ������Դ
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

// ָ�궨��
struct cal_busi_cfg_t
{
	string busi_code;
	string src_busi_code;
	int statistics_object;
	bool is_mix_flag;
	bool busi_local_flag;
	bool enable_cycles;
};

// ָ��������Դ��ʱ��
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
	string driver_data;			// ����Դ
	int complex_type;			// ����ָ�괦��ʽ
	string field_name;			// �ֶ�����
	field_data_t *field_data;	// ָ������
	cal_busi_subject_t subject;	// ��Ŀ
	int multiplicator;			// -1: û�о��ȣ�>=0 ָ������ת���ɳ���
	string sub_busi_code;		// ������ָ�������̯�֣����¼����ϸָ��
	int busi_type;
	bool enable_cycles;
	bool valid;

	cal_busi_t();
	~cal_busi_t();
};

struct table_index_t
{
    int current_index; // ��������ǰֵ
    int total_size; // ��������ֵ
};

class table_iterator
{
public:
    // ��������캯��
    table_iterator();
    table_iterator(map<int, table_index_t> *table_indexes_);
    // �����������ͷ���Դ
    ~table_iterator();
    // ���ù�������
    void set(map<int, table_index_t> *table_indexes_);

    // ��һ����¼
    bool next();
    // ����
    bool eof();
    // ��ȡָ���������
    int get_index(int table_id);

private:
    map<int, table_index_t> *table_indexes; // ����������
    bool eof_; // ������ʶ
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

	// DBC���ر���Ϣӳ��
	map<int, dbc_info_t> table_info_map;
	// ����Դ����
	vector<cal_data_source_t> data_source;
	// ָ�궨������
	vector<cal_busi_cfg_t> busi_cfg;
	// ָ��������ָ����Դӳ���
	set<cal_orign_data_source_t> orign_ds_set;
	// ������������ӳ���ϵ
	map<string, int> table_name_map;
	// ָ����������Դ��ӳ���
	map<string, string> busi_driver_map;
	// ����������Դ�£���Ҫ���Ե�ָ�꼯
	set<string> busi_ignore_set;
	// ��ʽ�����ָ��ӳ���
	map<string, cal_rule_busi_t> rule_busi_map;
	//�����������ػ����ָ��
	set<string> chnl_feature_busi_set;
	// ��ѯ��Ҫ�Ļ���
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

