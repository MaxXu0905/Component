#if !defined(__SA_STRUCT_H__)
#define __SA_STRUCT_H__

#include "app_locale.h"
#include "sg_internal.h"
#include "sql_control.h"
#include "struct_dynamic.h"
#include "compiler.h"
#include "dblog.h"
#include "sg_api.h"

namespace ai
{
namespace app
{

using namespace ai::sg;
using namespace ai::scci;

int const SA_RELEASE = 1;
int const MAX_LEN = 255;
int const GLOBAL_SVC_KEY_SERIAL = 0;
int const GLOBAL_FILE_NAME_SERIAL = 1;
int const GLOBAL_FILE_SN_SERIAL = 2;
int const GLOBAL_REDO_MARK_SERIAL = 3;
int const GLOBAL_SYSDATE_SERIAL = 4;
int const GLOBAL_FIRST_SERIAL = 5;
int const GLOBAL_SECOND_SERIAL = 6;
int const FIXED_GLOBAL_SIZE = 7;

int const INTEGRATE_NAME_LEN = 15;
int const RAW_FILE_NAME_LEN = 255;
int const FILE_NAME_LEN = 255;
int const FILE_SN_LEN = 10;
int const REDO_MARK_LEN = 10;
int const SYSDATE_LEN = 14;
int const SERIAL_LEN = 10;
int const COM_KEY_LEN = 15;
int const SVC_KEY_LEN = 15;
int const MAX_RECORD_LEN = 4000;

int const RECORD_SN_LEN = 10;
int const INPUT_RECORD_SN_SERIAL = 0;
int const FIXED_INPUT_SIZE = 1;
int const FIXED_OUTPUT_SIZE = 1;

int const ERROR_CODE_LEN = 6;
int const ROWID_LEN = 18;

int const OTYPE_INVALID = 0x1;	// ��Ч���
int const OTYPE_ERROR = 0x2;	// �������
int const OTYPE_TARGET = 0x4;	// Ŀ�����
int const OTYPE_SUMMARY = 0x8;	// ���ܶ���
int const OTYPE_DISTR = 0x10;	// �ַ����
int const OTYPE_STAT = 0x20;	// ͳ�ƶ���

int const SA_METAMSG = 0x1;	// ����ΪԪ����

enum field_type_enum
{
	FIELD_TYPE_INT = 0,
	FIELD_TYPE_LONG = 1,
	FIELD_TYPE_FLOAT = 2,
	FIELD_TYPE_DOUBLE = 3,
	FIELD_TYPE_STRING = 4
};

enum align_mode_enum
{
	ALIGN_MODE_LEFT = 0,
	ALIGN_MODE_INTERNAL = 1,
	ALIGN_MODE_RIGHT = 2
};

enum encode_type_enum
{
	ENCODE_NONE = -1,
	ENCODE_ASCII = 0,
	ENCODE_INTEGER = 1,
	ENCODE_OCTETSTRING = 2,
	ENCODE_BCD = 3,
	ENCODE_TBCD = 4,
	ENCODE_ASN = 5,
	ENCODE_ADDRSTRING = 6,
	ENCODE_SEQUENCEOF = 7,
	ENCODE_REALBCD = 8,
	ENCODE_REALTBCD = 9,
	ENCODE_BCDD = 10,
	ENCODE_UNINTEGER = 11
};

enum record_type_enum
{
	RECORD_TYPE_HEAD = 0,
	RECORD_TYPE_BODY = 1,
	RECORD_TYPE_TAIL = 2
};

enum action_type_enum
{
	ACTION_NONE = 0,	// Do nothing.
	ADD_INT = 1,		// Add two int values.
	ADD_LONG = 2,		// Add two long values.
	ADD_FLOAT = 3,		// Add two float values.
	ADD_DOUBLE = 4,		// Add two double values.
	INC_INT = 5,		// Increase automatically for every output record, it's an int value.
	MIN_INT = 6,		// Get min value of two int values.
	MIN_LONG = 7,		// Get min value of two long values.
	MIN_FLOAT = 8,		// Get min value of two float values.
	MIN_DOUBLE = 9,		// Get min value of two double values.
	MIN_STR = 10,		// Get min value of two string values.
	MAX_INT = 11,		// Get max value of two int values.
	MAX_LONG = 12,		// Get max value of two long values.
	MAX_FLOAT = 13,		// Get max value of two float values.
	MAX_DOUBLE = 14,	// Get max value of two double values.
	MAX_STR = 15,		// Get max value of two string values.
	SPLIT_FIELD = 16 	// This record should be split
};

enum field_orign_enum
{
	FIELD_ORIGN_INPUT = 0,
	FIELD_ORIGN_GLOBAL = 1,
	FIELD_ORIGN_OUTPUT = 2
};

int const SA_OPTION_INTERNAL = 0x1;		// �ڲ���ʽ
int const SA_OPTION_SAME_FILE = 0x2;	// ����д��ͬһ�ļ�
int const SA_OPTION_SPLIT = 0x4;		// ��Ҫ����ļ�

int const SA_OPTION_FROM_INPUT = 0x1;	// ������Դ����һ���������
int const SA_OPTION_FROM_OUTPUT = 0x2;	// ������Դ����һ��������

int const SUMMARY_ACTION_ASSIGN = 0x1;
int const SUMMARY_ACTION_MIN = 0x2;
int const SUMMARY_ACTION_MAX = 0x3;
int const SUMMARY_ACTION_ADD = 0x8;

int const STAT_ACTION_INSERT = 0x1;
int const STAT_ACTION_UPDATE = 0x2;

int const SA_SUCCESS = 0;
int const SA_INVALID = 1;
int const SA_ERROR = 2;
int const SA_SKIP = 3;
int const SA_FATAL = 4;
int const SA_EOF = 5;

int const EXCEPTION_POLICY_RETRY = 0x1;	// try after a little while
int const EXCEPTION_POLICY_DONE = 0x2;	// done this file
int const EXCEPTION_POLICY_EXIT = 0x4;	// exit process

int const DEL_BY_FILE_NAME = 0x1;
int const DEL_BY_FILE_SN = 0x2;

int const ROLLBACK_BY_NONE = 0;
int const ROLLBACK_BY_TRUNCATE = 1;
int const ROLLBACK_BY_DELETE = 2;
int const ROLLBACK_BY_SCRIPT = 3;

typedef std::map<std::string, int> field_map_t;

struct sa_resource_t
{
	std::string integrator_name;	// integrator name
	std::string dbname;			// database name to save log
	std::string openinfo;		// information to open database
	std::string libs;			// libraries to load dynamically
	std::string sginfo;			// information to login on ServiceGalaxy
	std::string dbcinfo;			// information to login on DBC
};

// Global definition.
class global_field_t;
struct sa_global_t
{
	std::vector<global_field_t> global_fields;	// ȫ�ֱ�������
	field_map_t global_map;				// ȫ�ֱ������±�Ķ�Ӧ��ϵ
	std::vector<int> input_globals;			// ��Ϊ���봫�ݵ�ȫ�ֱ����б�
	std::vector<int> output_globals;			// ��Ϊ������ݵ�ȫ�ֱ����б�
};

struct sa_parms_t
{
	std::string com_key;
	std::string svc_key;
	int version;
	int batch;
	int concurrency;
	bool disable_global;
	int options;
	int exception_policy;
	int exception_waits;
};

struct sa_source_t
{
	std::string class_name;			// The class to handle source records
	int per_records;				// Maximum records dealed once a time.
	int per_report;				// how many time to report once.
	int max_error_records;			// Maximum error records allowed in a file.
	std::map<std::string, std::string> args;	// argument map.
};

struct sa_target_t
{
	std::string class_name;						// The class to handle target records
	std::map<std::string, std::string> args;	// argument map.
};

struct sa_invalid_t
{
	std::string class_name;						// The class to handle invalid records
	std::map<std::string, std::string> args;	// argument map.
};

struct sa_error_t
{
	std::string class_name;						// The class to handle error records
	std::map<std::string, std::string> args;	// argument map.
};

struct global_field_t
{
	std::string field_name;			// Field name in string, and it can be used in expression.
	std::string default_value;		// This is the initial value of above variable in string.
	int field_size;					// The field length. It's assumed not exceed this limitation.
	bool readonly;					// The field value can't be changed.
};

struct input_field_t
{
	int field_serial;
	int factor1;					// In fixed mode this means field offset, otherwise max allow length.
	int factor2;					// In fixed mode this means field length, otherwise max keep length.
	encode_type_enum encode_type;	// In Asn1 mode this means the filed's encode_type such as BCD, IA5STRING etc.
	int parent_serial;				// parent field_serial
	std::string field_name;			// Field name of formatting table.name
	std::string column_name;		// Field name corresponding to error table.
	bool inv_flag;					//�Ƿ�����Ч�ļ������

	bool operator<(const input_field_t& rhs) const {
		if (factor1 < rhs.factor1)
			return true;
		else if (factor1 > rhs.factor1)
			return false;

		if (parent_serial < rhs.parent_serial)
			return true;

		return false;
	}
};

struct input_record_t
{
	record_type_enum record_type;				// ��¼����
	char delimiter;								// �ָ���
	std::string rule_condition;					// �ж�����
	std::vector<input_field_t> field_vector;	// The vector of source fields.
	std::set<input_field_t> field_set;			//  factor1 map for ans.1
};

struct output_field_t
{
	std::string field_name;
	int field_size;
	std::string column_name;
};

struct sa_summary_field_t
{
	field_orign_enum field_orign;	// orign of field
	std::string field_name;		// field name in global/output definition
	std::string column_name;	// column name in database
	int field_serial;			// field index in global/output
	field_type_enum field_type;
	int field_size;
	int action;
};

struct sa_summary_record_t
{
	std::string table_name;
	std::vector<sa_summary_field_t> keys;
	std::vector<sa_summary_field_t> values;
	int rollback_type;
	std::string rollback_script;
	int del_flag;
};

struct sa_summary_t
{
	std::string class_name;		// The class to handle summary records.
	std::vector<sa_summary_record_t> records;
};

struct dst_field_t
{
	field_orign_enum field_orign;		// field_orign of field
	int field_serial;			// field index in global/output
	field_type_enum field_type;
	align_mode_enum align_mode;
	int field_size;
	char fill;
	int precision;
	action_type_enum action;
};

struct dst_record_t
{
	char delimiter;		//The delimiter of fields in record.
	bool has_return;	//Whether add a return at the end of record.
	bool is_fixed;		//Whether the output record is fixed.
	std::vector<dst_field_t> fields;
};

struct dst_file_t
{
	std::string rule_condition;		// Judge if the record should output to this file.
	std::string rule_file_name;		// Output file name rule.
	std::string dst_dir;				//The output directory of destination file.
	std::string rdst_dir;			// The remote output directory of destination file.
	int options;					// options
	dst_record_t head;				//Head record definition to output.
	dst_record_t body;				//Body record definition to output.
	dst_record_t tail;				//Tail record definition to output.
	std::string sftp_prefix;
	std::string pattern;
};

struct sa_distribute_t
{
	std::string class_name;		// The class to handle distribute records
	int max_open_files;			// The maximum number of file descriptors.
	std::vector<dst_file_t> dst_files;	// �ַ��ļ�����
};

struct sa_stat_field_t
{
	std::string element_name;	// field name in table
	int field_size;				// maximum field size
	field_orign_enum field_orign;	// orign of field
	int field_serial;			// field index in global/output
	std::string insert_desc;	// part of insert statement
	std::string update_desc;	// part of update statement
	std::string operation;		// operation in where clause
	int insert_times;			// occurrence to bind insert statement
	int update_times;			// occurrence to bind update statement
};

struct sa_stat_record_t
{
	int stat_action;
	bool sort;
	std::string rule_condition;
	std::string rule_table_name;
	std::string table_name;
	int rollback_type;
	std::string rollback_script;
	int del_flag;
	bool same_struct;			// insert/update structs are the same
	std::vector<sa_stat_field_t> keys;
	std::vector<sa_stat_field_t> values;
};

struct sa_stat_t
{
	std::string class_name;		// The class to handle stat records
	std::vector<sa_stat_record_t> records;
};

struct sa_adaptor_t
{
	sa_parms_t parms;							// ͨ�ò���
	sa_source_t source;						// ������ز���
	sa_target_t target;							// Ŀ�������ز���
	sa_invalid_t invalid;						// ��Ч��ز���
	sa_error_t error;							// ������ز���

	// Global definition.
	field_map_t global_map;					// ȫ�ֱ������±�Ķ�Ӧ��ϵ
	std::vector<int> input_globals;				// ��Ϊ���봫�ݵ�ȫ�ֱ����б�
	std::vector<int> output_globals;				// ��Ϊ������ݵ�ȫ�ֱ����б�

	// Input definition.
	std::vector<input_record_t> input_records;		// �����������
	std::vector<field_map_t> input_maps;			// ����������±�Ķ�Ӧ��ϵ
	std::vector<int> input_sizes;					// ���������������

	// Output definition.
	std::vector<output_field_t> output_fields;		// �����������
	field_map_t output_map;					// ����������±�Ķ�Ӧ��ϵ

	// Summary definition.
	sa_summary_t summary;					// ժҪ��ض���

	// Distribute definition.
	sa_distribute_t distr;						// �ַ���ض���

	// Stat definition.
	sa_stat_t stat;								// ͳ����ض���

	// Component specific arguments.
	std::map<string, string> args;				// �����صĲ���
};

/* ���ݲ��ְ�������˳��:
 * 1. ȫ�ֱ���ƫ��������
 * 2. ȫ�ֱ�������
 * 3. �������ƫ��������
 * 4. �����������
 * 6. 3-4��ѭ����Σ���ʾ���������¼
 */
struct sa_rqst_t
{
	char svc_key[SVC_KEY_LEN + 1];	// ҵ��ؼ���
	int version;					// ҵ��汾
	int flags;						// ��ʶ
	int datalen;					// ���󳤶�
	int rows;						// �����¼��
	short global_size;				// ȫ�ֱ�������
	short input_size;				// �����������
	int64_t user_id;				// �û�ID
	int placeholder;				// ����ռλ��

	static size_t need(int _rows, int _global_size, int _global_dsize, int _input_size, int _input_dsize) {
		return sizeof(sa_rqst_t) - sizeof(int)
			+ common::round_up(sizeof(int) * _global_size + _global_dsize, sizeof(int))
			+ common::round_up(sizeof(int) * (1 + _input_size) + _input_dsize, sizeof(int)) * _rows;
	}
};

/* ���ݲ��ְ�������˳��:
 * 1. ����������ֵ����
 * 2. ȫ�ֱ���ƫ��������
 * 3. ȫ�ֱ�������
 * 4. �������ƫ��������
 * 5. �����������
 * 6. 4-5��ѭ����Σ���ʾ���������¼
 */
struct sa_rply_t
{
	int datalen;					// Ӧ�𳤶�
	int rows;						// Ӧ���¼��
	short global_size;				// ȫ�ֱ�������
	short output_size;				// �����������
	int placeholder;				// ����ռλ��

	static size_t need(int _rows, int _global_size, int _global_dsize, int _output_size, int _output_dsize) {
		return sizeof(sa_rply_t) - sizeof(int)
			+ common::round_up(sizeof(int) * _global_size + _global_dsize, sizeof(int))
			+ common::round_up(sizeof(int) * (1 + _output_size) + _output_dsize, sizeof(int)) * _rows;
	}
};

class sa_base;
class sa_input;
class sa_output;
class dblog_item_t;
class sa_base_creator_t;
class sa_input_creator_t;
class sa_output_creator_t;

}
}

#endif

