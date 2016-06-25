#if !defined(__PRSP_CTX_H__)
#define __PRSP_CTX_H__

#include "machine.h"
#include "user_exception.h"
#include "sg_internal.h"
#include "svcp_ctx.h"

using namespace ai::sg;
using namespace std;

namespace ai
{
namespace app
{

struct bps_global_field_t
{
	string field_name;
	int field_size;
	bool readonly;
};

struct bps_input_field_t
{
	string field_name;
	int field_size;
};

struct bps_input_table_t
{
	string name;
	vector<bps_input_field_t> fields;
};

struct bps_output_field_t
{
	string field_name;
	int field_size;
};

struct bps_output_table_t
{
	string name;
	vector<bps_output_field_t> fields;
};

struct bps_config_t
{
	svc_parms_t parms;
	vector<bps_global_field_t> global;
	vector<bps_input_table_t> input;
	vector<bps_output_table_t> output;
	vector<string> processes;
};

// 支持的原子类型
enum parser_atomic_type
{
	PARSER_TYPE_CHAR,
	PARSER_TYPE_SHORT,
	PARSER_TYPE_INT,
	PARSER_TYPE_LONG,
	PARSER_TYPE_FLOAT,
	PARSER_TYPE_DOUBLE,
	PARSER_TYPE_TIME_T,
	PARSER_TYPE_UNKNOWN
};

class parser_raw_type
{
public:
	parser_raw_type()
	{
		line_no = 0;
		column = 0;
	}

	~parser_raw_type()
	{
	}

	void set_line_no(int _line_no)
	{
		line_no = _line_no;
	}

	int get_line_no() const
	{
		return line_no;
	}

	void set_column(int _column)
	{
		column = _column;
	}

	int get_column() const
	{
		return column;
	}

	void set_is_text(bool _is_text)
	{
		is_text = _is_text;
	}

	bool get_is_text() const
	{
		return is_text;
	}

	void set_is_unsigned(bool _is_unsigned)
	{
		is_unsigned = _is_unsigned;
	}

	bool get_is_unsigned() const
	{
		return is_unsigned;
	}

	void set_atomic_type(parser_atomic_type _atomic_type)
	{
		atomic_type = _atomic_type;
	}

	parser_atomic_type get_atomic_type() const
	{
		return atomic_type;
	}

	void set_pointers(int _pointers)
	{
		pointers = _pointers;
	}

	int get_pointers() const
	{
		return pointers;
	}

	bool is_string() const
	{
		return (atomic_type == PARSER_TYPE_CHAR && pointers == 1);
	}

	string get_name() const
	{
		string name;

		if (is_unsigned)
			name = "unsigned ";

		switch (atomic_type) {
		case PARSER_TYPE_CHAR:
			name += "char";
			break;
		case PARSER_TYPE_SHORT:
			name += "short";
			break;
		case PARSER_TYPE_INT:
			name += "int";
			break;
		case PARSER_TYPE_LONG:
			name += "long";
			break;
		case PARSER_TYPE_FLOAT:
			name += "float";
			break;
		case PARSER_TYPE_DOUBLE:
			name += "double";
			break;
		case PARSER_TYPE_TIME_T:
			name += "time_t";
			break;
		default:
			name += "";
			break;
		}

		if (pointers > 0)
			name += " ";

		for (int i = 0; i < pointers; i++)
			name += "*";

		return name;
	}

	string get_partname() const
	{
		string name;

		if (pointers > 0)
			throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: internal error")).str(APPLOCALE));

		if (is_unsigned)
			name = "u";

		switch (atomic_type) {
		case PARSER_TYPE_CHAR:
			name += "char";
			break;
		case PARSER_TYPE_SHORT:
			name += "short";
			break;
		case PARSER_TYPE_INT:
			name += "int";
			break;
		case PARSER_TYPE_LONG:
			name += "long";
			break;
		case PARSER_TYPE_FLOAT:
			name += "float";
			break;
		case PARSER_TYPE_DOUBLE:
			name += "double";
			break;
		case PARSER_TYPE_TIME_T:
			name += "time_t";
			break;
		default:
			name += "";
			break;
		}

		return name;
	}

	bool operator==(const parser_raw_type& rhs) const
	{
		return is_unsigned == rhs.is_unsigned
			&& atomic_type == rhs.atomic_type
			&& pointers == rhs.pointers;
	}

private:
	int line_no;
	int column;
	bool is_text;
	bool is_unsigned;
	parser_atomic_type atomic_type;
	int pointers;
};

class parser_stmts_t;

struct prsp_ctx
{
public:
	static prsp_ctx * instance();
	~prsp_ctx();

	parser_stmts_t * _PRSP_stmts;
	string _PRSP_input_name;
	vector<string> _PRSP_output_names;
	map<string, parser_raw_type> _PRSP_input_types;
	vector<map<string, parser_raw_type> > _PRSP_output_types;
	map<string, parser_raw_type> _PRSP_temp_types;
	map<string, parser_raw_type> _PRSP_func_types;

private:
	prsp_ctx();

	static void init_once();

	static boost::once_flag once_flag;
	static prsp_ctx *_instance;
};

}
}

#endif

