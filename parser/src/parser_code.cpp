#include "parser_code.h"
#include "app_locale.h"
#include "sg_internal.h"
#include <boost/scope_exit.hpp>
#include <boost/foreach.hpp>

using namespace ai::sg;
using namespace ai::app;
#include "parser.cpp.h"

namespace ai
{
namespace app
{

parser_base::parser_base()
{
	PRSP = prsp_ctx::instance();
}

parser_base::~parser_base()
{
	BOOST_FOREACH(parser_string_t *str, strs) {
		delete str;
	}
}

void parser_base::set_prefix(const string& _prefix)
{
	prefix = _prefix;
}

const string& parser_base::get_prefix() const
{
	return prefix;
}

void parser_base::push_str(parser_string_t *str)
{
	strs.push_back(str);
}

const parser_raw_type& parser_base::get_raw_type() const
{
	return raw_type;
}

void parser_base::set_line_no(int _line_no)
{
	raw_type.set_line_no(_line_no);
}

int parser_base::get_line_no() const
{
	return raw_type.get_line_no();
}

void parser_base::set_column(int _column)
{
	raw_type.set_column(_column);
}

int parser_base::get_column() const
{
	return raw_type.get_column();
}

void parser_base::set_is_text(bool _is_text)
{
	raw_type.set_is_text(_is_text);
}

bool parser_base::get_is_text() const
{
	return raw_type.get_is_text();
}

void parser_base::set_is_unsigned(bool _is_unsigned)
{
	raw_type.set_is_unsigned(_is_unsigned);
}

bool parser_base::get_is_unsigned() const
{
	return raw_type.get_is_unsigned();
}

void parser_base::set_atomic_type(parser_atomic_type _atomic_type)
{
	return raw_type.set_atomic_type(_atomic_type);
}

parser_atomic_type parser_base::get_atomic_type() const
{
	return raw_type.get_atomic_type();
}

void parser_base::set_pointers(int _pointers)
{
	return raw_type.set_pointers(_pointers);
}

int parser_base::get_pointers() const
{
	return raw_type.get_pointers();
}

void parser_base::preparse(int flags)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(300, __PRETTY_FUNCTION__, (_("flags={1}") % flags).str(APPLOCALE), NULL);
#endif
}

string parser_base::insert_str(const string& src, const string& str)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(300, __PRETTY_FUNCTION__, "", NULL);
#endif
	string result;

	for (int i = 0; i < src.length(); i++) {
		char ch = src[i];

		if (isspace(ch)) {
			result += ch;
			continue;
		}

		if (ch == '/') {
			if (i + 1 == src.length()) {
				result += ch;
				break;
			}

			char ch2 = src[i + 1];
			if (ch2 == '/') {
				result += ch;
				result += ch2;

				for (i += 2; i < src.length(); i++) {
					ch = src[i];
					result += ch;

					if (ch == '\n')
						break;
				}

				continue;
			} else if (ch2 == '*') {
				result += ch;
				result += ch2;

				for (i += 2; i < src.length(); i++) {
					ch = src[i];
					result += ch;

					if (ch == '*') {
						if (i + 1 == src.length())
							break;

						ch2 = src[++i];
						result += ch2;

						if (ch2 == '/')
							break;
					}
				}

				continue;
			}
		}

		result += str;
		result += string(src.begin() + i, src.begin() + src.length());
		break;
	}

	return result;
}

string parser_base::convert(const parser_raw_type *dst_type, const parser_raw_type *src_type, const string& value)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(300, __PRETTY_FUNCTION__, "", NULL);
#endif

	if (dst_type == NULL)
		return value;

	string code;
	string code_prefix;
	string code_suffix;
	bool has_space = false;

	BOOST_FOREACH(const char& ch, value) {
		if (isspace(ch))
			has_space = true;
	}

	if (src_type->get_pointers() == 0) {	// 源不是指针
		if (dst_type->get_pointers() == 0) {	// 目标不是指针
			if (src_type->get_atomic_type() != dst_type->get_atomic_type()) {
				// 类型不一致，需要强制类型转换
				code_prefix = "(";
				code_prefix += dst_type->get_name();
				code_prefix += ")";
				if (has_space) {
					code_prefix += "(";
					code_suffix = ")";
				}
			}
		} else {	// 目标是指针
			if (dst_type->is_string()) {	// 目标为字符串
				if (src_type->get_is_text()) {	// 如果源为文本
					code_prefix = "\"";
					code_suffix = "\"";
				} else {	// 调用函数转换
					code_prefix = "ai_app_";
					code_prefix += src_type->get_partname();
					code_prefix += "_to_string(";
					code_suffix = ")";
				}
			} else if (src_type->get_atomic_type() == dst_type->get_atomic_type()) {
				// 类型相同，需要转换成地址
				for (int i = 0; i < dst_type->get_pointers(); i++)
					code_prefix += "&";
				if (has_space) {
					code_prefix += "(";
					code_suffix = ")";
				}
			} else {
				throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Can't convert to {1} for token {2}") % dst_type->get_name() % value).str(APPLOCALE));
			}
		}
	} else if (dst_type->get_pointers() == 0) {	// 目标不是指针
		if (!src_type->is_string())
			throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: expect string for token {1}") % value).str(APPLOCALE));

		if (src_type->get_is_text()) {
			switch (dst_type->get_atomic_type()) {
			case PARSER_TYPE_CHAR:
			case PARSER_TYPE_SHORT:
			case PARSER_TYPE_INT:
			case PARSER_TYPE_LONG:
			case PARSER_TYPE_FLOAT:
			case PARSER_TYPE_DOUBLE:
			case PARSER_TYPE_TIME_T:
				if (src_type->get_atomic_type() != dst_type->get_atomic_type()) {
					code_prefix = "(";
					code_prefix += dst_type->get_partname();
					code_prefix += ")";
				}
				break;
			default:
				throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: internal error")).str(APPLOCALE));
			}
		} else {
			switch (dst_type->get_atomic_type()) {
			case PARSER_TYPE_CHAR:
			case PARSER_TYPE_SHORT:
				code_prefix = "(";
				code_prefix += dst_type->get_partname();
				code_prefix += ")atoi(";
				code_suffix = ")";
				break;
			case PARSER_TYPE_INT:
				code_prefix = "atoi(";
				code_suffix = ")";
				break;
			case PARSER_TYPE_LONG:
				code_prefix = "atol(";
				code_suffix = ")";
				break;
			case PARSER_TYPE_FLOAT:
				code_prefix = "(float)atof(";
				code_suffix = ")";
				break;
			case PARSER_TYPE_DOUBLE:
				code_prefix = "atof(";
				code_suffix = ")";
				break;
			case PARSER_TYPE_TIME_T:
				code_prefix = "ai_app_string_to_time_t(";
				code_suffix = ")";
				break;
			default:
				throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: internal error")).str(APPLOCALE));
			}
		}
	}else {	// 源和目标都是指针
		if (*src_type == *dst_type) {	// 类型一致
			if (src_type->get_is_text()) {	// 如果源为文本
				code_prefix = "\"";
				code_suffix = "\"";
			}
		} else {
			code_prefix = "(";
			code_prefix += dst_type->get_name();
			code_prefix += ")";
			if (has_space) {
				code_prefix += "(";
				code_suffix = ")";
			}
		}
	}

	code =  insert_str(value, code_prefix);
	code += code_suffix;
	return code;
}

parser_string_t::parser_string_t(const string& _value)
	: value(_value)
{
}

parser_string_t::~parser_string_t()
{
}

string parser_string_t::gen_code(const parser_raw_type *_raw_type)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(300, __PRETTY_FUNCTION__, (_("_raw_type={1}") % _raw_type).str(APPLOCALE), NULL);
#endif

	string code = prefix;
	code += convert(_raw_type, &raw_type, value);
	return code;
}

void parser_string_t::to_lower()
{
	value = boost::to_lower_copy(value);
}

const string& parser_string_t::get_value() const
{
	return value;
}

parser_item_t::parser_item_t(bool _is_unsigned, parser_atomic_type _atomic_type)
{
	set_is_unsigned(_is_unsigned);
	set_atomic_type(_atomic_type);
}

parser_item_t::~parser_item_t()
{
}

string parser_item_t::gen_code(const parser_raw_type *_raw_type)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(300, __PRETTY_FUNCTION__, (_("_raw_type={1}") % _raw_type).str(APPLOCALE), NULL);
#endif

	string code;
	int idx = 0;

	if (get_is_unsigned()) {
		code += strs[idx++]->get_prefix();
		code += "unsigned ";
	}

	switch (get_atomic_type()) {
	case PARSER_TYPE_CHAR:
		code += strs[idx]->get_prefix();
		code += "char";
		break;
	case PARSER_TYPE_SHORT:
		code += strs[idx]->get_prefix();
		code += "short";
		break;
	case PARSER_TYPE_INT:
		if (idx < strs.size())
			code += strs[idx]->get_prefix();
		code += "int";
		break;
	case PARSER_TYPE_LONG:
		code += strs[idx]->get_prefix();
		code += "long";
		break;
	case PARSER_TYPE_FLOAT:
		code += strs[idx]->get_prefix();
		code += "float";
		break;
	case PARSER_TYPE_DOUBLE:
		code += strs[idx]->get_prefix();
		code += "double";
		break;
	case PARSER_TYPE_TIME_T:
		code = strs[idx]->get_prefix();
		code += "time_t";
		break;
	default:
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Invalid atomic_type {1}") % get_atomic_type()).str(APPLOCALE));
	}

	return code;
}

parser_pointers_t::parser_pointers_t()
{
	set_pointers(0);
}

parser_pointers_t::~parser_pointers_t()
{
}

string parser_pointers_t::gen_code(const parser_raw_type *_raw_type)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(300, __PRETTY_FUNCTION__, (_("_raw_type={1}") % _raw_type).str(APPLOCALE), NULL);
#endif

	string code;

	BOOST_FOREACH(parser_string_t *str, strs) {
		code += str->get_prefix();
		code += "*";
	}

	return code;
}

parser_type_t::parser_type_t(parser_item_t *_item, parser_pointers_t *_pointers, parser_exprs_t *_exprs)
	: item(_item),
	  pointers(_pointers),
	  exprs(_exprs)
{
}

parser_type_t::~parser_type_t()
{
	delete exprs;
}

void parser_type_t::set_exprs(parser_exprs_t *_exprs)
{
	exprs = _exprs;
}

void parser_type_t::preparse(int flags)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(300, __PRETTY_FUNCTION__, (_("flags={1}") % flags).str(APPLOCALE), NULL);
#endif

	if (exprs)
		exprs->preparse(flags);

	set_is_unsigned(item->get_is_unsigned());
	set_atomic_type(item->get_atomic_type());

	int count;
	if (pointers)
		count = pointers->get_pointers();
	else
		count = 0;

	if (exprs)
		count -= exprs->get_exprs().size();

	set_pointers(count);
}

string parser_type_t::gen_code(const parser_raw_type *_raw_type)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(300, __PRETTY_FUNCTION__, (_("_raw_type={1}") % _raw_type).str(APPLOCALE), NULL);
#endif

	string code = item->gen_code();

	if (pointers)
		code += pointers->gen_code();

	return code;
}

string parser_type_t::gen_code2()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(300, __PRETTY_FUNCTION__, "", NULL);
#endif

	if (exprs == NULL)
		return "";

	return exprs->gen_code();
}

parser_variable_t::parser_variable_t(parser_string_t *_direction, parser_string_t *_table, parser_string_t *_variable)
	: direction(_direction),
	  table(_table),
	  variable(_variable)
{
	if (direction)
		direction->to_lower();

	if (table) {
		table->to_lower();
		variable->to_lower();
	}
}

parser_variable_t::~parser_variable_t()
{
	delete direction;
	delete table;
	delete variable;
}

void parser_variable_t::preparse(int flags)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(300, __PRETTY_FUNCTION__, (_("flags={1}") % flags).str(APPLOCALE), NULL);
#endif

	if (flags == 1) {
		if (table) {
			code_prefix = table->get_prefix();
			code_prefix += strs[0]->get_prefix();
		}
		return;
	}

	const string& variable_value = variable->get_value();
	const string& input_name = PRSP->_PRSP_input_name;
	const vector<string>& output_names = PRSP->_PRSP_output_names;
	const map<string, parser_raw_type>& input_types = PRSP->_PRSP_input_types;
	const vector<map<string, parser_raw_type> >& output_types = PRSP->_PRSP_output_types;
	const map<string, parser_raw_type>& temp_types = PRSP->_PRSP_temp_types;
	map<string, parser_raw_type>::const_iterator iter;

	// 查找字段
	if (direction) {	// 指定了方向
		const string& direction_value = direction->get_value();
		const string& table_value = table->get_value();

		if (direction_value == "input") {
			if (input_name != table_value)
				throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: {1} is not an input table") % table_value).str(APPLOCALE));

			iter = input_types.find(variable_value);
			if (iter == input_types.end())
				throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: variable {1}.{2} not found in input table") % table_value % variable_value).str(APPLOCALE));

			code_prefix = direction->get_prefix();
			code_prefix += table->get_prefix();
			code_prefix += strs[0]->get_prefix();
			code_prefix += strs[1]->get_prefix();
			code_prefix += "$";
			code_prefix += table_value;
			code_prefix += ".";

			raw_type = iter->second;
		} else if (direction_value == "output") {
			for (int i = 0; i < output_names.size(); i++) {
				if (output_names[i] == table_value) {
					const map<string, parser_raw_type>& output_type = output_types[i];

					iter = output_type.find(variable_value);
					if (iter == output_type.end())
						throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: variable {1}.{2} not found in output tables") % table_value % variable_value).str(APPLOCALE));
					break;
				}
			}

			code_prefix = direction->get_prefix();
			code_prefix += table->get_prefix();
			code_prefix += strs[0]->get_prefix();
			code_prefix += strs[1]->get_prefix();
			code_prefix += "#";
			code_prefix += table_value;
			code_prefix += ".";

			raw_type = iter->second;
		} else {
			throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Only input/ouput is allowed for direction {1}") % direction_value).str(APPLOCALE));
		}
	} else if (table) {
		const string& table_value = table->get_value();
		int found = 0;

		if (table_value == "temporary") {
			iter = temp_types.find(variable_value);
			if (iter != temp_types.end()) {
				found++;
				code_prefix = table->get_prefix();
				code_prefix += strs[0]->get_prefix();
				raw_type = iter->second;
				return;
			}
		}

		if (input_name == table_value) {
			iter = input_types.find(variable_value);
			if (iter != input_types.end()) {
				found++;
				code_prefix = table->get_prefix();
				code_prefix += strs[0]->get_prefix();
				code_prefix += "$";
				code_prefix += table_value;
				code_prefix += ".";
				raw_type = iter->second;
			}
		}

		for (int i = 0; i < output_names.size(); i++) {
			if (output_names[i] == table_value) {
				const map<string, parser_raw_type>& output_type = output_types[i];

				iter = output_type.find(variable_value);
				if (iter != output_type.end()) {
					found++;
					code_prefix = table->get_prefix();
					code_prefix += strs[0]->get_prefix();
					code_prefix += "#";
					code_prefix += table_value;
					code_prefix += ".";
					raw_type = iter->second;
				}
				break;
			}
		}

		if (found == 0)
			throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: variable {1}.{2} not found in input/output tables") % table_value % variable_value).str(APPLOCALE));
		else if (found > 1)
			throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: variable {1}.{2} conflict both in input and output tables") % table_value % variable_value).str(APPLOCALE));
	} else {
		int found = 0;

		iter = temp_types.find(variable_value);
		if (iter != temp_types.end()) {
			found++;
			raw_type = iter->second;
		}

		iter = input_types.find(variable_value);
		if (iter != input_types.end()) {
			found++;
			code_prefix = "$";
			code_prefix += input_name;
			code_prefix += ".";
			raw_type = iter->second;
		}

		for (int i = 0; i < output_names.size(); i++) {
			const map<string, parser_raw_type>& output_type = output_types[i];

			iter = output_type.find(variable_value);
			if (iter != output_type.end()) {
				found++;
				code_prefix = "#";
				code_prefix += output_names[i];
				code_prefix += ".";
				raw_type = iter->second;
			}
		}

		if (found == 0) {
			raw_type.set_is_text(false);
			raw_type.set_is_unsigned(false);
			raw_type.set_atomic_type(PARSER_TYPE_UNKNOWN);
			raw_type.set_pointers(0);
		} else if (found > 1) {
			throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: variable {1} conflict in input, output and temporary tables") % variable_value).str(APPLOCALE));
		}
	}
}

string parser_variable_t::gen_code(const parser_raw_type *_raw_type)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(300, __PRETTY_FUNCTION__, (_("_raw_type={1}") % _raw_type).str(APPLOCALE), NULL);
#endif

	string code = code_prefix;
	code += variable->gen_code();
	return convert(_raw_type, &raw_type, code);
}

string parser_variable_t::get_value() const
{
	return variable->get_value();
}

parser_args_t::parser_args_t()
{
}

parser_args_t::~parser_args_t()
{
	BOOST_FOREACH(parser_expr_t *arg, args) {
		delete arg;
	}
}

void parser_args_t::push_front(parser_expr_t *arg)
{
	args.push_front(arg);
}

void parser_args_t::preparse(int flags)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(300, __PRETTY_FUNCTION__, (_("flags={1}") % flags).str(APPLOCALE), NULL);
#endif

	BOOST_FOREACH(parser_expr_t *arg, args) {
		arg->preparse(flags);
	}
}

string parser_args_t::gen_code(const parser_raw_type *_raw_type)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(300, __PRETTY_FUNCTION__, (_("_raw_type={1}") % _raw_type).str(APPLOCALE), NULL);
#endif

	bool first = true;
	string code;
	int idx = strs.size();

	BOOST_FOREACH(parser_expr_t *arg, args) {
		if (first) {
			first = false;
		} else {
			code += strs[--idx]->get_prefix();
			code += ",";
		}

		code += arg->gen_code();
	}

	return code;
}

parser_expr_t::parser_expr_t(parser_string_t *_value)
	: value(_value),
	  variable(NULL),
	  left(NULL),
	  op(0),
	  right(NULL),
	  ident(NULL),
	  args(NULL),
	  exprs(NULL),
	  type(NULL)
{
	expr_type = 0;
}

parser_expr_t::parser_expr_t(parser_variable_t *_variable)
	: value(NULL),
	  variable(_variable),
	  left(NULL),
	  op(0),
	  right(NULL),
	  ident(NULL),
	  args(NULL),
	  exprs(NULL),
	  type(NULL)
{
	expr_type = 1;
}

parser_expr_t::parser_expr_t(parser_variable_t *_variable, int _op, parser_expr_t *_right)
	: value(NULL),
	  variable(_variable),
	  left(NULL),
	  op(_op),
	  right(_right),
	  ident(NULL),
	  args(NULL),
	  exprs(NULL),
	  type(NULL)
{
	expr_type = 2;
}

parser_expr_t::parser_expr_t(int _op, parser_expr_t *_right)
	: value(NULL),
	  variable(NULL),
	  left(NULL),
	  op(_op),
	  right(_right),
	  ident(NULL),
	  args(NULL),
	  exprs(NULL),
	  type(NULL)
{
	expr_type = 3;
}

parser_expr_t::parser_expr_t(parser_expr_t *_left, int _op)
	: value(NULL),
	  variable(NULL),
	  left(_left),
	  op(_op),
	  right(NULL),
	  ident(NULL),
	  args(NULL),
	  exprs(NULL),
	  type(NULL)
{
	expr_type = 4;
}

parser_expr_t::parser_expr_t(parser_expr_t *_left, int _op, parser_expr_t *_right)
	: value(NULL),
	  variable(NULL),
	  left(_left),
	  op(_op),
	  right(_right),
	  ident(NULL),
	  args(NULL),
	  exprs(NULL),
	  type(NULL)
{
	expr_type = 5;
}

parser_expr_t::parser_expr_t(parser_string_t *_ident, parser_args_t *_args)
	: value(NULL),
	  variable(NULL),
	  left(NULL),
	  op(0),
	  right(NULL),
	  ident(_ident),
	  args(_args),
	  exprs(NULL),
	  type(NULL)
{
	expr_type = 6;
}

parser_expr_t::parser_expr_t(parser_expr_t *_left, parser_exprs_t *_exprs)
	: value(NULL),
	  variable(NULL),
	  left(_left),
	  op(0),
	  right(NULL),
	  ident(NULL),
	  args(NULL),
	  exprs(_exprs),
	  type(NULL)
{
	expr_type = 7;
}

parser_expr_t::parser_expr_t(parser_type_t *_type, parser_expr_t *_right)
	: value(NULL),
	  variable(NULL),
	  left(NULL),
	  op(-1),
	  right(_right),
	  ident(NULL),
	  args(NULL),
	  exprs(NULL),
	  type(_type)
{
	expr_type = 8;
}

parser_expr_t::~parser_expr_t()
{
	delete value;
	delete variable;
	delete left;
	delete right;
	delete ident;
	delete args;
	delete exprs;
	delete type;
}

void parser_expr_t::preparse(int flags)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(300, __PRETTY_FUNCTION__, (_("flags={1}") % flags).str(APPLOCALE), NULL);
#endif

	map<string, parser_raw_type>::const_iterator iter;

	if (value)
		value->preparse(flags);

	if (variable)
		variable->preparse(flags);

	if (left)
		left->preparse(flags);

	if (right)
		right->preparse(flags);

	if (ident)
		ident->preparse(flags);

	if (args)
		args->preparse(flags);

	if (exprs)
		exprs->preparse(flags);

	if (type)
		type->preparse(flags);

	switch (expr_type) {
	case 0:
		raw_type = value->get_raw_type();
		break;
	case 1:
		raw_type = variable->get_raw_type();
		break;
	case 2:
		set_type(&variable->get_raw_type(), &right->get_raw_type());
		set_ctype(&variable->get_raw_type(), &right->get_raw_type());
		break;
	case 3:
		set_type(NULL, &right->get_raw_type());
		set_ctype(NULL, &right->get_raw_type());
		break;
	case 4:
		set_type(&left->get_raw_type(), NULL);
		set_ctype(&left->get_raw_type(), NULL);
		break;
	case 5:
		set_type(&left->get_raw_type(), &right->get_raw_type());
		set_ctype(&left->get_raw_type(), &right->get_raw_type());
		break;
	case 6:
		{
			const string func_name = ident->get_value();
			iter = PRSP->_PRSP_func_types.find(func_name);
			if (iter != PRSP->_PRSP_func_types.end()) {
				raw_type = iter->second;
			} else {
				raw_type.set_is_text(false);
				raw_type.set_is_unsigned(false);
				if (memcmp(func_name.c_str(), "get_", 4) == 0)
					raw_type.set_atomic_type(PARSER_TYPE_INT);
				else
					raw_type.set_atomic_type(PARSER_TYPE_UNKNOWN);
				raw_type.set_pointers(0);
			}
		}
		break;
	case 7:
		raw_type = left->get_raw_type();
		raw_type.set_pointers(raw_type.get_pointers() - exprs->get_exprs().size());
		break;
	case 8:
		raw_type = type->get_raw_type();
		break;
	default:
		break;
	}
}

string parser_expr_t::gen_code(const parser_raw_type *_raw_type)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(300, __PRETTY_FUNCTION__, (_("_raw_type={1}") % _raw_type).str(APPLOCALE), NULL);
#endif

	string code;

	if (value) {	// 常量
		return value->gen_code(_raw_type);
	} else if (variable) {	// 变量
		return variable->gen_code(_raw_type);
	} else if (ident) {	// 函数
		code += ident->gen_code();
		code += strs[0]->get_prefix();
		code += "(";
		if (args)
			code += args->gen_code();
		code += strs[1]->get_prefix();
		code += ")";
	} else if (exprs) {	// 数组
		code += left->gen_code();
		code += exprs->gen_code();
	} else if (type) {	// 类型转换
		code += type->get_prefix();
		code += right->gen_code(&raw_type);
	} else {	// 操作
		switch (op) {
		case LCURVE:
			code = strs[0]->get_prefix();
			code += "(";
			code += right->gen_code();
			code += strs[1]->get_prefix();
			code += ")";
			break;
		case ADDRESS:
		case POINTER:
		case SIZEOF:
			code = strs[0]->get_prefix();
			code += strs[0]->get_value();
			code += right->gen_code();
			break;
		case ASSIGN:
			if (raw_type.is_string()) {
				string tmp_str = strs[0]->get_prefix();
				tmp_str += left->gen_code(&cltype);
				tmp_str += ", ";
				tmp_str += right->gen_code(&crtype);
				tmp_str += ")";
				code = insert_str(tmp_str, "strcpy(");
			} else {
				code = left->gen_code(&cltype);
				code += strs[0]->get_prefix();
				code += strs[0]->get_value();
				code += right->gen_code(&crtype);
			}
			break;
		default:
			if (left) {
				code = left->gen_code(&cltype);
				code += strs[0]->get_prefix();
				code += strs[0]->get_value();
				if (right)
					code += right->gen_code(&crtype);
			} else {
				code = strs[0]->get_prefix();
				code += strs[0]->get_value();
				code += right->gen_code(&crtype);
			}
			break;
		}
	}

	return convert(_raw_type, &raw_type, code);
}

void parser_expr_t::set_type(const parser_raw_type *ltype, const parser_raw_type *rtype)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(300, __PRETTY_FUNCTION__, (_("ltype={1}, rtype={2}") % ltype % rtype).str(APPLOCALE), NULL);
#endif

	switch (op) {
	case ASSIGN:
	case ADD_ASSIGN:
	case SUB_ASSIGN:
	case MUL_ASSIGN:
	case DIV_ASSIGN:
	case MOD_ASSIGN:
	case LSHIFT_ASSIGN:
	case RSHIFT_ASSIGN:
	case BITAND_ASSIGN:
	case BITXOR_ASSIGN:
	case BITOR_ASSIGN:
		raw_type = *ltype;
		break;
	case OR:
	case AND:
	case EQ:
	case NEQ:
	case LT:
	case LE:
	case GT:
	case GE:
		set_is_text(false);
		set_is_unsigned(false);
		set_atomic_type(PARSER_TYPE_INT);
		set_pointers(0);
		break;
	case BITOR:
	case BITAND:
	case LSHIFT:
	case RSHIFT:
		set_is_text(false);
		set_is_unsigned(false);
		set_pointers(0);

		switch (ltype->get_atomic_type()) {
		case PARSER_TYPE_CHAR:
		case PARSER_TYPE_SHORT:
		case PARSER_TYPE_INT:
			set_atomic_type(PARSER_TYPE_INT);
			break;
		case PARSER_TYPE_LONG:
		case PARSER_TYPE_TIME_T:
		case PARSER_TYPE_UNKNOWN:
			set_atomic_type(PARSER_TYPE_LONG);
			break;
		case PARSER_TYPE_FLOAT:
		case PARSER_TYPE_DOUBLE:
		default:
			throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Invalid field given")).str(APPLOCALE));
		}
		break;
	case ADD:
	case SUB:
		if (ltype->get_pointers() > 0 && rtype->get_pointers() == 0) {
			switch (rtype->get_atomic_type()) {
			case PARSER_TYPE_SHORT:
			case PARSER_TYPE_INT:
			case PARSER_TYPE_LONG:
				raw_type = *ltype;
				break;
			default:
				combine_type(*ltype, *rtype);
				break;
			}
		} else {
			combine_type(*ltype, *rtype);
		}
		break;
	case MUL:
	case DIV:
	case MOD:
		combine_type(*ltype, *rtype);
		break;
	case NOT:
		set_is_text(false);
		set_is_unsigned(false);
		set_atomic_type(PARSER_TYPE_INT);
		set_pointers(0);
		break;
	case BITNOT:
	case INC:
	case DEC:
	case UADD:
	case UMINUS:
	case LCURVE:
		if (left)
			raw_type = left->get_raw_type();
		else
			raw_type = right->get_raw_type();
		break;
	case POINTER:
		raw_type = right->get_raw_type();
		if (get_atomic_type() != PARSER_TYPE_UNKNOWN) {
			int pointers = get_pointers();
			if (pointers <= 0)
				throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: operation * is not allowed for expresssion")).str(APPLOCALE));
			raw_type.set_pointers(pointers - 1);
		}
		break;
	case ADDRESS:
		raw_type = right->get_raw_type();
		if (get_atomic_type() != PARSER_TYPE_UNKNOWN) {
			int pointers = get_pointers();
			raw_type.set_pointers(pointers + 1);
		}
		break;
	case SIZEOF:
		set_is_text(false);
		set_is_unsigned(true);
		set_atomic_type(PARSER_TYPE_LONG);
		set_pointers(0);
		break;
	default:
		set_is_text(false);
		set_is_unsigned(false);
		set_atomic_type(PARSER_TYPE_UNKNOWN);
		set_pointers(0);
		break;
	}
}

void parser_expr_t::set_ctype(const parser_raw_type *ltype, const parser_raw_type *rtype)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(300, __PRETTY_FUNCTION__, (_("ltype={1}, rtype={2}") % ltype % rtype).str(APPLOCALE), NULL);
#endif

	switch (op) {
	case ASSIGN:
	case ADD_ASSIGN:
	case SUB_ASSIGN:
	case MUL_ASSIGN:
	case DIV_ASSIGN:
	case MOD_ASSIGN:
	case LSHIFT_ASSIGN:
	case RSHIFT_ASSIGN:
	case BITAND_ASSIGN:
	case BITXOR_ASSIGN:
	case BITOR_ASSIGN:
		cltype = *ltype;
		crtype = *ltype;
		break;
	case OR:
	case AND:
	case EQ:
	case NEQ:
	case LT:
	case LE:
	case GT:
	case GE:
	case BITOR:
	case BITAND:
	case LSHIFT:
	case RSHIFT:
		cltype.set_is_text(false);
		cltype.set_is_unsigned(false);
		cltype.set_pointers(0);

		switch (ltype->get_atomic_type()) {
		case PARSER_TYPE_CHAR:
		case PARSER_TYPE_SHORT:
		case PARSER_TYPE_INT:
			cltype.set_atomic_type(PARSER_TYPE_INT);
			break;
		case PARSER_TYPE_LONG:
		case PARSER_TYPE_TIME_T:
		case PARSER_TYPE_UNKNOWN:
			cltype.set_atomic_type(PARSER_TYPE_LONG);
			break;
		case PARSER_TYPE_FLOAT:
		case PARSER_TYPE_DOUBLE:
		default:
			throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Invalid field given")).str(APPLOCALE));
		}

		crtype.set_is_text(false);
		crtype.set_is_unsigned(false);
		crtype.set_pointers(0);

		switch (rtype->get_atomic_type()) {
		case PARSER_TYPE_CHAR:
		case PARSER_TYPE_SHORT:
		case PARSER_TYPE_INT:
			crtype.set_atomic_type(PARSER_TYPE_INT);
			break;
		case PARSER_TYPE_LONG:
		case PARSER_TYPE_TIME_T:
		case PARSER_TYPE_UNKNOWN:
			crtype.set_atomic_type(PARSER_TYPE_LONG);
			break;
		case PARSER_TYPE_FLOAT:
		case PARSER_TYPE_DOUBLE:
		default:
			throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Invalid field given")).str(APPLOCALE));
		}
		break;
	case ADD:
	case SUB:
		if (ltype->get_pointers() > 0 && rtype->get_pointers() == 0) {
			switch (rtype->get_atomic_type()) {
			case PARSER_TYPE_SHORT:
			case PARSER_TYPE_INT:
			case PARSER_TYPE_LONG:
				raw_type = *ltype;
				cltype = *ltype;
				crtype = *rtype;
				break;
			default:
				combine_type(*ltype, *rtype);
				cltype = raw_type;
				crtype = raw_type;
				break;
			}
		} else {
			combine_type(*ltype, *rtype);
			cltype = raw_type;
			crtype = raw_type;
		}
		break;
	case MUL:
	case DIV:
	case MOD:
		combine_type(*ltype, *rtype);
		cltype = raw_type;
		crtype = raw_type;
		break;
	case NOT:
		set_is_text(false);
		set_is_unsigned(false);
		set_atomic_type(PARSER_TYPE_INT);
		set_pointers(0);
		cltype = *ltype;
		break;
	case BITNOT:
	case INC:
	case DEC:
	case UADD:
	case UMINUS:
	case LCURVE:
		if (left) {
			raw_type = left->get_raw_type();
			cltype = raw_type;
		} else {
			raw_type = right->get_raw_type();
			crtype = raw_type;
		}
		break;
	case POINTER:
		raw_type = right->get_raw_type();
		if (get_atomic_type() != PARSER_TYPE_UNKNOWN) {
			int pointers = get_pointers();
			if (pointers <= 0)
				throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: operation * is not allowed for expresssion")).str(APPLOCALE));
			raw_type.set_pointers(pointers - 1);
		}
		break;
	case ADDRESS:
		raw_type = right->get_raw_type();
		if (get_atomic_type() != PARSER_TYPE_UNKNOWN) {
			int pointers = get_pointers();
			raw_type.set_pointers(pointers + 1);
		}
		break;
	case SIZEOF:
		set_is_text(false);
		set_is_unsigned(true);
		set_atomic_type(PARSER_TYPE_LONG);
		set_pointers(0);
		break;
	default:
		set_is_text(false);
		set_is_unsigned(false);
		set_atomic_type(PARSER_TYPE_UNKNOWN);
		set_pointers(0);
		break;
	}
}

void parser_expr_t::combine_type(const parser_raw_type& ltype, const parser_raw_type& rtype)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(300, __PRETTY_FUNCTION__, "", NULL);
#endif

	if (ltype.get_pointers() > 0 && rtype.get_pointers() > 0) {
		raw_type.set_is_text(false);
		raw_type.set_is_unsigned(false);
		raw_type.set_atomic_type(PARSER_TYPE_LONG);
		raw_type.set_pointers(0);
	} else if (ltype.get_pointers() > 0) {
		raw_type = rtype;
	} else if (rtype.get_pointers() > 0) {
		raw_type = ltype;
	} else {
		switch (ltype.get_atomic_type()) {
		case PARSER_TYPE_CHAR:
			switch (rtype.get_atomic_type()) {
			case PARSER_TYPE_CHAR:
				raw_type = ltype;
				break;
			default:
				raw_type = rtype;
				break;
			}
			break;
		case PARSER_TYPE_SHORT:
			switch (rtype.get_atomic_type()) {
			case PARSER_TYPE_CHAR:
			case PARSER_TYPE_SHORT:
				raw_type = ltype;
				break;
			default:
				raw_type = rtype;
				break;
			}
			break;
		case PARSER_TYPE_INT:
			switch (rtype.get_atomic_type()) {
			case PARSER_TYPE_CHAR:
			case PARSER_TYPE_SHORT:
			case PARSER_TYPE_INT:
				raw_type = ltype;
				break;
			default:
				raw_type = rtype;
				break;
			}
			break;
		case PARSER_TYPE_LONG:
			switch (rtype.get_atomic_type()) {
			case PARSER_TYPE_CHAR:
			case PARSER_TYPE_SHORT:
			case PARSER_TYPE_INT:
			case PARSER_TYPE_LONG:
				raw_type = ltype;
				break;
			default:
				raw_type = rtype;
				break;
			}
			break;
		case PARSER_TYPE_TIME_T:
			raw_type = ltype;
			return;
		case PARSER_TYPE_FLOAT:
			switch (rtype.get_atomic_type()) {
			case PARSER_TYPE_CHAR:
			case PARSER_TYPE_SHORT:
			case PARSER_TYPE_INT:
			case PARSER_TYPE_LONG:
			case PARSER_TYPE_TIME_T:
			case PARSER_TYPE_FLOAT:
				raw_type = ltype;
				break;
			default:
				raw_type = rtype;
				break;
			}
			break;
		case PARSER_TYPE_DOUBLE:
			switch (rtype.get_atomic_type()) {
			case PARSER_TYPE_CHAR:
			case PARSER_TYPE_SHORT:
			case PARSER_TYPE_INT:
			case PARSER_TYPE_LONG:
			case PARSER_TYPE_TIME_T:
			case PARSER_TYPE_FLOAT:
			case PARSER_TYPE_DOUBLE:
				raw_type = ltype;
				break;
			default:
				raw_type = rtype;
				break;
			}
			break;
		case PARSER_TYPE_UNKNOWN:
		default:
			raw_type = rtype;
			break;
		}
	}
}

parser_exprs_t::parser_exprs_t()
{
}

parser_exprs_t::~parser_exprs_t()
{
	BOOST_FOREACH(parser_expr_t *expr, exprs) {
		delete expr;
	}
}

void parser_exprs_t::push_front(parser_expr_t *expr)
{
	exprs.push_front(expr);
}

void parser_exprs_t::preparse(int flags)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(300, __PRETTY_FUNCTION__, (_("flags={1}") % flags).str(APPLOCALE), NULL);
#endif

	BOOST_FOREACH(parser_expr_t *expr, exprs) {
		expr->preparse(flags);
	}
}

string parser_exprs_t::gen_code(const parser_raw_type *_raw_type)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(300, __PRETTY_FUNCTION__, (_("_raw_type={1}") % _raw_type).str(APPLOCALE), NULL);
#endif

	string code;
	vector<parser_string_t *>::const_reverse_iterator riter = strs.rbegin();

	BOOST_FOREACH(parser_expr_t *expr, exprs) {
		parser_string_t *second = *riter++;
		parser_string_t *first = *riter++;

		code += first->get_prefix();
		code += "[";
		code += expr->gen_code();
		code += second->get_prefix();
		code += "]";
	}

	return code;
}

const deque<parser_expr_t *>& parser_exprs_t::get_exprs() const
{
	return exprs;
}

parser_stmt_t::parser_stmt_t()
{
}

parser_stmt_t::~parser_stmt_t()
{
}

parser_stmt_enum parser_stmt_t::get_stmt_type() const
{
	return stmt_type;
}

parser_stmts_t::parser_stmts_t()
{
}

parser_stmts_t::~parser_stmts_t()
{
	BOOST_FOREACH(parser_stmt_t *stmt, stmts) {
		delete stmt;
	}
}

void parser_stmts_t::push_front(parser_stmt_t *stmt)
{
	stmts.push_front(stmt);
}

void parser_stmts_t::preparse(int flags)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(300, __PRETTY_FUNCTION__, (_("flags={1}") % flags).str(APPLOCALE), NULL);
#endif

	BOOST_FOREACH(parser_stmt_t *stmt, stmts) {
		stmt->preparse(flags);
	}
}

string parser_stmts_t::gen_code(const parser_raw_type *_raw_type)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(300, __PRETTY_FUNCTION__, (_("_raw_type={1}") % _raw_type).str(APPLOCALE), NULL);
#endif

	string code;

	BOOST_FOREACH(parser_stmt_t *stmt, stmts) {
		code += stmt->gen_code();
	}

	return code;
}

parser_declare_t::parser_declare_t(parser_type_t *_type, parser_variable_t *_variable, parser_expr_t *_expr)
	: type(_type),
	  variable(_variable),
	  expr(_expr)
{
}

parser_declare_t::~parser_declare_t()
{
	delete type;
	delete variable;
	delete expr;
}

void parser_declare_t::preparse(int flags)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(300, __PRETTY_FUNCTION__, (_("flags={1}") % flags).str(APPLOCALE), NULL);
#endif

	type->preparse(flags);
	variable->preparse(1);

	if (expr)
		expr->preparse(flags);

	PRSP->_PRSP_temp_types[variable->get_value()] = type->get_raw_type();
}

string parser_declare_t::gen_code(const parser_raw_type *_raw_type)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(300, __PRETTY_FUNCTION__, (_("_raw_type={1}") % _raw_type).str(APPLOCALE), NULL);
#endif

	int idx = 0;
	string code = type->gen_code();
	code += variable->gen_code();
	code += type->gen_code2();

	if (expr) {
		code += strs[idx++]->get_prefix();
		code += "=";
		code += expr->gen_code();
	}

	code += strs[idx]->get_prefix();
	code += ";";
	return code;
}

parser_ctrl_t::parser_ctrl_t()
{
}

parser_ctrl_t::~parser_ctrl_t()
{
}

parser_expr_stmt_t::parser_expr_stmt_t()
{
}

parser_expr_stmt_t::~parser_expr_stmt_t()
{
}

void parser_expr_stmt_t::push_front(parser_expr_t *_expr)
{
	exprs.push_front(_expr);
}

void parser_expr_stmt_t::preparse(int flags)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(300, __PRETTY_FUNCTION__, (_("flags={1}") % flags).str(APPLOCALE), NULL);
#endif

	exprs.preparse(flags);
}

string parser_expr_stmt_t::gen_code(const parser_raw_type *_raw_type)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(300, __PRETTY_FUNCTION__, (_("_raw_type={1}") % _raw_type).str(APPLOCALE), NULL);
#endif

	string code;
	bool first = true;
	vector<parser_string_t *>::const_reverse_iterator riter = strs.rbegin();

	BOOST_FOREACH(parser_expr_t *expr, exprs.get_exprs()) {
		if (first) {
			first = false;
		} else {
			code += (*riter++)->get_prefix();
			code += ",";
		}

		code += expr->gen_code();
	}

	code += (*riter)->get_prefix();
	code += ";";
	return code;
}

parser_if_stmt_t::parser_if_stmt_t(parser_expr_t *_cond, parser_stmt_t *_if_stmt, parser_stmt_t *_else_stmt)
	: cond(_cond),
	  if_stmt(_if_stmt),
	  else_stmt(_else_stmt)
{
}

parser_if_stmt_t::~parser_if_stmt_t()
{
	delete cond;
	delete if_stmt;
	delete else_stmt;
}

void parser_if_stmt_t::preparse(int flags)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(300, __PRETTY_FUNCTION__, (_("flags={1}") % flags).str(APPLOCALE), NULL);
#endif

	cond->preparse(flags);

	if (if_stmt)
		if_stmt->preparse(flags);

	if (else_stmt)
		else_stmt->preparse(flags);
}

string parser_if_stmt_t::gen_code(const parser_raw_type *_raw_type)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(300, __PRETTY_FUNCTION__, (_("_raw_type={1}") % _raw_type).str(APPLOCALE), NULL);
#endif

	int idx = 0;
	string code = strs[idx++]->get_prefix();
	code += "if";

	code += strs[idx++]->get_prefix();
	code += "(";
	code += cond->gen_code();
	code += strs[idx++]->get_prefix();
	code += ")";

	code += if_stmt->gen_code();

	if (else_stmt) {
		code += strs[idx++]->get_prefix();
		code += "else";
		code += else_stmt->gen_code();
	}

	return code;
}

parser_for_stmt_t::parser_for_stmt_t(parser_expr_t *_init, parser_expr_t *_cond, parser_expr_t *_modify, parser_stmt_t *_stmt)
	: init(_init),
	  cond(_cond),
	  modify(_modify),
	  stmt(_stmt)
{
}

parser_for_stmt_t::~parser_for_stmt_t()
{
	delete init;
	delete cond;
	delete modify;
	delete stmt;
}

void parser_for_stmt_t::preparse(int flags)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(300, __PRETTY_FUNCTION__, (_("flags={1}") % flags).str(APPLOCALE), NULL);
#endif

	if (init)
		init->preparse(flags);

	if (cond)
		cond->preparse(flags);

	if (modify)
		modify->preparse(flags);

	if (stmt)
		stmt->preparse(flags);
}

string parser_for_stmt_t::gen_code(const parser_raw_type *_raw_type)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(300, __PRETTY_FUNCTION__, (_("_raw_type={1}") % _raw_type).str(APPLOCALE), NULL);
#endif

	int idx = 0;
	string code = strs[idx++]->get_prefix();
	code += "for";

	code += strs[idx++]->get_prefix();
	code += "(";

	if (init)
		code += init->gen_code();

	code = strs[idx++]->get_prefix();
	code += ";";

	if (cond)
		code += cond->gen_code();

	code = strs[idx++]->get_prefix();
	code += ";";

	if (modify)
		code += modify->gen_code();

	code = strs[idx++]->get_prefix();
	code += ")";

	code += stmt->gen_code();
	return code;
}

parser_case_t::parser_case_t(parser_string_t *_number_type, parser_stmts_t *_stmts)
	: number_type(_number_type),
	  stmts(_stmts)
{
}

parser_case_t::~parser_case_t()
{
	delete number_type;
	delete stmts;
}

void parser_case_t::preparse(int flags)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(300, __PRETTY_FUNCTION__, (_("flags={1}") % flags).str(APPLOCALE), NULL);
#endif

	if (number_type)
		number_type->preparse(flags);

	if (stmts)
		stmts->preparse(flags);
}

string parser_case_t::gen_code(const parser_raw_type *_raw_type)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(300, __PRETTY_FUNCTION__, (_("_raw_type={1}") % _raw_type).str(APPLOCALE), NULL);
#endif

	int idx = 0;
	string code = strs[idx++]->get_prefix();

	if (number_type) {
		code += "case";
		code += number_type->gen_code();
	} else {
		code += "default";
	}

	code += strs[idx++]->get_prefix();
	code += ":";

	if (stmts)
		code += stmts->gen_code();

	return code;
}

parser_cases_t::parser_cases_t()
{
}

parser_cases_t::~parser_cases_t()
{
	BOOST_FOREACH(parser_case_t *item, cases) {
		delete item;
	}
}

void parser_cases_t::push_front(parser_case_t *item)
{
	cases.push_front(item);
}

void parser_cases_t::preparse(int flags)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(300, __PRETTY_FUNCTION__, (_("flags={1}") % flags).str(APPLOCALE), NULL);
#endif

	BOOST_FOREACH(parser_case_t *item, cases) {
		item->preparse(flags);
	}
}

string parser_cases_t::gen_code(const parser_raw_type *_raw_type)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(300, __PRETTY_FUNCTION__, (_("_raw_type={1}") % _raw_type).str(APPLOCALE), NULL);
#endif

	string code;

	BOOST_FOREACH(parser_case_t *item, cases) {
		code += item->gen_code();
	}

	return code;
}

parser_switch_stmt_t::parser_switch_stmt_t(parser_expr_t *_cond, parser_cases_t *_cases)
	: cond(_cond),
	  cases(_cases)
{
}

parser_switch_stmt_t::~parser_switch_stmt_t()
{
	delete cond;
	delete cases;
}

void parser_switch_stmt_t::preparse(int flags)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(300, __PRETTY_FUNCTION__, (_("flags={1}") % flags).str(APPLOCALE), NULL);
#endif

	cond->preparse(flags);

	if (cases)
		cases->preparse(flags);
}

string parser_switch_stmt_t::gen_code(const parser_raw_type *_raw_type)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(300, __PRETTY_FUNCTION__, (_("_raw_type={1}") % _raw_type).str(APPLOCALE), NULL);
#endif

	int idx = 0;
	string code = strs[idx++]->get_prefix();
	code += "switch";

	code += strs[idx++]->get_prefix();
	code += "(";

	code += cond->gen_code();

	code += strs[idx++]->get_prefix();
	code += ")";

	code += strs[idx++]->get_prefix();
	code += "{";

	if (cases)
		code += cases->gen_code();

	code += strs[idx++]->get_prefix();
	code += "}";
	return code;
}

parser_while_stmt_t::parser_while_stmt_t(parser_expr_t *_cond, parser_ctrl_t *_stmt)
	: cond(_cond),
	  stmt(_stmt)
{
}

parser_while_stmt_t::~parser_while_stmt_t()
{
	delete cond;
	delete stmt;
}

void parser_while_stmt_t::preparse(int flags)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(300, __PRETTY_FUNCTION__, (_("flags={1}") % flags).str(APPLOCALE), NULL);
#endif

	cond->preparse(flags);
	stmt->preparse(flags);
}

string parser_while_stmt_t::gen_code(const parser_raw_type *_raw_type)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(300, __PRETTY_FUNCTION__, (_("_raw_type={1}") % _raw_type).str(APPLOCALE), NULL);
#endif

	int idx = 0;
	string code = strs[idx++]->get_prefix();
	code += "while";

	code += strs[idx++]->get_prefix();
	code += "(";

	code += cond->gen_code();

	code += strs[idx++]->get_prefix();
	code += ")";

	code += stmt->gen_code();

	return code;
}

parser_dowhile_stmt_t::parser_dowhile_stmt_t(parser_ctrl_t *_stmt, parser_expr_t *_cond)
	: stmt(_stmt),
	  cond(_cond)
{
}

parser_dowhile_stmt_t::~parser_dowhile_stmt_t()
{
	delete stmt;
	delete cond;
}

void parser_dowhile_stmt_t::preparse(int flags)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(300, __PRETTY_FUNCTION__, (_("flags={1}") % flags).str(APPLOCALE), NULL);
#endif

	stmt->preparse(flags);
	cond->preparse(flags);
}

string parser_dowhile_stmt_t::gen_code(const parser_raw_type *_raw_type)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(300, __PRETTY_FUNCTION__, (_("_raw_type={1}") % _raw_type).str(APPLOCALE), NULL);
#endif

	int idx = 0;
	string code = strs[idx++]->get_prefix();
	code += "do";

	code += stmt->gen_code();

	code += strs[idx++]->get_prefix();
	code += "while";

	code += strs[idx++]->get_prefix();
	code += "(";

	code += cond->gen_code();

	code += strs[idx++]->get_prefix();
	code += ")";

	code += strs[idx++]->get_prefix();
	code += ";";

	return code;
}

parser_goto_stmt_t::parser_goto_stmt_t(parser_string_t *_ident)
	: ident(_ident)
{
}

parser_goto_stmt_t::~parser_goto_stmt_t()
{
	delete ident;
}

string parser_goto_stmt_t::gen_code(const parser_raw_type *_raw_type)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(300, __PRETTY_FUNCTION__, (_("_raw_type={1}") % _raw_type).str(APPLOCALE), NULL);
#endif

	int idx = 0;
	string code = strs[idx++]->get_prefix();
	code += "goto";

	code += ident->gen_code();

	code += strs[idx++]->get_prefix();
	code += ";";
	return code;
}

parser_break_stmt_t::parser_break_stmt_t()
{
}

parser_break_stmt_t::~parser_break_stmt_t()
{
}

string parser_break_stmt_t::gen_code(const parser_raw_type *_raw_type)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(300, __PRETTY_FUNCTION__, (_("_raw_type={1}") % _raw_type).str(APPLOCALE), NULL);
#endif

	int idx = 0;
	string code = strs[idx++]->get_prefix();
	code += "break";

	code += strs[idx++]->get_prefix();
	code += ";";
	return code;
}

parser_continue_stmt_t::parser_continue_stmt_t()
{
}

parser_continue_stmt_t::~parser_continue_stmt_t()
{
}

string parser_continue_stmt_t::gen_code(const parser_raw_type *_raw_type)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(300, __PRETTY_FUNCTION__, (_("_raw_type={1}") % _raw_type).str(APPLOCALE), NULL);
#endif

	int idx = 0;
	string code = strs[idx++]->get_prefix();
	code += "continue";

	code += strs[idx++]->get_prefix();
	code += ";";
	return code;
}

parser_return_stmt_t::parser_return_stmt_t(parser_expr_t *_expr)
	: expr(_expr)
{
}

parser_return_stmt_t::~parser_return_stmt_t()
{
	delete expr;
}

void parser_return_stmt_t::preparse(int flags)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(300, __PRETTY_FUNCTION__, (_("flags={1}") % flags).str(APPLOCALE), NULL);
#endif

	if (expr)
		expr->preparse(flags);
}

string parser_return_stmt_t::gen_code(const parser_raw_type *_raw_type)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(300, __PRETTY_FUNCTION__, (_("_raw_type={1}") % _raw_type).str(APPLOCALE), NULL);
#endif

	int idx = 0;
	string code = strs[idx++]->get_prefix();
	code += "return";

	if (expr)
		code += expr->gen_code();

	code += strs[idx++]->get_prefix();
	code += ";";
	return code;
}

parser_compound_stmt_t::parser_compound_stmt_t(parser_stmts_t *_stmts)
	: stmts(_stmts)
{
}

parser_compound_stmt_t::~parser_compound_stmt_t()
{
	delete stmts;
}

void parser_compound_stmt_t::preparse(int flags)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(300, __PRETTY_FUNCTION__, (_("flags={1}") % flags).str(APPLOCALE), NULL);
#endif

	if (stmts)
		stmts->preparse(flags);
}

string parser_compound_stmt_t::gen_code(const parser_raw_type *_raw_type)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(300, __PRETTY_FUNCTION__, (_("_raw_type={1}") % _raw_type).str(APPLOCALE), NULL);
#endif

	int idx = 0;
	string code = strs[idx++]->get_prefix();
	code += "{";

	if (stmts)
		code += stmts->gen_code();

	code += strs[idx++]->get_prefix();
	code += "}";
	return code;
}

parser_blank_stmt_t::parser_blank_stmt_t()
{
}

parser_blank_stmt_t::~parser_blank_stmt_t()
{
}

string parser_blank_stmt_t::gen_code(const parser_raw_type *_raw_type)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(300, __PRETTY_FUNCTION__, (_("_raw_type={1}") % _raw_type).str(APPLOCALE), NULL);
#endif

	string code = strs[0]->get_prefix();
	code += ";";

	return code;
}

parser_question_stmt_t::parser_question_stmt_t(parser_expr_t *_cond, parser_expr_t *_expr1, parser_expr_t *_expr2)
	: cond(_cond),
	  expr1(_expr1),
	  expr2(_expr2)
{
}

parser_question_stmt_t::~parser_question_stmt_t()
{
	delete cond;
	delete expr1;
	delete expr2;
}

void parser_question_stmt_t::preparse(int flags)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(300, __PRETTY_FUNCTION__, (_("flags={1}") % flags).str(APPLOCALE), NULL);
#endif

	cond->preparse(flags);
	expr1->preparse(flags);
	expr2->preparse(flags);
}

string parser_question_stmt_t::gen_code(const parser_raw_type *_raw_type)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(300, __PRETTY_FUNCTION__, (_("_raw_type={1}") % _raw_type).str(APPLOCALE), NULL);
#endif

	int idx = 0;
	string code = cond->gen_code();

	code += strs[idx++]->get_prefix();
	code += "?";

	code += expr1->gen_code();

	code += strs[idx++]->get_prefix();
	code += ":";

	code += expr2->gen_code();

	code += strs[idx++]->get_prefix();
	code += ";";
	return code;
}

parser_label_stmt_t::parser_label_stmt_t(parser_string_t *_ident)
	: ident(_ident)
{
}

parser_label_stmt_t::~parser_label_stmt_t()
{
	delete ident;
}

string parser_label_stmt_t::gen_code(const parser_raw_type *_raw_type)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(300, __PRETTY_FUNCTION__, (_("_raw_type={1}") % _raw_type).str(APPLOCALE), NULL);
#endif

	string code = ident->gen_code();

	code += strs[0]->get_prefix();
	code += ":";
	return code;
}

}
}

