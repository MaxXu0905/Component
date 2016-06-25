#include "cal_tree.h"
#include "dbc_internal.h"

namespace ai
{
namespace app
{

using namespace std;
using namespace ai::sg;
using namespace ai::scci;

dbc_field_t::dbc_field_t()
{
	field_data = NULL;
}

dbc_field_t::~dbc_field_t()
{
}

dbc_info_t::dbc_info_t()
{
	valid = true;
}

dbc_info_t::~dbc_info_t()
{
}

bool cal_orign_data_source_t::operator<(const cal_orign_data_source_t& rhs) const
{
	if (busi_code < rhs.busi_code)
		return true;
	else if (busi_code > rhs.busi_code)
		return false;

	return chnl_level < rhs.chnl_level;
}

cal_busi_t::cal_busi_t()
{
	flag = 0;
	complex_type = COMPLEX_TYPE_NONE;
	field_data = NULL;
	multiplicator = -1;
	busi_type = -1;
	valid = true;
}

cal_busi_t::~cal_busi_t()
{
}

table_iterator::table_iterator(map<int, table_index_t> *table_indexes_)
	: table_indexes(table_indexes_)
{
	eof_ = false;
}

table_iterator::table_iterator()
{
	eof_ = true;
}

table_iterator::~table_iterator()
{
}

void table_iterator::set(map<int, table_index_t> *table_indexes_)
{
	table_indexes = table_indexes_;
	eof_ = false;
}

bool table_iterator::next()
{
	if (eof_)
		return false;

	map<int, table_index_t>::reverse_iterator iter;
	for (iter = table_indexes->rbegin(); iter != table_indexes->rend(); ++iter) {
		table_index_t& table_index = iter->second;
		table_index.current_index++;
		if (table_index.current_index == table_index.total_size)
			table_index.current_index = 0;
		else
			return true;
	}

	eof_ = true;
	return false;
}

bool table_iterator::eof()
{
	return eof_;
}

int table_iterator::get_index(int table_id)
{
	map<int, table_index_t>::const_iterator iter = table_indexes->find(table_id);
	return iter->second.current_index;
}

cal_tree& cal_tree::instance(calt_ctx *CALT)
{
	return *reinterpret_cast<cal_tree *>(CALT->_CALT_mgr_array[CAL_TREE_MANAGER]);
}

void cal_tree::load()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif
	map<string, cal_busi_t>& busi_map = CALT->_CALT_busi_map;

	table_info_map.clear();
	busi_map.clear();

	load_dbc_table();
	load_dbc_alias();
	make_storage();
	load_busi_driver();
	load_datasource_cfg();
	load_busi_special();
	load_busi_cfg();
	load_busi_ignore();
	load_rule_busi();
	init_datasource();
	init_busi_cfg();
	load_subject();
	load_chl_taxtype();

	// 删除无效的DBC表定义
	map<int, dbc_info_t>::iterator table_info_iter;
	for (table_info_iter = table_info_map.begin(); table_info_iter != table_info_map.end();) {
		dbc_info_t& dbc_info = table_info_iter->second;
		if (!dbc_info.valid) {
			map<int, dbc_info_t>::iterator tmp_iter = table_info_iter++;
			table_info_map.erase(tmp_iter);
		} else {
			++table_info_iter;
		}
	}

	data_source.clear();
	busi_cfg.clear();
	table_name_map.clear();
	rule_busi_map.clear();
}

// 获取指定字段的值
void cal_tree::get_field_value(field_data_t *field_data)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif
	map<string, cal_busi_t>& busi_map = CALT->_CALT_busi_map;

	switch (field_data->deal_type) {
	case DEAL_TYPE_FUNCTION:
		{
			vector<field_data_t *>& rela_fields = field_data->rela_fields;
			vector<field_data_t *>::iterator data_iter;
			for (data_iter = rela_fields.begin(); data_iter != rela_fields.end(); ++data_iter) {
				field_data_t *rela_field = *data_iter;
				if (!(rela_field->flag & FIELD_DATA_GOTTEN))
					get_field_value(rela_field);
			}

			if ((*field_data->func)(NULL, NULL, field_data->in_array, &field_data->data) != 0)
				field_data->flag |= (FIELD_DATA_SET_DEFAULT | FIELD_DATA_GOTTEN);
			else
				field_data->flag |= FIELD_DATA_GOTTEN;
			field_data->data_size = 1;

			dout << "size = " << field_data->data_size << ", busi_code(" << *field_data << ")" << endl;
			return;
		}
	case DEAL_TYPE_FILTER:
		{
			string subject_id = CALT->_CALT_input_fields[field_data->subject_index]->data;
			if (in_subject(field_data->ref_busi_code, subject_id)) {
				if (field_data->orig_deal_type == DEAL_TYPE_COUNT) {
					*reinterpret_cast<int *>(field_data->data) = 1;
				} else {
					field_data_t *src_data = CALT->_CALT_input_fields[field_data->value_index];
					memcpy(field_data->data, src_data->data, field_data->field_size);
				}
			} else {
				switch (field_data->field_type) {
				case SQLTYPE_SHORT:
					*reinterpret_cast<short *>(field_data->data) = 0;
					break;
				case SQLTYPE_INT:
					*reinterpret_cast<int *>(field_data->data) = 0;
					break;
				case SQLTYPE_LONG:
					*reinterpret_cast<long *>(field_data->data) = 0L;
					break;
				case SQLTYPE_FLOAT:
					*reinterpret_cast<float *>(field_data->data) = 0.0;
					break;
				case SQLTYPE_DOUBLE:
					*reinterpret_cast<double *>(field_data->data) = 0.0;
					break;
				}
				field_data->flag |= FIELD_DATA_SET_DEFAULT;
			}
			field_data->data_size = 1;
			field_data->flag |= FIELD_DATA_GOTTEN;

			dout << "size = " << field_data->data_size << ", busi_code(" << *field_data << ")" << endl;
			return;
		}
	default:
		// Passthrough
		break;
	}

	map<int, dbc_info_t>::iterator table_info_iter = table_info_map.find(field_data->table_id);
	dbc_info_t& dbc_info = table_info_iter->second;
	vector<dbc_field_t>& keys = dbc_info.keys;
	vector<dbc_field_t>& values = dbc_info.values;
	vector<dbc_field_t>::iterator field_iter;

	map<int, table_index_t> table_index_map;
	table_iterator table_iter;

	if (dbc_info.dbc_flag & DBC_FLAG_BUSI_TABLE) // 指标表
		memcpy(CALT->_CALT_global_fields[BUSI_CODE_GI]->data, field_data->busi_code.c_str(), field_data->busi_code.length() + 1);

	// 检查需要查找表的关键字的值是否都已经获取
	int i = 0;
	for (field_iter = keys.begin(); field_iter != keys.end(); ++field_iter, i++) {
		field_data_t *field_item = field_iter->field_data;
		if (!(field_item->flag & FIELD_DATA_GOTTEN)) {
			// 如果需要的关键字还没获取，则需要递归调用
			get_field_value(field_item);
		}

		if (field_item->data_size > 1) {
			table_index_t item;
			item.current_index = 0;
			item.total_size = field_item->data_size;
			table_index_map[i] = item;
		} else if (field_item->data_size == 0) {
			field_data->set_data(field_data->default_value);
			field_data->data_size = 1;
			field_data->flag |= FIELD_DATA_GOTTEN | FIELD_DATA_SET_DEFAULT;
			set_null_fields(values);
			return;
		}
	}

	// 初始化结果数组大小
	field_data->flag |= FIELD_DATA_GOTTEN;
	field_data->data_size = 0;
	for (field_iter = values.begin(); field_iter != values.end(); ++field_iter) {
		field_data_t *field_item = field_iter->field_data;
		field_item->flag |= FIELD_DATA_GOTTEN;
		field_item->data_size = 0;
	}

	// 循环调用查找函数
	for (table_iter.set(&table_index_map); !table_iter.eof(); table_iter.next()) {
		// 设置关键字值到数据结构中
		i = 0;
		for (field_iter = keys.begin(); field_iter != keys.end(); ++field_iter, i++) {
			char *addr = src_buf + field_iter->field_offset;

			field_data_t *field_item = field_iter->field_data;
			const char *data = field_item->data;
			if (field_item->data_size > 1) { // 多行记录
				int current_index = table_iter.get_index(i);
				data += current_index * field_item->field_size;
			}

			memcpy(addr, data, field_item->field_size);

			dump(field_iter->field_name, field_iter->field_type, data);
		}

		// 查找结果
		while (1) {
			int count = sdc_mgr.find_dynamic(dbc_info.real_table_id, dbc_info.real_index_id, src_buf, result_buf, max_result_records);
			if (count == max_result_records) {
				max_result_records <<= 1;
				delete []result_buf;
				result_buf = new char[max_record_size * max_result_records];
				continue;
			}

			// 渠道级汇总指标
			if (dbc_info.dbc_flag & DBC_FLAG_BUSI_TABLE) {
				if (count == 1) {
					dbc_field_t& busi_vaule_field = values[field_data->value_index];
					// 这个字段的值直接拷贝到目标中
					char *data = field_data->data;
					double& value = *reinterpret_cast<double *>(result_buf + busi_vaule_field.field_offset);
					switch (field_data->field_type) {
					case SQLTYPE_SHORT:
						*reinterpret_cast<short *>(data) = static_cast<short>(value);
						break;
					case SQLTYPE_INT:
						*reinterpret_cast<int *>(data) = static_cast<int>(value);
						break;
					case SQLTYPE_LONG:
						*reinterpret_cast<long *>(data) = static_cast<long>(value);
						break;
					case SQLTYPE_FLOAT:
						*reinterpret_cast<float *>(data) = static_cast<float>(value);
						break;
					case SQLTYPE_DOUBLE:
						*reinterpret_cast<double *>(data) = value;
						break;
					}
					field_data->data_size = 1;

					dump(field_data->busi_code, field_data->field_type, data);
					goto EXIT_LABEL;
				} else {
					goto NULL_LABEL;
				}
 			}

			switch (field_data->deal_type) {
			case DEAL_TYPE_DIRECT:
				for (i = 0; i < count; i++) {
					char *record = result_buf + i * dbc_info.struct_size;
					for (field_iter = values.begin(); field_iter != values.end(); ++field_iter) {
						field_data_t *field_item = field_iter->field_data;

						if (field_item->data_size == field_item->max_size) {
							field_item->max_size <<= 1;
							field_item->data = reinterpret_cast<char *>(realloc(field_item->data, field_item->max_size * field_item->field_size));
						}

						char *data = field_item->data + field_item->data_size * field_item->field_size;
						char *addr = record + field_iter->field_offset;
						memcpy(data, addr, field_item->field_size);
						field_item->data_size++;

						dump(field_iter->field_name, field_iter->field_type, data);
					}
				}
				break;
			case DEAL_TYPE_SUM:
				{
					//解决合同约定提成比例指标，当用户有多个项目时的问题
					if (count == 0) break;
					dbc_field_t& subject_field = values[dbc_info.subject_index];
					dbc_field_t& result_field = values[field_data->value_index];

					double value = 0.0;
					for (i = 0; i < count; i++) {
						char *record = result_buf + i * dbc_info.struct_size;
						string subject_id = record + subject_field.field_offset;

						dout << "subject_id = " << subject_id << endl;
						dump(field_data->busi_code, field_data->field_type, record);

						if (!in_subject(field_data->ref_busi_code, subject_id))
							continue;

						char *addr = record + result_field.field_offset;
						switch (result_field.field_type) {
						case SQLTYPE_SHORT:
							value += static_cast<double>(*reinterpret_cast<short *>(addr));
							break;
						case SQLTYPE_INT:
							value += static_cast<double>(*reinterpret_cast<int *>(addr));
							break;
						case SQLTYPE_LONG:
							value += static_cast<double>(*reinterpret_cast<long *>(addr));
							break;
						case SQLTYPE_FLOAT:
							value += static_cast<double>(*reinterpret_cast<float *>(addr));
							break;
						case SQLTYPE_DOUBLE:
							value += *reinterpret_cast<double *>(addr);
							break;
						default:
							break;
						}
					}

					char *data = field_data->data;
					switch (field_data->field_type) {
					case SQLTYPE_SHORT:
						*reinterpret_cast<short *>(data) = static_cast<short>(value);
						break;
					case SQLTYPE_INT:
						*reinterpret_cast<int *>(data) = static_cast<int>(value);
						break;
					case SQLTYPE_LONG:
						*reinterpret_cast<long *>(data) = static_cast<long>(value);
						break;
					case SQLTYPE_FLOAT:
						*reinterpret_cast<float *>(data) = static_cast<float>(value);
						break;
					case SQLTYPE_DOUBLE:
						*reinterpret_cast<double *>(data) = value;
						break;
					}

					field_data->data_size = 1;
					goto EXIT_LABEL;
				}
			case DEAL_TYPE_COUNT:
				{
					dbc_field_t& subject_field = values[dbc_info.subject_index];

					long value = 0;
					for (i = 0; i < count; i++) {
						char *record = result_buf + i * dbc_info.struct_size;
						string subject_id = record + subject_field.field_offset;
						dout << "subject_id = " << subject_id << endl;

						if (!in_subject(field_data->ref_busi_code, subject_id))
							continue;

						value++;
					}

					char *data = field_data->data;
					switch (field_data->field_type) {
					case SQLTYPE_SHORT:
						*reinterpret_cast<short *>(data) = static_cast<short>(value);
						break;
					case SQLTYPE_INT:
						*reinterpret_cast<int *>(data) = static_cast<int>(value);
						break;
					case SQLTYPE_LONG:
						*reinterpret_cast<long *>(data) = value;
						break;
					case SQLTYPE_FLOAT:
						*reinterpret_cast<float *>(data) = static_cast<float>(value);
						break;
					case SQLTYPE_DOUBLE:
						*reinterpret_cast<double *>(data) = static_cast<double>(value);
						break;
					}

					field_data->data_size = 1;
					goto EXIT_LABEL;
				}
			case DEAL_TYPE_ATTR:
				{
					dbc_field_t& subject_field = values[dbc_info.subject_index];

					map<string, cal_busi_t>::const_iterator busi_iter = busi_map.find(field_data->ref_busi_code);
					if (busi_iter == busi_map.end()) {
						dout << "return " << endl;
						return ;
					}

					const cal_busi_subject_t& subject = busi_iter->second.subject;

					if (subject.rel_type == SUBJECT_REL_TYPE_INCLUDE) //包含属性，默认为0
						*reinterpret_cast<int *>(field_data->data) = 0;
					else //不包含属性默认为1
						*reinterpret_cast<int *>(field_data->data) = 1;

					for (i = 0; i < count; i++) {
						char *record = result_buf + i * dbc_info.struct_size;
						string subject_id = record + subject_field.field_offset;

						dout << "busi_code = " << field_data->ref_busi_code << endl;
						dout << "subject_id = " << subject_id << endl;

						if (!in_attr(subject, subject_id))
							continue;

						// 只要in_str返回true，即找到包含或不包含的属性，则赋值退出
						*reinterpret_cast<int *>(field_data->data) = ((*reinterpret_cast<int *>(field_data->data)==1)?0:1);
						break;
					}

					field_data->data_size = 1;
					break;
				}
			case DEAL_TYPE_BOOL:
				if (field_data->data_size != 1) {
					*reinterpret_cast<int *>(field_data->data) = (count > 0);
					field_data->data_size = 1;
				} else {
					*reinterpret_cast<int *>(field_data->data) |= (count > 0);
				}
				break;
			}

			break;
		}
	}

NULL_LABEL:
	if (field_data->data_size == 0) {
		field_data->set_data(field_data->default_value);
		field_data->data_size = 1;
		field_data->flag |= FIELD_DATA_SET_DEFAULT;
		set_null_fields(values);
		return;
	}

EXIT_LABEL:
	dout << "size = " << field_data->data_size << ", busi_code(" << *field_data << ")" << endl;
}

bool cal_tree::in_attr(const cal_busi_subject_t& subject, const string& subject_id)
{
	bool retval = false;
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, (_("subject_id={1}") % subject_id).str(APPLOCALE), &retval);
#endif

	const vector<string>& items = subject.items;

	if (subject.rel_type == SUBJECT_REL_TYPE_ALL) {
		retval = true;
		return retval;
	}

	for (vector<string>::const_iterator iter = items.begin(); iter != items.end(); ++iter) {
		if (*iter == subject_id) {
			retval = true;
			return retval;
		}
	}

	return retval;
}

bool cal_tree::in_subject(const string& busi_code, const string& subject_id)
{
	bool retval = false;
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, (_("busi_code={1}, subject_id={2}") % busi_code % subject_id).str(APPLOCALE), &retval);
#endif
	map<string, cal_busi_t>& busi_map = CALT->_CALT_busi_map;

	map<string, cal_busi_t>::const_iterator busi_iter = busi_map.find(busi_code);
	if (busi_iter == busi_map.end())
		return retval;

	const cal_busi_subject_t& subject = busi_iter->second.subject;
	const vector<string>& items = subject.items;

	if (subject.rel_type == SUBJECT_REL_TYPE_ALL) {
		retval = true;
		return retval;
	}

	bool include = (subject.rel_type == SUBJECT_REL_TYPE_INCLUDE);
	for (vector<string>::const_iterator iter = items.begin(); iter != items.end(); ++iter) {
		if (*iter == subject_id) {
			retval = include;
			return include;
		}
	}

	retval = !include;
	return retval;
}

bool cal_tree::ignored(const string& busi_code)
{
	bool retval = false;
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, (_("busi_code={1}") % busi_code).str(APPLOCALE), &retval);
#endif

	retval = (busi_ignore_set.find(busi_code) != busi_ignore_set.end());
	return retval;
}

void cal_tree::reset_fields()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	for (vector<field_data_t *>::iterator iter = policy_field_data.begin(); iter != policy_field_data.end(); ++iter) {
		field_data_t *field_data = *iter;
		field_data->data_size = 0;
		field_data->flag &= ~(FIELD_DATA_SET_DEFAULT | FIELD_DATA_GOTTEN);
	}
}

bool cal_tree::add_rule_busi(int rule_id, const string& alias_name, const string& rule_busi, string& busi_code)
{
	bool retval = false;
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, (_("rule_id={1}, alias_name={2}, rule_busi={3}") % rule_id % alias_name % rule_busi).str(APPLOCALE), &retval);
#endif

	string new_rule_busi ;

	if (rule_busi.find("$calc_cycle")!= string::npos) {
        new_rule_busi = rule_busi.substr(0, rule_busi.find("$calc_cycle"));
        new_rule_busi += "month_between($calc_cycle,";
		string::const_iterator iter;
		string busi2 = rule_busi.substr(rule_busi.find("%"));
		bool close=false;
		for (iter = busi2.begin(); iter != busi2.end();iter++) {
		    new_rule_busi+=*iter;
			if ((iter  + 1) != busi2.end() && !isalnum(*(iter+1)))
				if(!close){
				 	new_rule_busi+=")";
					close=true;
				}
		}
		if (!close) new_rule_busi+=")";
	}else
		new_rule_busi = rule_busi;

	map<string, string>::const_iterator expr_iter = CALT->_CALT_expr_map.find(new_rule_busi);
	if (expr_iter != CALT->_CALT_expr_map.end()) {
		busi_code = expr_iter->second;
	} else {
		// 添加指标
		busi_code = "~" + boost::lexical_cast<string>(CALT->_CALT_gen_busi_idx++);
		CALT->_CALT_gen_busi[busi_code] = new_rule_busi;
		CALT->_CALT_expr_map[new_rule_busi] = busi_code;

		cal_rule_busi_item_t item;
		item.busi_code = busi_code;
		item.rule_busi = "#0 = MAXDOUBLE; #0 = ";
		item.rule_busi += new_rule_busi;
		item.rule_busi += ";";
		item.field_type = SQLTYPE_DOUBLE;
		item.field_size = sizeof(double);
		item.precision = 6;
		item.busi_local_flag = false;

		if (!add_rule_busi(item))
			return retval;

		// 设置驱动源，所有引用的指标驱动源相同，则驱动源可确定，
		// 否则，驱动源不定
		string driver_data;
		for (string::const_iterator iter = rule_busi.begin(); iter != rule_busi.end(); ++iter) {
			if (*iter != '%')
				continue;

			string token;
			for (; iter != rule_busi.end(); ++iter) {
				if (!isalnum(*iter))
					break;

				token += *iter;
			}

			map<string, string>::const_iterator driver_iter = busi_driver_map.find(token);
			if (driver_iter == busi_driver_map.end()) {
				driver_data.clear();
				break;
			}

			if (driver_data.empty()) {
				driver_data = driver_iter->second;
			} else if (driver_data != driver_iter->second) {
				driver_data = "09";
				break;
			}
		}

		if (!driver_data.empty() && driver_data != "00")
			busi_driver_map[busi_code] = driver_data;

		// 创建指标
		map<string, cal_rule_busi_t>::const_iterator rule_busi_iter = rule_busi_map.find(busi_code);
		if (rule_busi_iter != rule_busi_map.end())
			set_data_source(busi_code, busi_code, rule_busi_iter->second);

		// 该指标有别名，需要在输出时使用别名
		map<string, cal_busi_t>& busi_map = CALT->_CALT_busi_map;
		map<string, cal_busi_t>::iterator busi_iter = busi_map.find(busi_code);
		if (busi_iter != busi_map.end()) {
			cal_busi_t& item = busi_iter->second;
			if (item.valid)
				item.field_data->flag |= FIELD_DATA_HAS_ALIAS;
		}
	}

	// 设置指标别名
	cal_busi_alias_t alias;
	alias.rule_id = rule_id;
	alias.busi_code = busi_code;
	CALT->_CALT_busi_alias[alias] = busi_code;

	retval = true;
	return retval;
}

cal_tree::cal_tree()
	:  sdc_mgr(sdc_api::instance())
{
	src_buf = NULL;
	result_buf = NULL;
}

cal_tree::~cal_tree()
{
	delete []result_buf;
	delete []src_buf;
}

// 加载DBC配置信息
void cal_tree::load_dbc_table()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif
	dbc_config& config_mgr = dbc_config::instance();
	if (!config_mgr.open())
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Can't open DBCPROFILE.")).str(APPLOCALE));

	const map<int, dbc_data_t>& data_map = config_mgr.get_data_map();
	const map<dbc_index_key_t, dbc_index_t>& index_map = config_mgr.get_index_map();

	BOOST_FOREACH (const cal_dbc_table_t& table, CALP->_CALP_dbc_tables) {
		map<int, dbc_data_t>::const_iterator data_iter;
		for (data_iter = data_map.begin(); data_iter != data_map.end(); ++data_iter) {
			if (strcasecmp(table.table_name.c_str(), data_iter->second.table_name) == 0)
				break;
		}
		if (data_iter == data_map.end())
			throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Can't find table {1}") % table.table_name).str(APPLOCALE));

		dbc_index_key_t index_key;
		index_key.table_id = data_iter->first;
		index_key.index_id = table.index_id;
		map<dbc_index_key_t, dbc_index_t>::const_iterator index_iter = index_map.find(index_key);
		if (index_iter == index_map.end())
			throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Can't find index {1} for table {2}") % table.index_id % table.table_name).str(APPLOCALE));

		const dbc_data_t& dbc_data = data_iter->second;
		const dbc_index_t& dbc_index = index_iter->second;

		map<int, dbc_info_t>::iterator table_info_iter = table_info_map.insert(std::make_pair(data_iter->first, dbc_info_t())).first;
		dbc_info_t& item = table_info_iter->second;

		item.real_table_id = data_iter->first;
		item.real_index_id = table.index_id;
		item.dbc_flag = 0;
		item.table_name = table.table_name;
		item.struct_size = dbc_data.struct_size;
		item.subject_index = -1;
		item.valid = true;

		for (int i = 0; i < dbc_data.field_size; i++) {
			const dbc_data_field_t& data_field = dbc_data.fields[i];

			bool found = false;
			for (int j = 0; j < dbc_index.field_size; j++) {
				const dbc_index_field_t& index_field = dbc_index.fields[j];
				if (strcmp(index_field.field_name, data_field.field_name) == 0) {
					found = true;
					break;
				}
			}

			dbc_field_t field;
			field.field_name = data_field.field_name;
			field.field_type = data_field.field_type;
			field.field_size = data_field.field_size;
			field.field_offset = data_field.field_offset;

			if (found)
				item.keys.push_back(field);
			else
				item.values.push_back(field);
		}

		if (table_name_map.find(item.table_name) != table_name_map.end())
			throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: table {1} is defined more times.") % item.table_name).str(APPLOCALE));

		table_name_map[item.table_name] = item.real_table_id;
	}
}

// 增加对表的别名的定义
void cal_tree::load_dbc_alias()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif
	dbc_config& config_mgr = dbc_config::instance();
	const map<int, dbc_data_t>& data_map = config_mgr.get_data_map();
	const map<dbc_index_key_t, dbc_index_t>& index_map = config_mgr.get_index_map();
	map<int, dbc_info_t>::iterator table_info_iter;
	vector<dbc_field_t>::iterator field_iter;

	if (data_map.empty())
		return;

	// 获取表的最大ID
	int max_table_id = data_map.rbegin()->first;

	BOOST_FOREACH (const cal_dbc_alias_t& table, CALP->_CALP_dbc_aliases) {
		map<int, dbc_data_t>::const_iterator data_iter;
		for (data_iter = data_map.begin(); data_iter != data_map.end(); ++data_iter) {
			if (strcasecmp(table.orig_table_name.c_str(), data_iter->second.table_name) == 0)
				break;
		}
		if (data_iter == data_map.end())
			throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Can't find table {1}") % table.orig_table_name).str(APPLOCALE));

		dbc_index_key_t index_key;
		index_key.table_id = data_iter->first;
		index_key.index_id = table.orig_index_id;
		map<dbc_index_key_t, dbc_index_t>::const_iterator index_iter = index_map.find(index_key);
		if (index_iter == index_map.end())
			throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Can't find index {1} for table {2}") % table.orig_index_id % table.orig_table_name).str(APPLOCALE));

		const dbc_data_t& dbc_data = data_iter->second;
		const dbc_index_t& dbc_index = index_iter->second;

		int table_id = ++max_table_id;

		map<int, dbc_info_t>::iterator table_info_iter = table_info_map.insert(std::make_pair(table_id, dbc_info_t())).first;
		dbc_info_t& item = table_info_iter->second;

		item.real_table_id = data_iter->first;
		item.real_index_id = table.orig_index_id;
		item.dbc_flag = 0;
		item.table_name = table.table_name;
		item.struct_size = dbc_data.struct_size;
		item.subject_index = -1;
		item.valid = true;

		for (int i = 0; i < dbc_data.field_size; i++) {
			const dbc_data_field_t& data_field = dbc_data.fields[i];

			bool found = false;
			for (int j = 0; j < dbc_index.field_size; j++) {
				const dbc_index_field_t& index_field = dbc_index.fields[j];
				if (strcmp(index_field.field_name, data_field.field_name) == 0) {
					found = true;
					break;
				}
			}

			dbc_field_t field;

			const map<string, string>& field_map = table.field_map;
			map<string, string>::const_iterator field_iter = field_map.find(data_field.field_name);
			if (field_iter == field_map.end())
				field.field_name = data_field.field_name;
			else
				field.field_name = field_iter->second;

			field.field_type = data_field.field_type;
			field.field_size = data_field.field_size;
			field.field_offset = data_field.field_offset;

			if (found)
				item.keys.push_back(field);
			else
				item.values.push_back(field);
		}

		if (table_name_map.find(item.table_name) != table_name_map.end())
			throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: table {1} is defined more times.") % item.table_name).str(APPLOCALE));

		table_name_map[item.table_name] = table_id;
	}
}

void cal_tree::make_storage()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif
	const cal_parms_t& parms = CALP->_CALP_parms;
	field_data_map_t& data_map = CALT->_CALT_data_map;
	map<int, dbc_info_t>::iterator table_info_iter;
	vector<dbc_field_t>::iterator field_iter;

	// 检查所有关键字，是否能找到来源
	check_dbc(false);

	// 创建字段的存储
	max_record_size = 0;
	busi_table_id = -1;
	acct_busi_table_id = -1;
	busi_value_index = -1;
	acct_busi_value_index = -1;

	for (table_info_iter = table_info_map.begin(); table_info_iter != table_info_map.end(); ++table_info_iter) {
		dbc_info_t& dbc_info = table_info_iter->second;
		if (!dbc_info.valid)
			continue;

		vector<dbc_field_t>& keys = dbc_info.keys;
		vector<dbc_field_t>& values = dbc_info.values;

		if (max_record_size < dbc_info.struct_size)
			max_record_size = dbc_info.struct_size;

		// 对查表的结果字段分配内存
		for (field_iter = values.begin(); field_iter != values.end(); ++field_iter) {
			if (field_iter->field_name == SUBJECT_ID)
				dbc_info.subject_index = field_iter - values.begin();

			field_data_map_t::iterator iter = data_map.find(field_iter->field_name);
			if (iter != data_map.end()) {
				// 对于科目字段，允许重复
				if (field_iter->field_name == SUBJECT_ID) {
					field_iter->field_data = iter->second;
					continue;
				}

				if (field_iter->field_name == BUSI_VALUE)
					throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: field_name {1} of table_id {2} is already defined in another table or source record.") % field_iter->field_name % table_info_iter->first).str(APPLOCALE));

				// 如果结果字段冲突，则需要加后缀
				switch (parms.collision_level) {
				case COLLISION_LEVEL_NONE: // 不允许冲突
					throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: field_name {1} of table_id {2} is already defined in another table or source record.") % field_iter->field_name % table_info_iter->first).str(APPLOCALE));
				case COLLISION_LEVEL_SOURCE: // 允许于输入冲突
					if (iter->second->table_id == -1) // 来源于输入
						field_iter->field_name += (boost::format("$%1%") % table_info_iter->first).str();
					else
						throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: field_name {1} of table_id {2} is already defined in another table.") % field_iter->field_name % table_info_iter->first).str(APPLOCALE));
					break;
				case COLLISION_LEVEL_ALL: // 允许任何冲突
					field_iter->field_name += (boost::format("$%1%") % table_info_iter->first).str();
					break;
				}
			}

			// 如果是指标表，则需要做特殊标记
			if (field_iter->field_name == BUSI_VALUE) {
				bool found = false;
				for (vector<dbc_field_t>::iterator key_iter = keys.begin(); key_iter != keys.end(); ++key_iter) {
					if (key_iter->field_name == BUSI_CODE) {
						found = true;
						break;
					}
				}

				if (!found)
					throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: table {1} has value {2}, but missing key {3}") % dbc_info.table_name % BUSI_CODE % BUSI_CODE).str(APPLOCALE));

				dbc_info.dbc_flag |= DBC_FLAG_BUSI_TABLE;
				busi_table_id = table_info_iter->first;
				busi_value_index = (field_iter - values.begin());
			}else if (field_iter->field_name == ACCT_BUSI_VALUE) {
				bool found = false;
				for (vector<dbc_field_t>::iterator key_iter = keys.begin(); key_iter != keys.end(); ++key_iter) {
					if (key_iter->field_name == BUSI_CODE) {
						found = true;
						break;
					}
				}

				if (!found)
					throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: table {1} has value {2}, but missing key {3}") % dbc_info.table_name % ACCT_BUSI_VALUE % BUSI_CODE).str(APPLOCALE));

				dbc_info.dbc_flag |= DBC_FLAG_BUSI_TABLE;
				acct_busi_table_id = table_info_iter->first;
				acct_busi_value_index = (field_iter - values.begin());
			}else if (field_iter->field_name == PAY_CHNL_ID) {
				dbc_info.dbc_flag |= DBC_FLAG_PAY_CHNL;
			}

			// 创建字段的存储，不要对busi_code赋值
			field_data_t *field_data = new field_data_t(FIELD_DATA_RESET | FIELD_DATA_NO_DEFAULT, "",
				"", field_iter->field_type, field_iter->field_size, table_info_iter->first,
				field_iter - values.begin(), -1, DEAL_TYPE_DIRECT);

			// 如果是政策级的字段，在切换政策时需要初始化
			if (dbc_info.dbc_flag & DBC_FLAG_POLICY_LEVEL)
				policy_field_data.push_back(field_data);

			data_map[field_iter->field_name] = field_data;
			field_iter->field_data = field_data;
		}
	}

	// 设置关键字字段的存储位置
	for (table_info_iter = table_info_map.begin(); table_info_iter != table_info_map.end(); ++table_info_iter) {
		dbc_info_t& dbc_info = table_info_iter->second;
		if (!dbc_info.valid)
			continue;

		vector<dbc_field_t>& keys = dbc_info.keys;

		// 对查表的条件字段设置值
		for (field_iter = keys.begin(); field_iter != keys.end(); ++field_iter) {
			field_data_map_t::iterator iter = data_map.find(field_iter->field_name);
			if (iter == data_map.end())
				throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: field_name {1} of table_id {2} is not defined yet.") % field_iter->field_name % table_info_iter->first).str(APPLOCALE));
			field_iter->field_data = iter->second;
		}
	}

	// 检查所有关键字，如有需要，重新分配内存
	check_dbc(true);

	max_result_records = INIT_MAX_RESULT_RECORDS;
	src_buf = new char[max_record_size];
	result_buf = new char[max_record_size * max_result_records];
}

// 加载指标与驱动源映射表
void cal_tree::load_busi_driver()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif
	set<string>& specify_driver_busi_set = CALT->_CALT_specify_driver_busi_set;
	string sql_stmt = (boost::format("SELECT busi_code{char[%1%]}, driver_data{char[%2%]} "
		"FROM unical_busi_driver")
		% BUSI_CODE_LEN % DRIVER_DATA_LEN).str();

	try {
		Generic_Database *db = CALT->_CALT_db;
		auto_ptr<Generic_Statement> auto_stmt(db->create_statement());
		Generic_Statement *stmt = auto_stmt.get();

		auto_ptr<struct_dynamic> auto_data(db->create_data(true));
		struct_dynamic *data = auto_data.get();
		if (data == NULL) {
			GPP->write_log(__FILE__, __LINE__, (_("ERROR: Memory allocation failure")).str(APPLOCALE));
			exit(1);
		}

		data->setSQL(sql_stmt);
		stmt->bind(data);

		auto_ptr<Generic_ResultSet> auto_rset(stmt->executeQuery());
		Generic_ResultSet *rset = auto_rset.get();

		while (rset->next()) {
			string busi_code = rset->getString(1);
			string driver_data = rset->getString(2);
			if (driver_data != "00")
				busi_driver_map[busi_code] = driver_data;
			else
				specify_driver_busi_set.insert(busi_code);
		}
	} catch (bad_sql& ex) {
		throw bad_msg(__FILE__, __LINE__, 0, ex.what());
	}
}

// 加载指标来源表
void cal_tree::load_datasource_cfg()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif
	gpenv& env_mgr = gpenv::instance();
	string sql_stmt = (boost::format("SELECT driver_data{char[%1%]}, busi_code{char[%2%]}, "
		"ref_busi_code{char[%2%]}, channel_level{int}, lower(table_name){char[%3%]}, "
		"lower(column_name){char[%4%]}, deal_type{int}, use_default{int}, default_value{char[%5%]}, "
		"lower(subject_name){char[%6%]}, complex_type{int}, precision{int}, busi_type{int} "
		"FROM unical_datasource_cfg")
		% DRIVER_DATA_LEN % BUSI_CODE_LEN % TABLE_NAME_SIZE % COLUMN_NAME_SIZE
		% DEFAULT_VALUE_LEN % SUBJECT_ID_LEN).str();

	try {
		Generic_Database *db = CALT->_CALT_db;
		auto_ptr<Generic_Statement> auto_stmt(db->create_statement());
		Generic_Statement *stmt = auto_stmt.get();

		auto_ptr<struct_dynamic> auto_data(db->create_data(true));
		struct_dynamic *data = auto_data.get();
		if (data == NULL) {
			GPP->write_log(__FILE__, __LINE__, (_("ERROR: Memory allocation failure")).str(APPLOCALE));
			exit(1);
		}

		data->setSQL(sql_stmt);
		stmt->bind(data);

		auto_ptr<Generic_ResultSet> auto_rset(stmt->executeQuery());
		Generic_ResultSet *rset = auto_rset.get();

		while (rset->next()) {
			cal_data_source_t item;

			item.driver_data = rset->getString(1);
			item.busi_code = rset->getString(2);
			item.ref_busi_code = rset->getString(3);
			item.chnl_level = rset->getInt(4);
			env_mgr.expand_var(item.table_name, rset->getString(5));
			item.column_name = rset->getString(6);

			item.deal_type = rset->getInt(7);
			if (item.deal_type < DEAL_TYPE_MIN || item.deal_type > DEAL_TYPE_MAX)
				throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: deal_type for busi_code {1} is not correct.") % item.busi_code).str(APPLOCALE));

			item.use_default = (rset->getInt(8) == 1);
			item.default_value = rset->getString(9);
			item.subject_name = rset->getString(10);

			item.complex_type = rset->getInt(11);
			if (item.complex_type < COMPLEX_TYPE_MIN || item.complex_type > COMPLEX_TYPE_MAX)
				throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: complex_type for busi_code {1} is not correct.") % item.busi_code).str(APPLOCALE));

			int precision = rset->getInt(12);
			if (precision < 0) {
				item.multiplicator = -1;
			} else {
				item.multiplicator = 1;
				for (int i = 0; i < precision; i++)
					item.multiplicator *= 10;
			}

			item.busi_type = rset->getInt(13);

			data_source.push_back(item);
		}
	} catch (bad_sql& ex) {
		throw bad_msg(__FILE__, __LINE__, 0, ex.what());
	}
}

// 加载指标来源表
void cal_tree::load_busi_cfg()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif
	string sql_stmt = (boost::format("SELECT busi_code{char[%1%]}, src_busi_code{char[%1%]}, "
		"statistics_object{int}, is_mix_flag{int}, busi_local_flag{int}, is_kpi_flag{int} FROM ai_busi_cfg "
		" where province_code in('09',:province_code{char[%2%]})")
		% BUSI_CODE_LEN % PROVINCE_CODE_LEN).str();

	try {
		Generic_Database *db = CALT->_CALT_db;
		auto_ptr<Generic_Statement> auto_stmt(db->create_statement());
		Generic_Statement *stmt = auto_stmt.get();

		auto_ptr<struct_dynamic> auto_data(db->create_data(true));
		struct_dynamic *data = auto_data.get();
		if (data == NULL) {
			GPP->write_log(__FILE__, __LINE__, (_("ERROR: Memory allocation failure")).str(APPLOCALE));
			exit(1);
		}

		data->setSQL(sql_stmt);
		stmt->bind(data);

		const cal_parms_t& parms = CALP->_CALP_parms;
		stmt->setString(1, parms.province_code);

		auto_ptr<Generic_ResultSet> auto_rset(stmt->executeQuery());
		Generic_ResultSet *rset = auto_rset.get();

		while (rset->next()) {
			cal_busi_cfg_t item;

			item.busi_code = rset->getString(1);
			item.src_busi_code = rset->getString(2);
			item.statistics_object = rset->getInt(3);
			item.is_mix_flag = (rset->getInt(4) == 1);
			item.busi_local_flag = (rset->getInt(5) == 1);
			item.enable_cycles = (rset->getInt(6) == 3);

			busi_cfg.push_back(item);
		}
	} catch (bad_sql& ex) {
		throw bad_msg(__FILE__, __LINE__, 0, ex.what());
	}
}

// 加载忽略的指标表
void cal_tree::load_busi_ignore()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	const cal_parms_t& parms = CALP->_CALP_parms;
	string sql_stmt = (boost::format("SELECT busi_code{char[%1%]} FROM unical_busi_ignore "
		"WHERE driver_data = :driver_data{char[%2%]}")
		% BUSI_CODE_LEN % DRIVER_DATA_LEN).str();

	try {
		Generic_Database *db = CALT->_CALT_db;
		auto_ptr<Generic_Statement> auto_stmt(db->create_statement());
		Generic_Statement *stmt = auto_stmt.get();

		auto_ptr<struct_dynamic> auto_data(db->create_data(true));
		struct_dynamic *data = auto_data.get();
		if (data == NULL) {
			GPP->write_log(__FILE__, __LINE__, (_("ERROR: Memory allocation failure")).str(APPLOCALE));
			exit(1);
		}

		data->setSQL(sql_stmt);
		stmt->bind(data);

		stmt->setString(1, parms.driver_data);

		auto_ptr<Generic_ResultSet> auto_rset(stmt->executeQuery());
		Generic_ResultSet *rset = auto_rset.get();

		while (rset->next()) {
			string busi_code = rset->getString(1);

			busi_ignore_set.insert(busi_code);

			BOOST_FOREACH(const cal_busi_cfg_t& v, busi_cfg) {
				if (busi_code == v.src_busi_code) {
					busi_ignore_set.insert(v.busi_code);
					break;
				}
			}

			busi_ignore_set.insert(busi_code);
		}
	} catch (bad_sql& ex) {
		throw bad_msg(__FILE__, __LINE__, 0, ex.what());
	}
}

// 加载通过规则配置的指标
void cal_tree::load_rule_busi()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif
	const cal_parms_t& parms = CALP->_CALP_parms;
	vector<cal_rule_busi_item_t> busi_items;

	for (int i = 0; i < 2; i++) {
		string sql_stmt = (boost::format("SELECT busi_code{char[%1%]}, rule_busi{char[4000]}, "
			"decode(field_type, 1, 2, 2, 4, 3, 6, 4, 8, 5, 9, 6, 10, 7, 13){int}, "
			"field_size{int}, precision{int}, busi_local_flag{int} "
			"FROM unical_rule_busi "
			"WHERE driver_data = :driver_data{char[%2%]}")
			% BUSI_CODE_LEN % DRIVER_DATA_LEN).str();

		try {
			Generic_Database *db = CALT->_CALT_db;
			auto_ptr<Generic_Statement> auto_stmt(db->create_statement());
			Generic_Statement *stmt = auto_stmt.get();

			auto_ptr<struct_dynamic> auto_data(db->create_data(true));
			struct_dynamic *data = auto_data.get();
			if (data == NULL) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: Memory allocation failure")).str(APPLOCALE));
				exit(1);
			}

			data->setSQL(sql_stmt);
			stmt->bind(data);

			if (i == 0)
				stmt->setString(1, parms.driver_data);
			else
				stmt->setString(1, "00");

			auto_ptr<Generic_ResultSet> auto_rset(stmt->executeQuery());
			Generic_ResultSet *rset = auto_rset.get();

			while (rset->next()) {
				cal_rule_busi_item_t item;

				item.busi_code = rset->getString(1);
				item.rule_busi = rset->getString(2) + '\n';
				item.field_type = rset->getInt(3);
				item.field_size = rset->getInt(4);
				item.precision = rset->getInt(5);
				item.busi_local_flag = (rset->getInt(6) == 1);

				busi_items.push_back(item);
			}
		} catch (bad_sql& ex) {
			throw bad_msg(__FILE__, __LINE__, 0, ex.what());
		}
	}

	BOOST_FOREACH(const cal_rule_busi_item_t& busi_item, busi_items) {
		(void)add_rule_busi(busi_item);
	}
}

bool cal_tree::add_rule_busi(const cal_rule_busi_item_t& busi_item)
{
	bool retval = false;
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", &retval);
#endif
	map<string, cal_busi_t>& busi_map = CALT->_CALT_busi_map;

	map<string, cal_busi_t>::iterator busi_iter = busi_map.find(busi_item.busi_code);
	if (busi_iter != busi_map.end()) {
		GPP->write_log(__FILE__, __LINE__, (_("ERROR: busi_code {1} is already defined in unical_datasource_cfg.") % busi_item.busi_code).str(APPLOCALE));
		return retval;
	}

	if (rule_busi_map.find(busi_item.busi_code) != rule_busi_map.end()) {
		GPP->write_log(__FILE__, __LINE__, (_("ERROR: busi_code {1} is defined more times.") % busi_item.busi_code).str(APPLOCALE));
		return retval;
	}

	cal_rule_busi_t item;
	item.rule_busi = busi_item.rule_busi;
	item.field_type = busi_item.field_type;

	switch (item.field_type) {
	case SQLTYPE_CHAR:
		item.field_size = sizeof(char);
		break;
	case SQLTYPE_SHORT:
		item.field_size = sizeof(short);
		break;
	case SQLTYPE_INT:
		item.field_size = sizeof(int);
		break;
	case SQLTYPE_LONG:
		item.field_size = sizeof(long);
		break;
	case SQLTYPE_FLOAT:
		item.field_size = sizeof(float);
		break;
	case SQLTYPE_DOUBLE:
		item.field_size = sizeof(double);
		break;
	case SQLTYPE_STRING:
		item.field_size = busi_item.field_size + 1;
		break;
	case SQLTYPE_DATETIME:
		item.field_size = sizeof(time_t);
		break;
	default:
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Unsupported field_type for busi_code {1}") % busi_item.busi_code).str(APPLOCALE));
	}

	item.precision = busi_item.precision;
	item.busi_local_flag = busi_item.busi_local_flag;

	rule_busi_map[busi_item.busi_code] = item;

	retval = true;
	return retval;
}

// 加载科目表，自定义指标需要使用该表
void cal_tree::load_subject()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif
	map<string, cal_busi_t>& busi_map = CALT->_CALT_busi_map;
	for (map<string, cal_busi_t>::iterator busi_iter = busi_map.begin();  busi_iter != busi_map.end(); busi_iter++)
			busi_iter->second.subject.rel_type = SUBJECT_REL_TYPE_ALL;


	string sql_stmt = (boost::format("SELECT distinct busi_code{char[%1%]}, item_type{char[4000]}, "
		"item_id{char[4000]}, rel_type{int} FROM ai_busi_comm_item_rel"
		" where province_code in('09',:province_code{char[%2%]})")
		% BUSI_CODE_LEN % PROVINCE_CODE_LEN).str();
	set<string> ignore_set;

	try {
		Generic_Database *db = CALT->_CALT_db;
		auto_ptr<Generic_Statement> auto_stmt(db->create_statement());
		Generic_Statement *stmt = auto_stmt.get();

		auto_ptr<struct_dynamic> auto_data(db->create_data(true));
		struct_dynamic *data = auto_data.get();
		if (data == NULL) {
			GPP->write_log(__FILE__, __LINE__, (_("ERROR: Memory allocation failure")).str(APPLOCALE));
			exit(1);
		}

		data->setSQL(sql_stmt);
		stmt->bind(data);
		const cal_parms_t& parms = CALP->_CALP_parms;
		stmt->setString(1, parms.province_code);

		auto_ptr<Generic_ResultSet> auto_rset(stmt->executeQuery());
		Generic_ResultSet *rset = auto_rset.get();

		while (rset->next()) {
			string busi_code = rset->getString(1);
			map<string, cal_busi_t>::iterator busi_iter = busi_map.find(busi_code);
			if (busi_iter == busi_map.end()) {
				set<string>::const_iterator ignore_iter = ignore_set.find(busi_code);
				if (ignore_iter == ignore_set.end()) {
					ignore_set.insert(busi_code);
					GPP->write_log(__FILE__, __LINE__, (_("WARNING: busi_code {1} in ai_busi_comm_item_rel is missing in unical_datasource_cfg.") % busi_code).str(APPLOCALE));
				}
				continue;
			}

			cal_busi_t& busi_item = busi_iter->second;
			if (!busi_item.valid)
				continue;

			string item_type = rset->getString(2);
			string item_id;
			if (!item_type.empty()) {
				item_id = item_type;
				item_id += ':';
			}
			item_id += rset->getString(3);
			int rel_type = rset->getInt(4);
			if (rel_type < SUBJECT_REL_TYPE_MIN || rel_type > SUBJECT_REL_TYPE_MAX) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: rel_type for busi_code {1} is not correct.") % busi_code).str(APPLOCALE));
				busi_item.valid = false;
				continue;
			}

			cal_busi_subject_t& subject = busi_item.subject;
			if (subject.items.empty()) {
				subject.rel_type = rel_type;
			} else if (subject.rel_type != rel_type) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: all rel_type for busi_code {1} should be identical.") % busi_code).str(APPLOCALE));
				busi_item.valid = false;
				continue;
			}

			subject.items.push_back(item_id);
		}
	} catch (bad_sql& ex) {
		throw bad_msg(__FILE__, __LINE__, 0, ex.what());
	}
}

// 加载纳税人类型税率配置表
void cal_tree::load_chl_taxtype()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif
	map<string, double>& chl_taxtype_map = CALT->_CALT_chl_taxtype_map;
	string sql_stmt = (boost::format("SELECT taxpayer_type{char[%1%]}, tax_value{double} FROM ai_chl_taxtype")
		% TAXPAYER_TYPE_LEN).str();

	try {
		Generic_Database *db = CALT->_CALT_db;
		auto_ptr<Generic_Statement> auto_stmt(db->create_statement());
		Generic_Statement *stmt = auto_stmt.get();

		auto_ptr<struct_dynamic> auto_data(db->create_data(true));
		struct_dynamic *data = auto_data.get();
		if (data == NULL) {
			GPP->write_log(__FILE__, __LINE__, (_("ERROR: Memory allocation failure")).str(APPLOCALE));
			exit(1);
		}

		data->setSQL(sql_stmt);
		stmt->bind(data);

		auto_ptr<Generic_ResultSet> auto_rset(stmt->executeQuery());
		Generic_ResultSet *rset = auto_rset.get();

		while (rset->next()) {
			string taxpayer_type = rset->getString(1);
			double tax_value = rset->getDouble(2);

			chl_taxtype_map[taxpayer_type] = tax_value;
		}
	} catch (bad_sql& ex) {
		throw bad_msg(__FILE__, __LINE__, 0, ex.what());
	}
}

// 设置指标来源表
void cal_tree::init_datasource()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif
	const cal_parms_t& parms = CALP->_CALP_parms;
	/*
	 * 指标定义表来源:
	 * 1. 在本驱动源下定义的指标
	 * 2. 在全局驱动源下定义，但不在本驱动源下定义的指标
	 * 3. 别的驱动源下定义(channel_level = 1)的渠道级指标
	 */
	BOOST_FOREACH(const cal_data_source_t& v, data_source) {
		cal_orign_data_source_t item;
		bool match;

		if (v.driver_data == parms.driver_data) {
			match = true;
		} else if (v.driver_data == "00") {
			match = true;
			BOOST_FOREACH(const cal_data_source_t& v2, data_source) {
				if (v2.driver_data == parms.driver_data
					&& v2.busi_code == v.busi_code
					&& v2.chnl_level == v.chnl_level) {
					match = false;
					break;
				}
			}
		} else {
			match = false;
		}

		while (match) {
			item.busi_code = v.busi_code;
			item.chnl_level = v.chnl_level;
			if (v.chnl_level== CHNL_LEVEL_USER_CHNL) {
				item.chnl_level = CHNL_LEVEL_CHNL;
				item.is_user_chnl_level = true;
			} else {
				item.is_user_chnl_level = false;
			}
			item.driver_data = "";
			item.table_name = v.table_name;
			item.field_name = v.column_name;
			item.deal_type = v.deal_type;
			item.use_default = v.use_default;
			item.default_value = v.default_value;
			item.subject_name = v.subject_name;
			item.is_mix_flag = false;
			item.complex_type = v.complex_type;
			item.multiplicator = v.multiplicator;
			item.busi_type = v.busi_type;

			set<cal_orign_data_source_t>::iterator ds_iter = orign_ds_set.find(item);
			if (ds_iter != orign_ds_set.end()) {
				if (ds_iter->driver_data != item.driver_data)
					break;

				// 指标重复定义
				throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: busi_code {1} is defined more times.") % item.busi_code).str(APPLOCALE));
			}

			// 计算的复合指标，其驱动源都是渠道级的，不需设置
			map<string, string>::const_iterator busi_driver_iter = busi_driver_map.find(item.busi_code);
			if (busi_driver_iter != busi_driver_map.end())
				item.driver_data = busi_driver_iter->second;

			orign_ds_set.insert(item);

			const string& busi_code = item.busi_code;
			if (!busi_code.empty() && *busi_code.begin() == 'P')
				set_data_source(busi_code, busi_code, item);

			break;
		}

		if (v.chnl_level != 1) {
			match = false;
		} else {
			match = true;
			BOOST_FOREACH(const cal_data_source_t& v2, data_source) {
				if ((v2.driver_data == parms.driver_data || v2.driver_data == "00")
					&& v2.busi_code == v.busi_code) {
					match = false;
					break;
				}
			}
		}

		if (match) {
			item.busi_code = v.busi_code;
			item.chnl_level = v.chnl_level;
			if (v.chnl_level== CHNL_LEVEL_USER_CHNL) {
				item.chnl_level = CHNL_LEVEL_CHNL;
				item.is_user_chnl_level = true;
			} else {
				item.is_user_chnl_level = false;
			}
			item.driver_data = "";

			item.table_name = "$$";
			item.field_name = BUSI_VALUE;
			item.deal_type = DEAL_TYPE_DIRECT;
			item.use_default = v.use_default;
			item.default_value = v.default_value;
			item.subject_name = "";
			item.is_mix_flag = false;
			item.complex_type = v.complex_type;
			item.multiplicator = v.multiplicator;
			item.busi_type = v.busi_type;

			if (orign_ds_set.find(item) != orign_ds_set.end())
				continue;

			// 计算的复合指标，其驱动源都是渠道级的，不需设置
			map<string, string>::const_iterator busi_driver_iter = busi_driver_map.find(item.busi_code);
			if (busi_driver_iter != busi_driver_map.end())
				item.driver_data = busi_driver_iter->second;

			orign_ds_set.insert(item);

			const string& busi_code = item.busi_code;
			if (!busi_code.empty() && *busi_code.begin() == 'P')
				set_data_source(busi_code, busi_code, item);
		}
	}
}

// 加载指标来源表
void cal_tree::load_busi_special()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	/*
	 *指标来源来自本地化后的配置表ai_busi_comm_item_rel
	 */
	string sql_stmt = (boost::format("SELECT a.driver_data{char[%1%]}, b.busi_code{char[%2%]}, "
		"a.ref_busi_code{char[%2%]}, a.channel_level{int}, lower(c.item_type){char[%3%]}, "
		"lower(c.item_id){char[%4%]}, a.deal_type{int}, a.use_default{int}, a.default_value{char[%5%]}, "
                "lower(a.subject_name){char[%6%]}, a.complex_type{int}, a.precision{int}, a.busi_type{int} "
                "FROM unical_datasource_cfg a, ai_busi_cfg b, ai_busi_comm_item_rel c "
                "where b.src_busi_code = a.busi_code "
                "and c.busi_code = b.busi_code "
                "and trim(a.table_name) = '$CHNL_FEATURE' "
                "and b.province_code in('09',:province_code{char[%7%]})")
		% DRIVER_DATA_LEN % BUSI_CODE_LEN % TABLE_NAME_SIZE % COLUMN_NAME_SIZE
		% DEFAULT_VALUE_LEN % SUBJECT_ID_LEN % PROVINCE_CODE_LEN).str();

	const cal_parms_t& parms = CALP->_CALP_parms;
	gpenv& env_mgr = gpenv::instance();
	try {
		Generic_Database *db = CALT->_CALT_db;
		auto_ptr<Generic_Statement> auto_stmt(db->create_statement());
		Generic_Statement *stmt = auto_stmt.get();

		auto_ptr<struct_dynamic> auto_data(db->create_data(true));
		struct_dynamic *data = auto_data.get();
		if (data == NULL) {
			GPP->write_log(__FILE__, __LINE__, (_("ERROR: Memory allocation failure")).str(APPLOCALE));
			exit(1);
		}

		data->setSQL(sql_stmt);
		stmt->bind(data);

		stmt->setString(1, parms.province_code);
		auto_ptr<Generic_ResultSet> auto_rset(stmt->executeQuery());
		Generic_ResultSet *rset = auto_rset.get();

		while (rset->next()) {
			cal_data_source_t item;

			item.driver_data = rset->getString(1);
			item.busi_code = rset->getString(2);
			item.ref_busi_code = rset->getString(3);
			item.chnl_level = rset->getInt(4);
			env_mgr.expand_var(item.table_name, rset->getString(5));
			item.column_name = rset->getString(6);

			item.deal_type = rset->getInt(7);
			if (item.deal_type < DEAL_TYPE_MIN || item.deal_type > DEAL_TYPE_MAX)
				throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: deal_type for busi_code {1} is not correct.") % item.busi_code).str(APPLOCALE));

			item.use_default = (rset->getInt(8) == 1);
			item.default_value = rset->getString(9);
			item.subject_name = rset->getString(10);

			item.complex_type = rset->getInt(11);
			if (item.complex_type < COMPLEX_TYPE_MIN || item.complex_type > COMPLEX_TYPE_MAX)
				throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: complex_type for busi_code {1} is not correct.") % item.busi_code).str(APPLOCALE));

			int precision = rset->getInt(12);
			if (precision < 0) {
				item.multiplicator = -1;
			} else {
				item.multiplicator = 1;
				for (int i = 0; i < precision; i++)
					item.multiplicator *= 10;
			}

			item.busi_type = rset->getInt(13);

			//保存渠道特征本地化后的指标
			chnl_feature_busi_set.insert(item.busi_code);

			data_source.push_back(item);
		}
	} catch (bad_sql& ex) {
		throw bad_msg(__FILE__, __LINE__, 0, ex.what());
	}
}


// 设置指标配置表
void cal_tree::init_busi_cfg()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif
	const cal_parms_t& parms = CALP->_CALP_parms;

	/* 有些指标是后台为了方便处理新加的
	 * 在加载时需要补充，以保证正确创建指标
	 */
	vector<cal_rule_busi_source_t> todo_vector;

	BOOST_FOREACH(const cal_busi_cfg_t& v, busi_cfg) {
		// 非个性化指标
		if (!v.busi_local_flag)
			set_busi_cfg(v.busi_code, v.src_busi_code, v.busi_code, v.statistics_object, v.is_mix_flag, v.enable_cycles, todo_vector);

		BOOST_FOREACH(const cal_data_source_t& v2, data_source) {
			if (v2.ref_busi_code.empty())
				continue;

			if (v2.ref_busi_code != v.src_busi_code)
				continue;

			bool match;
			if (v2.driver_data == parms.driver_data) {
				match = true;
			} else if (v2.driver_data == "00") {
				match = true;
				BOOST_FOREACH(const cal_data_source_t& v3, data_source) {
					if (v2.busi_code == v3.busi_code && v3.driver_data == parms.driver_data) {
						match = false;
						break;
					}
				}
			} else {
				match = false;
			}

			if (match)
				set_busi_cfg(v2.busi_code + "_" + v.busi_code, v2.busi_code, v.busi_code, v.statistics_object, v.is_mix_flag, v.enable_cycles, todo_vector);
		}
	}

	BOOST_FOREACH(const cal_data_source_t& v, data_source) {
		if (!v.ref_busi_code.empty())
			continue;

		if (*v.busi_code.begin() == 'P')
			continue;

		bool match = true;
		BOOST_FOREACH(const cal_busi_cfg_t& v2, busi_cfg) {
			if (v.busi_code == v2.busi_code) {
				match = false;
				break;
			}
		}

		if (match)
			set_busi_cfg(v.busi_code, v.busi_code, v.busi_code, v.chnl_level, false, false, todo_vector);
	}

	// 必须在其他指标先创建后才能创建规则指标，
	// 因为规则指标需要引用别的指标
	for (map<string, cal_rule_busi_t>::const_iterator rule_busi_iter = rule_busi_map.begin(); rule_busi_iter != rule_busi_map.end(); ++rule_busi_iter) {
		// 非本地化指标需要创建
		if (!rule_busi_iter->second.busi_local_flag)
			set_data_source(rule_busi_iter->first, rule_busi_iter->first, rule_busi_iter->second);
	}

	for (vector<cal_rule_busi_source_t>::const_iterator todo_iter = todo_vector.begin(); todo_iter != todo_vector.end(); ++todo_iter)
		set_data_source(todo_iter->busi_code, todo_iter->ref_busi_code, todo_iter->rule_busi);
}

void cal_tree::set_null_fields(vector<dbc_field_t>& values)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	for (vector<dbc_field_t>::iterator iter = values.begin(); iter != values.end(); ++iter) {
		field_data_t *field_data = iter->field_data;
		if (field_data->flag & FIELD_DATA_NO_DEFAULT) {
			field_data->data_size = 0;
			field_data->flag |= FIELD_DATA_GOTTEN;
		} else {
			field_data->set_data(field_data->default_value);
			field_data->data_size = 1;
			field_data->flag |= FIELD_DATA_GOTTEN | FIELD_DATA_SET_DEFAULT;
		}
	}
}

void cal_tree::set_data_source(const string& busi_code, const string& ref_busi_code, const cal_rule_busi_t& rule_busi_item)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, (_("busi_code={1}, ref_busi_code={2}") % busi_code % ref_busi_code).str(APPLOCALE), NULL);
#endif

	cal_parms_t& parms = CALP->_CALP_parms;
	map<string, cal_busi_t>& busi_map = CALT->_CALT_busi_map;
	field_data_map_t& data_map = CALT->_CALT_data_map;
	vector<field_data_t *> rela_fields;
	vector<const char *> in_array;
	string dst_code;

	cal_busi_t busi_item;
	busi_item.flag |= BUSI_DEAL_FUNCTION;
	map<string, string>::const_iterator busi_driver_iter = busi_driver_map.find(busi_code);
	if (busi_driver_iter != busi_driver_map.end())
		busi_item.driver_data = busi_driver_iter->second;

	if (!parse_rule_busi(busi_code, rule_busi_item.rule_busi, rule_busi_item.field_type, rela_fields, in_array, dst_code, busi_item.flag, busi_item.busi_type, rule_busi_item.busi_local_flag)) {
		// 渠道级的规则指标，要设置为复合指标
		if (busi_item.flag & BUSI_CHANNEL_LEVEL)
			busi_item.complex_type = COMPLEX_TYPE_FUNC;
		else
			busi_item.complex_type = COMPLEX_TYPE_NONE;
		busi_item.enable_cycles = false;
		busi_item.valid = false;
		busi_map[busi_code] = busi_item;
		if(parms.channel_level && strncmp(busi_code.c_str(), "~", 1) == 0)
			busi_ignore_set.insert(busi_code);
		return;
	}

	// 渠道级的规则指标，要设置为复合指标
	if (busi_item.flag & BUSI_CHANNEL_LEVEL)
		busi_item.complex_type = COMPLEX_TYPE_FUNC;
	else
		busi_item.complex_type = COMPLEX_TYPE_NONE;

	// 分配内存
	field_data_t *item = new field_data_t(FIELD_DATA_RESET, busi_code, ref_busi_code, rule_busi_item.field_type,
		rule_busi_item.field_size, -1, -1, -1, DEAL_TYPE_FUNCTION);
	item->rela_fields = rela_fields;
	if (!in_array.empty()) {
		item->in_array = new const char *[in_array.size()];
		memcpy(item->in_array, &in_array[0], sizeof(const char *) * in_array.size());
	}

	compiler& cmpl = SAP->_SAP_cmpl;
	item->func_idx = cmpl.create_function(dst_code, map<string, int>(), map<string, int>(), map<string, int>());

	// 对于复合指标，字段增加后缀
	string field_name = item->busi_code + "#";
	data_map[field_name] = item;

	busi_item.field_name = field_name;
	busi_item.field_data = item;

	if (rule_busi_item.precision < 0) {
		busi_item.multiplicator = -1;
	} else {
		busi_item.multiplicator = 1;
		for (int i = 0; i < rule_busi_item.precision; i++)
			busi_item.multiplicator *= 10;
	}
	busi_item.enable_cycles = false;
	busi_map[busi_code] = busi_item;
}

// 根据表名和需要的结果字段，找到需要查找的表ID
void cal_tree::set_data_source(const string& busi_code, const string& ref_busi_code, const cal_orign_data_source_t& orign_ds)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, (_("busi_code={1}, ref_busi_code={2}") % busi_code % ref_busi_code).str(APPLOCALE), NULL);
#endif

	const string& table_name = orign_ds.table_name;
	const int& deal_type = orign_ds.deal_type;

	if (deal_type < DEAL_TYPE_MIN || deal_type > DEAL_TYPE_MAX) {
		GPP->write_log(__FILE__, __LINE__, (_("ERROR: deal_type for busi_code {1} is not supported.") % orign_ds.busi_code).str(APPLOCALE));
		return;
	}

	if (table_name.empty()) // 指标来源于驱动源
		set_from_source(busi_code, ref_busi_code, orign_ds);
	else
		set_from_table(busi_code, ref_busi_code, orign_ds);
}

// 设置来源于输入的指标
void cal_tree::set_from_source(const string& busi_code, const string& ref_busi_code, const cal_orign_data_source_t& orign_ds)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, (_("busi_code={1}, ref_busi_code={2}") % busi_code % ref_busi_code).str(APPLOCALE), NULL);
#endif
	cal_parms_t& parms = CALP->_CALP_parms;
	map<string, cal_busi_t>& busi_map = CALT->_CALT_busi_map;
	field_data_map_t& data_map = CALT->_CALT_data_map;
	const int& deal_type = orign_ds.deal_type;
	const string& subject_name = orign_ds.subject_name;
	const string& field_name = orign_ds.field_name;
	field_data_t *item1 = NULL;
	field_data_t *item2 = NULL;
	field_data_t *item3;

	// 从输入中查找科目和字段位置
	int subject_index = -1;
	int value_index = -1;
	string full_subject_name = string("input.") + subject_name;
	string full_field_name = string("input.") + field_name;
	const vector<input_field_t>& fields = SAP->_SAP_adaptors[0].input_records[0].field_vector;
	for (vector<input_field_t>::const_iterator iter = fields.begin(); iter != fields.end(); ++iter) {
		if (iter->field_name == full_subject_name)
			subject_index = iter - fields.begin();
		else if (iter->field_name == full_field_name)
			value_index = iter - fields.begin();
	}

	// 初始化指标值
	cal_busi_t busi_item;
	if (orign_ds.is_mix_flag)
		busi_item.flag |= BUSI_IS_MIXED;
	if (orign_ds.chnl_level == CHNL_LEVEL_CHNL)
		busi_item.flag |= BUSI_CHANNEL_LEVEL;
	else if (orign_ds.chnl_level == CHNL_LEVEL_ACCT)
		busi_item.flag |= BUSI_ACCT_LEVEL;
	if (orign_ds.is_user_chnl_level)
		busi_item.flag |= BUSI_USER_CHNL_LEVEL;
	busi_item.driver_data = orign_ds.driver_data;
	busi_item.complex_type = orign_ds.complex_type;
	busi_item.multiplicator = orign_ds.multiplicator;
	busi_item.busi_type = orign_ds.busi_type;
	busi_item.enable_cycles = orign_ds.enable_cycles;

	if (deal_type != DEAL_TYPE_COUNT && value_index == -1) {
		GPP->write_log(__FILE__, __LINE__, (_("ERROR: field_name {1} is not defined in input section, busi_code {2}") % field_name % busi_code).str(APPLOCALE));
		busi_item.valid = false;
		busi_map[busi_code] = busi_item;
		return;
	}

	if (deal_type == DEAL_TYPE_DIRECT || deal_type == DEAL_TYPE_SUM) {
		field_data_map_t::iterator data_iter = data_map.find(field_name);
		if (data_iter == data_map.end()) {
			GPP->write_log(__FILE__, __LINE__, (_("ERROR: field_name {1} is not defined yet, busi_code {2}") % field_name % busi_code).str(APPLOCALE));
			busi_item.valid = false;
			busi_map[busi_code] = busi_item;
			return;
		}
		item1 = data_iter->second;
	}



	switch (deal_type) {
	case DEAL_TYPE_DIRECT:
		if (!subject_name.empty() && subject_index == -1) {
			GPP->write_log(__FILE__, __LINE__, (_("ERROR: busi_code {1}, subject_name {2} is not found in data source.") % busi_code % subject_name).str(APPLOCALE));
			busi_item.valid = false;
			busi_map[busi_code] = busi_item;
			return;
		}
		if (subject_index != -1) {
			// 指定科目的自定义指标
			item2 = new field_data_t(FIELD_DATA_RESET,
				busi_code, ref_busi_code, item1->field_type, item1->field_size,
				-1, value_index, subject_index, DEAL_TYPE_FILTER, true);

			string sub_field_name = busi_code + "#";
			data_map[sub_field_name] = item2;

			busi_item.field_name = sub_field_name;
			busi_item.field_data = item2;
			/* 允许一个字段对应对个指标，为考核并计算到管理本部使用
		} else if (item1->flag & FIELD_DATA_BELONG_BUSI) {
			GPP->write_log(__FILE__, __LINE__, (_("ERROR: busi_code {1}, field_name {2} is conflict with busi_code {3} since they are from the same data source.") % busi_code % field_name % item1->busi_code).str(APPLOCALE));
			busi_item.valid = false;
			busi_map[busi_code] = busi_item;
			break;
			*/
		} else {
			item1->flag |= FIELD_DATA_BELONG_BUSI;
			item1->busi_code = busi_code;
			item1->ref_busi_code = ref_busi_code;
			item1->value_index = -1;
			item1->subject_index = -1;
			item1->deal_type = deal_type;

			busi_item.field_name = field_name;
			busi_item.field_data = item1;
		}

		if (subject_index != -1)
			busi_item.flag |= BUSI_USER_DEFINED;
		busi_item.flag |= BUSI_FROM_SOURCE | BUSI_DEAL_DIRECT;
		busi_map[busi_code] = busi_item;
		break;
	case DEAL_TYPE_SUM:
		if (parms.stage != 3) {
			if (!subject_name.empty() && subject_index == -1) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: busi_code {1}, subject_name {2} is not found in data source.") % busi_code % subject_name).str(APPLOCALE));
				busi_item.valid = false;
				busi_map[busi_code] = busi_item;
				return;
			}
			if (subject_index != -1) {
				// 指定科目的自定义指标
				item2 = new field_data_t(FIELD_DATA_RESET,
					busi_code, ref_busi_code, item1->field_type, item1->field_size,
					-1, value_index, subject_index, DEAL_TYPE_FILTER, true);

				string sub_field_name = busi_code + "#";
				data_map[sub_field_name] = item2;

				busi_item.field_name = sub_field_name;
				busi_item.field_data = item2;
			} else if (item1->flag & FIELD_DATA_BELONG_BUSI) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: busi_code {1}, field_name {2} is conflict with busi_code {3} since they are from the same data source.") % busi_code % field_name % item1->busi_code).str(APPLOCALE));
				busi_item.valid = false;
				busi_map[busi_code] = busi_item;
				break;
			} else {
				item1->flag |= FIELD_DATA_BELONG_BUSI;
				item1->busi_code = busi_code;
				item1->ref_busi_code = ref_busi_code;
				item1->value_index = value_index;
				item1->subject_index = subject_index;
				item1->deal_type = deal_type;

				busi_item.field_name = field_name;
				busi_item.field_data = item1;
			}

			if (subject_index != -1)
				busi_item.flag |= BUSI_USER_DEFINED;
			busi_item.flag |= BUSI_FROM_SOURCE | BUSI_DEAL_SUM;
			if (!orign_ds.is_mix_flag)
				busi_item.flag |= BUSI_ALLOCATABLE;
			busi_map[busi_code] = busi_item;
		} else {
			/*
			 * 第三阶段时，这类指标需要生成两个指标
			 * 一个为累计指标，从指标表获取，该指标作为条件参考
			 * 另一个为原始值指标，从输入记录获取，该指标作为计算参考
			 */
			if (orign_ds.chnl_level == CHNL_LEVEL_CHNL) {
				if (busi_table_id == -1)
					throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Can't find table to get {1}") % BUSI_VALUE).str(APPLOCALE));

				/*
				 * 累计指标没有字段存储空间，需要创建
				 * 创建时其字段名称为指标值+ "#"，以与正常的字段名称相区别
				 * 属性的类型为double
				 */
				item2 = new field_data_t(FIELD_DATA_RESET, busi_code, ref_busi_code, SQLTYPE_DOUBLE,
					sizeof(double), busi_table_id, busi_value_index, -1, DEAL_TYPE_DIRECT);
			}
			if (orign_ds.chnl_level == CHNL_LEVEL_ACCT) {
				if (acct_busi_table_id == -1)
					throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Can't find table to get {1}") % ACCT_BUSI_VALUE).str(APPLOCALE));

				/*
				 * 累计指标没有字段存储空间，需要创建
				 * 创建时其字段名称为指标值+ "#"，以与正常的字段名称相区别
				 * 属性的类型为double
				 */
				item2 = new field_data_t(FIELD_DATA_RESET, busi_code, ref_busi_code, SQLTYPE_DOUBLE,
					sizeof(double), acct_busi_table_id, acct_busi_value_index, -1, DEAL_TYPE_DIRECT);
			}
			string sub_field_name = busi_code + "$";
			data_map[sub_field_name] = item2;

			busi_item.flag &= ~BUSI_CHANNEL_LEVEL;
			busi_item.flag &= ~BUSI_ACCT_LEVEL;
			busi_item.flag |= BUSI_DYNAMIC | BUSI_FROM_TABLE | BUSI_DEAL_DIRECT;
			if (!orign_ds.is_mix_flag)
				busi_item.flag |= BUSI_ALLOCATABLE;
			busi_item.field_name = sub_field_name;
			busi_item.field_data = item2;

			// 指标添加后缀，以与其他指标区别，表示来源于输入的原始指标
			// 同时需要保证，各个客户化的指标，其原始指标指向同一个值
			string sub_busi_code = busi_code + "#";

			busi_item.sub_busi_code = sub_busi_code;
			busi_map[busi_code] = busi_item;

			if (subject_name.empty()) {
				item3 = item1;

				// 已经赋值则跳过
				if (item3->flag & FIELD_DATA_BELONG_BUSI)
					break;

				item3->flag |= FIELD_DATA_BELONG_BUSI;
				item3->busi_code = sub_busi_code;
				item3->ref_busi_code = ref_busi_code;
				item3->deal_type = DEAL_TYPE_DIRECT;
				item3->flag &= ~FIELD_DATA_NO_DEFAULT; // 指标相关的字段都有默认值
				busi_item.field_name = field_name;
			} else {
				// 指定科目的自定义指标
				item3 = new field_data_t(FIELD_DATA_RESET,
					sub_busi_code, ref_busi_code, item1->field_type, item1->field_size,
					-1, value_index, subject_index, DEAL_TYPE_FILTER, true);

				data_map[sub_busi_code] = item3;
				busi_item.field_name = sub_busi_code;
			}

			// 设置渠道级汇总指标的子指标
			item2->ref_busi_code = item3->busi_code;

			busi_item.flag = BUSI_FROM_SOURCE | BUSI_DEAL_DIRECT;
			busi_item.complex_type = COMPLEX_TYPE_NONE;
			busi_item.field_data = item3;
			busi_item.sub_busi_code = "";
			busi_map[sub_busi_code] = busi_item;
		}
		break;
	case DEAL_TYPE_COUNT:
		if (parms.stage != 3) {
			if (!subject_name.empty()) {
				if (subject_index == -1) {
					GPP->write_log(__FILE__, __LINE__, (_("ERROR: busi_code {1}, subject_name {2} is not found in data source.") % busi_code % subject_name).str(APPLOCALE));
					busi_item.valid = false;
					busi_map[busi_code] = busi_item;
					break;
				}

				// 指定科目的自定义指标
				item2 = new field_data_t(FIELD_DATA_RESET,
					busi_code, ref_busi_code, SQLTYPE_INT, sizeof(int),
					-1, value_index, subject_index, DEAL_TYPE_FILTER, true, "", deal_type);

				string sub_field_name = busi_code + "#";
				data_map[sub_field_name] = item2;

				busi_item.field_name = sub_field_name;
				busi_item.field_data = item2;
			} else {

				// 原始指标，可分摊的对象
				item2 = new field_data_t(FIELD_DATA_NO_DEFAULT | FIELD_DATA_GOTTEN, busi_code, ref_busi_code,
					SQLTYPE_INT, sizeof(int), -1, -1, -1, deal_type);

				*reinterpret_cast<int *>(item2->data) = 1;
				string sub_field_name = busi_code + "@";
				data_map[sub_field_name] = item2;

				busi_item.field_name = sub_field_name;
				busi_item.field_data = item2;
			}

			/*
			// 原始指标，可分摊的对象
			item1 = new field_data_t(FIELD_DATA_NO_DEFAULT | FIELD_DATA_GOTTEN, busi_code, ref_busi_code,
				SQLTYPE_INT, sizeof(int), -1, -1, -1, deal_type);

			*reinterpret_cast<int *>(item1->data) = 1;
			string sub_field_name = busi_code + "@";
			data_map[sub_field_name] = item1;
			*/

			if (subject_index != -1)
				busi_item.flag |= BUSI_USER_DEFINED;
			busi_item.flag |= BUSI_FROM_SOURCE | BUSI_DEAL_COUNT;
			if (!orign_ds.is_mix_flag)
				busi_item.flag |= BUSI_ALLOCATABLE;
			//busi_item.field_name = sub_field_name;
			//busi_item.field_data = item1;
			busi_map[busi_code] = busi_item;
		} else {
			/*
			 * 第三阶段时，这类指标需要生成两个指标
			 * 一个为累计指标，从指标表获取，该指标作为条件参考
			 * 另一个为原始值指标，从输入记录获取，该指标作为计算参考
			 */
			if (orign_ds.chnl_level == CHNL_LEVEL_CHNL) {
				if (busi_table_id == -1)
					throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Can't find table to get {1}") % BUSI_VALUE).str(APPLOCALE));

				/*
				 * 累计指标没有字段存储空间，需要创建
				 * 创建时其字段名称为指标值+ "#"，以与正常的字段名称相区别
				 * 属性的类型为int
				 */
				item2 = new field_data_t(FIELD_DATA_RESET, busi_code, ref_busi_code, SQLTYPE_INT,
					sizeof(int), busi_table_id, busi_value_index, -1, DEAL_TYPE_DIRECT);
			}
			if (orign_ds.chnl_level == CHNL_LEVEL_ACCT) {
				if (acct_busi_table_id == -1)
					throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Can't find table to get {1}") % ACCT_BUSI_VALUE).str(APPLOCALE));

				/*
				 * 累计指标没有字段存储空间，需要创建
				 * 创建时其字段名称为指标值+ "#"，以与正常的字段名称相区别
				 * 属性的类型为int
				 */
				item2 = new field_data_t(FIELD_DATA_RESET, busi_code, ref_busi_code, SQLTYPE_INT,
					sizeof(int), acct_busi_table_id, acct_busi_value_index, -1, DEAL_TYPE_DIRECT);
			}

			if (item2 != NULL) {
				string sub_field_name = item2->busi_code + "$";
				data_map[sub_field_name] = item2;

				busi_item.flag &= ~BUSI_CHANNEL_LEVEL;
				busi_item.flag &= ~BUSI_ACCT_LEVEL;
				busi_item.flag |= BUSI_DYNAMIC | BUSI_FROM_TABLE | BUSI_DEAL_DIRECT;
				if (!orign_ds.is_mix_flag)
					busi_item.flag |= BUSI_ALLOCATABLE;
				busi_item.field_name = sub_field_name;
				busi_item.field_data= item2;

				// 指标添加后缀，以与其他指标区别，表示来源于输入的原始指标
				// 同时需要保证，各个客户化的指标，其原始指标指向同一个值
				string sub_busi_code = busi_code + "#";

				// 设置渠道级汇总指标的子指标
				busi_item.sub_busi_code = sub_busi_code;
				busi_map[busi_code] = busi_item;

				if (subject_name.empty()) {
					// 原始指标，可分摊的对象
					item3 = new field_data_t(FIELD_DATA_GOTTEN, sub_busi_code, "",
						SQLTYPE_INT, sizeof(int), -1, -1, -1, DEAL_TYPE_DIRECT);

					*reinterpret_cast<int *>(item3->data) = 1;
				} else {
					// 指定科目的自定义指标
					item3 = new field_data_t(FIELD_DATA_RESET,
						sub_busi_code, ref_busi_code, SQLTYPE_INT, sizeof(int),
						-1, value_index, subject_index, DEAL_TYPE_FILTER, true);
				}

				data_map[sub_busi_code] = item3;
				busi_item.field_name = sub_busi_code;
				busi_item.flag = BUSI_FROM_SOURCE | BUSI_DEAL_DIRECT;
				busi_item.complex_type = COMPLEX_TYPE_NONE;
				busi_item.field_data = item3;
				busi_item.sub_busi_code = "";
				busi_map[sub_busi_code] = busi_item;
			} else {
				if (subject_name.empty()) {
					// 原始指标，可分摊的对象
					item3 = new field_data_t(FIELD_DATA_GOTTEN, busi_code, "",
						SQLTYPE_INT, sizeof(int), -1, -1, -1, DEAL_TYPE_DIRECT);

					*reinterpret_cast<int *>(item3->data) = 1;
				} else {
					// 指定科目的自定义指标
					item3 = new field_data_t(FIELD_DATA_RESET,
						busi_code, ref_busi_code, SQLTYPE_INT, sizeof(int),
						-1, value_index, subject_index, DEAL_TYPE_FILTER, true);
				}

				string field_name = busi_code + "$";
				data_map[field_name] = item3;
				busi_item.field_name = field_name;
				busi_item.flag = BUSI_FROM_SOURCE | BUSI_DEAL_DIRECT | BUSI_ALLOCATABLE;
				busi_item.complex_type = COMPLEX_TYPE_NONE;
				busi_item.field_data = item3;
				busi_item.sub_busi_code = "";
				busi_map[busi_code] = busi_item;
			}
		}
		break;
	default:
		GPP->write_log(__FILE__, __LINE__, (_("ERROR: Unsupported deal_type {1} for busi_code {2}, only DEAL_TYPE_DIRECT, DEAL_TYPE_COUNT, or DEAL_TYPE_SUM is allowed.") % deal_type % busi_code).str(APPLOCALE));
		busi_item.valid = false;
		busi_map[busi_code] = busi_item;
		break;
	}
}

// 设置来源于表的指标
void cal_tree::set_from_table(const string& busi_code, const string& ref_busi_code, const cal_orign_data_source_t& orign_ds)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, (_("busi_code={1}, ref_busi_code={2}") % busi_code % ref_busi_code).str(APPLOCALE), NULL);
#endif
	cal_parms_t& parms = CALP->_CALP_parms;
	map<string, cal_busi_t>& busi_map = CALT->_CALT_busi_map;
	field_data_map_t& data_map = CALT->_CALT_data_map;
	string table_name = orign_ds.table_name;
	const string& field_name = orign_ds.field_name;
	const int& deal_type = orign_ds.deal_type;
	const bool& use_default = orign_ds.use_default;
	const string& default_value = orign_ds.default_value;
	field_data_t *field_data;

	cal_busi_t busi_item;
	if (orign_ds.is_mix_flag)
		busi_item.flag |= BUSI_IS_MIXED;
	if (orign_ds.chnl_level == CHNL_LEVEL_CHNL)
		busi_item.flag |= BUSI_CHANNEL_LEVEL;
	else if (orign_ds.chnl_level == CHNL_LEVEL_ACCT)
		busi_item.flag |= BUSI_ACCT_LEVEL;
	if (orign_ds.is_user_chnl_level)
		busi_item.flag |= BUSI_USER_CHNL_LEVEL;
	busi_item.driver_data = orign_ds.driver_data;
	busi_item.complex_type = orign_ds.complex_type;
	busi_item.multiplicator = orign_ds.multiplicator;
	busi_item.busi_type = orign_ds.busi_type;
	busi_item.enable_cycles = orign_ds.enable_cycles;

	vector<dbc_field_t>::iterator field_iter;
	bool is_dynamic;

	if (((field_name == BUSI_VALUE || field_name== ACCT_BUSI_VALUE) && parms.stage != 3) || (orign_ds.chnl_level != CHNL_LEVEL_NONE && orign_ds.is_user_chnl_level != true && parms.stage == 3))
		is_dynamic = true;
	else
		is_dynamic = false;

	if (is_dynamic) {
		if (orign_ds.chnl_level == CHNL_LEVEL_CHNL) {
			if (busi_table_id == -1) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't find busi table to get busi_code {1}") % busi_code).str(APPLOCALE));
				busi_item.valid = false;
				busi_map[busi_code] = busi_item;
				return;
			}

			table_name = table_info_map[busi_table_id].table_name;
		}
		if (orign_ds.chnl_level == CHNL_LEVEL_ACCT) {
			if (acct_busi_table_id == -1) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: Can't find busi table to get busi_code {1}") % busi_code).str(APPLOCALE));
				busi_item.valid = false;
				busi_map[busi_code] = busi_item;
				return;
			}

			table_name = table_info_map[acct_busi_table_id].table_name;
		}
	}

	map<string, int>::const_iterator name_iter = table_name_map.find(table_name);
	if (name_iter == table_name_map.end()) {
		GPP->write_log(__FILE__, __LINE__, (_("ERROR: busi_code {1} is invalid since we can't find its source table {2} in shared memory definition.") % busi_code % table_name).str(APPLOCALE));
		busi_item.valid = false;
		busi_map[busi_code] = busi_item;
		return;
	}

	dbc_info_t& dbc_info = table_info_map[name_iter->second];
	if (!dbc_info.valid) {
		GPP->write_log(__FILE__, __LINE__, (_("WARNING: busi_code {1} is invalid since its related source table {2} is invalid.") % busi_code % table_name).str(APPLOCALE));
		busi_item.valid = false;
		busi_map[busi_code] = busi_item;
		return;
	}

	vector<dbc_field_t>& values = dbc_info.values;

	// 如果来源于指标表
	if (is_dynamic) {
		/*
		 * 指标属性没有字段存储空间，需要创建
		 * 创建时其字段名称为指标值+ "#"，以与正常的字段名称相区别
		 * 属性的类型为int，实际上，其值只可能为0或1
		 */
		field_data_t *item = NULL;
		if (orign_ds.chnl_level == CHNL_LEVEL_CHNL) {
			item = new field_data_t(FIELD_DATA_RESET, busi_code, ref_busi_code,
				SQLTYPE_DOUBLE, sizeof(double), name_iter->second, busi_value_index, -1,
				deal_type, use_default, default_value);
		} else if (orign_ds.chnl_level == CHNL_LEVEL_ACCT) {
			item = new field_data_t(FIELD_DATA_RESET, busi_code, ref_busi_code,
				SQLTYPE_DOUBLE, sizeof(double), name_iter->second, acct_busi_value_index, -1,
				deal_type, use_default, default_value);
		}

		string sub_field_name = busi_code + "$";
		data_map[sub_field_name] = item;

		busi_item.flag &= ~BUSI_CHANNEL_LEVEL;
		//busi_item.flag &= ~BUSI_ACCT_LEVEL;
		busi_item.flag |= BUSI_DYNAMIC | BUSI_FROM_TABLE | BUSI_DEAL_DIRECT;
		busi_item.field_name = sub_field_name;
		busi_item.field_data = item;
		busi_map[busi_code] = busi_item;
		return;
	}

	switch (deal_type) { // 用户扩展属性
	case DEAL_TYPE_DIRECT:
		{
			for (field_iter = values.begin(); field_iter != values.end(); ++field_iter) {
				if (field_iter->field_name == field_name)
					break;
			}

			if (field_iter == values.end()) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: busi_code {1}, field_name {2} is not defined in table {3}") % busi_code % field_name % table_name).str(APPLOCALE));
				busi_item.valid = false;
				break;
			}

			field_data = field_iter->field_data;
			map<int, dbc_info_t>::iterator table_info_iter = table_info_map.find(field_data->table_id);
			dbc_info_t& dbc_info = table_info_iter->second;
			if (dbc_info.subject_index == -1) {
				if (field_data->flag & FIELD_DATA_BELONG_BUSI) {
					GPP->write_log(__FILE__, __LINE__, (_("ERROR: busi_code {1}, field_name {2} is conflict with busi_code {3} since they are from the same data source.") % busi_code % field_name % field_data->busi_code).str(APPLOCALE));
					busi_item.valid = false;
					break;
				}

				if (orign_ds.use_default) {
					field_data->flag |= FIELD_DATA_USE_DEFAULT;
					field_data->flag &= ~FIELD_DATA_NO_DEFAULT;
				}
				field_data->flag |= FIELD_DATA_BELONG_BUSI;
				field_data->busi_code = busi_code;
				field_data->ref_busi_code = ref_busi_code;
				field_data->deal_type = deal_type;
				field_data->flag |= FIELD_DATA_RESET;

				busi_item.flag |= BUSI_FROM_TABLE | BUSI_DEAL_DIRECT;
				busi_item.field_name = field_name;
				busi_item.field_data = field_data;
			} else {
				/*
				 * 来自于表的自定义指标需要新分配内存，
				 * 这是因为不同的指标可能来自于同一个表的同一个字段，
				 * 而指标有其自定义选项
				 */
				field_data_t *item = new field_data_t(FIELD_DATA_RESET, busi_code,
					ref_busi_code, field_data->field_type, field_data->field_size, field_data->table_id,
					field_data->value_index, dbc_info.subject_index, DEAL_TYPE_FILTER, true);

				string sub_field_name = busi_code + "#";
				data_map[sub_field_name] = item;

				busi_item.flag |= BUSI_USER_DEFINED | BUSI_FROM_TABLE | BUSI_DEAL_DIRECT;
				busi_item.field_name = field_name;
				busi_item.field_data = item;
			}
			break;
		}
	case DEAL_TYPE_SUM:
		{
			if (dbc_info.subject_index == -1) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: busi_code {1} is defined as DEAL_TYPE_SUM or DEAL_TYPE_COUNT, but its source table {2} doesn't have field_name subject_id.") % busi_code % table_name).str(APPLOCALE));
				busi_item.valid = false;
				break;
			}

			int value_index = -1;
			for (field_iter = values.begin(); field_iter != values.end(); ++field_iter) {
				if (field_iter->field_name == field_name) {
					value_index = field_iter - values.begin();
					break;
				}
			}

			if (field_iter == values.end()) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: busi_code {1}, field_name {2} is not defined in table {3}") % busi_code % field_name % table_name).str(APPLOCALE));
				busi_item.valid = false;
				break;
			}

			/*
			 * 来自于表的自定义指标需要新分配内存，
			 * 这是因为不同的指标可能来自于同一个表的同一个字段，
			 * 而指标有其自定义选项
			 */
			field_data_t *field_data = field_iter->field_data;
			field_data_t *item = new field_data_t(FIELD_DATA_RESET | FIELD_DATA_NO_DEFAULT, busi_code,
				ref_busi_code, field_data->field_type, field_data->field_size, field_data->table_id,
				field_data->value_index, field_data->subject_index, deal_type, use_default, default_value);

			string sub_field_name = busi_code + "$";
			data_map[sub_field_name] = item;

			busi_item.flag |= BUSI_USER_DEFINED | BUSI_FROM_TABLE | BUSI_DEAL_SUM;
			busi_item.field_name = sub_field_name;
			busi_item.field_data = item;
			break;
		}
	case DEAL_TYPE_COUNT:
		{
			if (dbc_info.subject_index == -1) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: busi_code {1} is defined as DEAL_TYPE_SUM or DEAL_TYPE_COUNT, but its source table {2} doesn't have field_name subject_id.") % busi_code % table_name).str(APPLOCALE));
				busi_item.valid = false;
				break;
			}

			/*
			 * 来自于表的自定义指标需要新分配内存，
			 * 这是因为不同的指标可能来自于同一个表的同一个字段，
			 * 而指标有其自定义选项
			 */
			field_data_t *item = new field_data_t(FIELD_DATA_RESET | FIELD_DATA_NO_DEFAULT, busi_code,
				ref_busi_code, SQLTYPE_INT, sizeof(int), name_iter->second,
				-1, -1, deal_type, use_default, default_value);

			string sub_field_name = busi_code + "$";
			data_map[sub_field_name] = item;

			busi_item.flag |= BUSI_USER_DEFINED | BUSI_FROM_TABLE | BUSI_DEAL_COUNT;
			busi_item.field_name = sub_field_name;
			busi_item.field_data = item;
			break;
		}
	case DEAL_TYPE_ATTR:
		{
			if (dbc_info.subject_index == -1) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: busi_code {1} is defined as DEAL_TYPE_ATTR, but its source table doesn't have field_name subject_id.") % busi_code).str(APPLOCALE));
				busi_item.valid = false;
			}

			/*
			 * 用户扩展属性没有字段存储空间，需要创建
			 * 创建时其字段名称为指标值+ "#"，以与正常的字段名称相区别
			 * 属性的类型为int，实际上，其值只可能为0或1
			 */
			field_data_t *item = new field_data_t(FIELD_DATA_RESET | FIELD_DATA_NO_DEFAULT, busi_code,
				ref_busi_code, SQLTYPE_INT, sizeof(int), name_iter->second,
				-1, -1, deal_type, use_default, default_value);

			string sub_field_name = busi_code + "$";
			data_map[sub_field_name] = item;

			busi_item.flag |= BUSI_USER_DEFINED | BUSI_FROM_TABLE | BUSI_DEAL_ATTR;
			busi_item.field_name = sub_field_name;
			busi_item.field_data = item;
			break;
		}
	case DEAL_TYPE_BOOL:
		{
			/*
			 * 是否存在状态属性没有字段存储空间，需要创建
			 * 创建时其字段名称为指标值+ "#"，以与正常的字段名称相区别
			 * 属性的类型为int，实际上，其值只可能为0或1
			 */
			field_data_t *item = new field_data_t(FIELD_DATA_RESET | FIELD_DATA_NO_DEFAULT, busi_code,
				ref_busi_code, SQLTYPE_INT, sizeof(int), name_iter->second,
				-1, -1, deal_type, use_default, default_value);

			string sub_field_name = busi_code + "$";
			data_map[sub_field_name] = item;

			busi_item.flag |= BUSI_FROM_TABLE | BUSI_DEAL_BOOL;
			busi_item.field_name = sub_field_name;
			busi_item.field_data = item;
			break;
		}
	default:
		GPP->write_log(__FILE__, __LINE__, (_("ERROR: Unsupported deal_type {1} for busi_code {2}") % deal_type % busi_code).str(APPLOCALE));
		busi_item.valid = false;
		break;
	}

	busi_map[busi_code] = busi_item;
}

void cal_tree::set_busi_cfg(const string& busi_code, const string& src_busi_code, const string& ref_busi_code,
	int chnl_level, bool is_mix_flag, bool enable_cycles, vector<cal_rule_busi_source_t>& todo_vector)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, (_("busi_code={1}, src_busi_code={2}, ref_busi_code={3}, chnl_level={4}, is_mix_flag={5}, enable_cycles={6}") % busi_code % src_busi_code % ref_busi_code % chnl_level % is_mix_flag % enable_cycles).str(APPLOCALE), NULL);
#endif
	cal_orign_data_source_t key;
	set<string>::iterator chnl_feature_iter;

	if (!src_busi_code.empty()) {
		// 如果指标模板在忽略范围内，则其个性化指标也在忽略范围
		if (ignored(src_busi_code))
			busi_ignore_set.insert(busi_code);

		map<string, string>::const_iterator busi_driver_iter = busi_driver_map.find(src_busi_code);
		if (busi_driver_iter != busi_driver_map.end())
			busi_driver_map[busi_code] = busi_driver_iter->second;

		//将渠道特征本地化后的指标认为是基础指标
                chnl_feature_iter = chnl_feature_busi_set.find(busi_code);
		if(chnl_feature_iter == chnl_feature_busi_set.end())
			key.busi_code = src_busi_code;
		else
			key.busi_code = busi_code;
	} else {
		key.busi_code = busi_code;
	}

	key.chnl_level = chnl_level;

	set<cal_orign_data_source_t>::iterator ds_iter = orign_ds_set.find(key);
	if (ds_iter == orign_ds_set.end()) {
		map<string, cal_rule_busi_t>::const_iterator rule_busi_iter = rule_busi_map.find(key.busi_code);
		if (rule_busi_iter == rule_busi_map.end()) {
			if (src_busi_code.empty())
				GPP->write_log(__FILE__, __LINE__, (_("WARNING: busi_code {1} is not defined in unical_datasource_cfg/unical_rule_busi.") % busi_code).str(APPLOCALE));
			else
				GPP->write_log(__FILE__, __LINE__, (_("WARNING: busi_code {1}, src_busi_code {2} is not defined in unical_datasource_cfg/unical_rule_busi.") % busi_code % src_busi_code).str(APPLOCALE));
			return;
		}

		cal_rule_busi_source_t todo_item;
		todo_item.busi_code = busi_code;
		todo_item.ref_busi_code = ref_busi_code;
		todo_item.rule_busi = rule_busi_iter->second;
		todo_vector.push_back(todo_item);
	} else {
		cal_orign_data_source_t& orign_ds = const_cast<cal_orign_data_source_t&>(*ds_iter);
		orign_ds.is_mix_flag = is_mix_flag;
		orign_ds.enable_cycles = enable_cycles;
		if (orign_ds.is_mix_flag && orign_ds.chnl_level ==  CHNL_LEVEL_NONE) {
			GPP->write_log(__FILE__, __LINE__, (_("ERROR: busi_code {1} is complex busi, but is not channel level or account level.") % busi_code).str(APPLOCALE));
			return;
		}

		set_data_source(busi_code, ref_busi_code, orign_ds);
	}
}

bool cal_tree::has_orign(const dbc_field_t& field, bool enlarge)
{
	bool retval = false;
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, (_("enlarge={1}") % enlarge).str(APPLOCALE), NULL);
#endif

	field_data_map_t& data_map = CALT->_CALT_data_map;
	field_data_map_t::iterator data_iter = data_map.find(field.field_name);
	if (data_iter != data_map.end()) {
		field_data_t *field_data = data_iter->second;
		if (field_data->field_type != field.field_type || field_data->field_size != field.field_size) {
			if (field_data->field_type == field.field_type && is_global(field.field_name)) {
				// 全局变量需要重新调整长度
				if (enlarge) {
					delete []field_data->data;
					field_data->field_size = field.field_size;
					field_data->data = new char[field_data->field_size];
				}
				retval = true;
				return retval;
			}

			throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: field_name {1} is defined as different field_type/field_size.") % field.field_name).str(APPLOCALE));
		}

		retval = true;
		return retval;
	}

	map<int, dbc_info_t>::iterator table_info_iter;
	for (table_info_iter = table_info_map.begin(); table_info_iter != table_info_map.end(); ++table_info_iter) {
		dbc_info_t& dbc_info = table_info_iter->second;
		if (!dbc_info.valid)
			continue;

		vector<dbc_field_t>& values = dbc_info.values;
		for (vector<dbc_field_t>::iterator iter = values.begin(); iter != values.end(); ++iter) {
			if (iter->field_name == field.field_name) {
				if (iter->field_type != field.field_type || iter->field_size != field.field_size) {
					if (iter->field_type == field.field_type && is_global(field.field_name)) {
						// 全局变量需要重新调整长度
						if (enlarge) {
							field_data_t *field_data = iter->field_data;
							delete []field_data->data;
							field_data->field_size = field.field_size;
							field_data->data = new char[field_data->field_size];
						}
						retval = true;
						return retval;
					}

					throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: field_name {1} is defined as different field_type/field_size.") % field.field_name).str(APPLOCALE));
				}

				retval = true;
				return retval;
			}
		}
	}

	return retval;
}

bool cal_tree::is_global(const string& field_name)
{
	bool retval = false;
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, (_("field_name={1}") % field_name).str(APPLOCALE), NULL);
#endif
	cal_parms_t& parms = CALP->_CALP_parms;
	const global_busi_t *busi_codes;
	int busi_size;

	if (parms.stage != 3) {
		busi_codes = GLOBAL_BUSI_CODES_STAGE1;
		busi_size = GLOBAL_BUSI_DATA_SIZE1;
	} else {
		busi_codes = GLOBAL_BUSI_CODES_STAGE2;
		busi_size = GLOBAL_BUSI_DATA_SIZE2;
	}

	for (int i = 0; i < busi_size; i++) {
		const global_busi_t& global_item = busi_codes[i];
		if (field_name == global_item.field_name) {
			retval = true;
			return retval;
		}
	}

	return retval;
}

bool cal_tree::parse_rule_busi(const string& busi_code, const string& rule_busi, int field_type,
	vector<field_data_t *>& rela_fields, vector<const char *>& in_array, string& dst_code, int& busi_flag,
	int& busi_type, bool local_flag)
{
	bool retval = false;
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, (_("busi_code={1}, rule_busi={2}, field_type={3}, local_flag={4}") % busi_code % rule_busi % field_type % local_flag).str(APPLOCALE), NULL);
#endif
	map<string, cal_busi_t>& busi_map = CALT->_CALT_busi_map;
	field_data_map_t& data_map = CALT->_CALT_data_map;
	string::const_iterator iter;
	string token;
	int index_id = 0;
	map<string, int> key_map;
	bool status = true;

	for (iter = rule_busi.begin(); iter != rule_busi.end();) {
		if (*iter == '\\') {
			// This charactor and next are a whole.
			dst_code += *iter;
			++iter;
			dst_code += *iter;
			++iter;
		} else if (*iter == '\"') {
			// Const string.
			dst_code += *iter;
			++iter;
			while (iter != rule_busi.end()) {
				// Skip const string.
				dst_code += *iter;
				++iter;
				if (*(iter-1) == '\"' && *(iter-2) != '\\')
					break;
			}
		} else if (*iter == '/' && (iter  + 1) != rule_busi.end() && *(iter + 1) == '*') {
			// Multi Line Comments
			dst_code += *iter;
			++iter;
			dst_code += *iter;
			++iter;
			while (iter != rule_busi.end()) {
				// Skip comment string.
				dst_code += *iter;
				++iter;
				if (*(iter - 1) == '/' && *(iter - 2) == '*')
					break;
			}
		} else if (*iter == '/' && (iter  + 1) != rule_busi.end() && *(iter + 1) != '*' && *(iter + 1) != '%' && *(iter + 1) != '$') {
			//  change "/"  to "/(double)"
			dst_code += *iter;
			dst_code += "(double)";
			++iter;
		} else if (*iter == '%') {
			// busi_code variables
			iter++;
			if (!::isalnum(*iter) && *iter != '_') {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: busi_code should be followed after $ for busi_code {1}, rule_busi = [{2}]") % busi_code % rule_busi).str(APPLOCALE));
				return retval;
			}

			token = "";
			for (; iter != rule_busi.end(); ++iter) {
				if (::isalnum(*iter) || *iter == '_')
					token += *iter;
				else
					break;
			}

			string real_busi_code = token;
			map<string, cal_busi_t>::iterator busi_iter = busi_map.find(real_busi_code);
			if (local_flag || (busi_iter == busi_map.end())) {
				// 查找其个性化指标
				real_busi_code += "_";
				real_busi_code += busi_code;
				busi_iter = busi_map.find(real_busi_code);
				if (busi_iter == busi_map.end()) {
					GPP->write_log(__FILE__, __LINE__, (_("ERROR: referenced busi_code {1} is not defined in unical_datasource_cfg.") % real_busi_code).str(APPLOCALE));
					return retval;
				}
			}

			// 需要继承其他指标的特定标识
			cal_busi_t& busi_item = busi_iter->second;
			busi_flag |= busi_item.flag & (BUSI_IS_MIXED | BUSI_CHANNEL_LEVEL | BUSI_DYNAMIC | BUSI_USER_CHNL_LEVEL);
			if (busi_type == -1)
				busi_type = busi_item.busi_type;
			else if (busi_type != busi_item.busi_type)
				busi_type = -1;

			if (!busi_item.valid) {
				status = false;
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: invalid real_busi_code {1} for busi_code {2}") % real_busi_code % busi_code).str(APPLOCALE));
				continue;
			} else if (!status) {
				continue;
			}

			field_data_t *field_data = busi_item.field_data;

			string index_str;
			string key = string("%") + real_busi_code;
			map<string, int>::const_iterator key_iter = key_map.find(key);
			if (key_iter == key_map.end()) {
				key_map[key] = index_id;
				/*
				if (!(field_data->flag & FIELD_DATA_GOTTEN)) 公式中有汇总指标需要增加，如发展用户*/
					rela_fields.push_back(field_data);
				in_array.push_back(field_data->data);
				index_str = boost::lexical_cast<string>(index_id);
				index_id++;
			} else {
				index_str = boost::lexical_cast<string>(key_iter->second);
			}

			switch (field_data->field_type) {
			case SQLTYPE_CHAR:
				dst_code += " *(char *)($";
				dst_code += index_str;
				dst_code += ")";
				break;
			case SQLTYPE_SHORT:
				dst_code += " *(short *)($";
				dst_code += index_str;
				dst_code += ")";
				break;
			case SQLTYPE_INT:
				dst_code += " *(int *)($";
				dst_code += index_str;
				dst_code += ")";
				break;
			case SQLTYPE_LONG:
				dst_code += " *(long *)($";
				dst_code += index_str;
				dst_code += ")";
				break;
			case SQLTYPE_FLOAT:
				dst_code += " *(float *)($";
				dst_code += index_str;
				dst_code += ")";
				break;
			case SQLTYPE_DOUBLE:
				dst_code += " *(double *)($";
				dst_code += index_str;
				dst_code += ")";
				break;
			case SQLTYPE_STRING:
				dst_code += "$";
				dst_code += index_str;
				break;
			case SQLTYPE_DATETIME:
				dst_code += " *reinterpret_cast<time_t *>($";
				dst_code += index_str;
				dst_code += ")";
				break;
			}
		} else if (*iter == '$') {
			// field_name variables
			iter++;
			if (!::isalnum(*iter) && *iter != '_') {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: busi_code should be followed after $ for busi_code {1}, rule_busi = [{2}]") % busi_code % rule_busi).str(APPLOCALE));
				return retval;
			}

			token = "";
			for (; iter != rule_busi.end(); ++iter) {
				if (::isalnum(*iter) || *iter == '_')
					token += *iter;
				else
					break;
			}

			field_data_map_t::const_iterator data_iter = data_map.find(token);
			if (data_iter == data_map.end()) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: referenced field_name {1} is not defined.") % token).str(APPLOCALE));
				return retval;
			}

			field_data_t *field_data = data_iter->second;

			if (!field_data->busi_code.empty()) {
				map<string, cal_busi_t>::iterator busi_iter = busi_map.find(field_data->busi_code);
				if (busi_iter == busi_map.end()) {
					GPP->write_log(__FILE__, __LINE__, (_("ERROR: referenced busi_code {1} is not defined in unical_datasource_cfg.") % token).str(APPLOCALE));
					return retval;
				}

				cal_busi_t& busi_item = busi_iter->second;
				if (busi_item.flag & BUSI_DEAL_FUNCTION) {
					GPP->write_log(__FILE__, __LINE__, (_("ERROR: busi_code {1} can't reference busi_code {2} defined in ai_rule_busi.") % busi_code % token).str(APPLOCALE));
					return retval;
				}

				// 需要继承其他指标的特定标识
				busi_flag |= busi_item.flag & (BUSI_IS_MIXED | BUSI_CHANNEL_LEVEL | BUSI_DYNAMIC);
				if (busi_type == -1)
					busi_type = busi_item.busi_type;
				else if (busi_type != busi_item.busi_type)
					busi_type = -1;

				if (!busi_item.valid) {
					status = false;
					continue;
				} else if (!status) {
					continue;
				}
			}

			string index_str;
			string key = string("$") + token;
			map<string, int>::const_iterator key_iter = key_map.find(key);
			if (key_iter == key_map.end()) {
				key_map[key] = index_id;
				if (!(field_data->flag & FIELD_DATA_GOTTEN))
					rela_fields.push_back(field_data);
				in_array.push_back(field_data->data);
				index_str = boost::lexical_cast<string>(index_id);
				index_id++;
			} else {
				index_str = boost::lexical_cast<string>(key_iter->second);
			}

			switch (field_data->field_type) {
			case SQLTYPE_CHAR:
				dst_code += " *(char *)($";
				dst_code += index_str;
				dst_code += ")";
				break;
			case SQLTYPE_SHORT:
				dst_code += " *(short *)($";
				dst_code += index_str;
				dst_code += ")";
				break;
			case SQLTYPE_INT:
				dst_code += " *(int *)($";
				dst_code += index_str;
				dst_code += ")";
				break;
			case SQLTYPE_LONG:
				dst_code += " *(long *)($";
				dst_code += index_str;
				dst_code += ")";
				break;
			case SQLTYPE_FLOAT:
				dst_code += " *(float *)($";
				dst_code += index_str;
				dst_code += ")";
				break;
			case SQLTYPE_DOUBLE:
				dst_code += " *(double *)($";
				dst_code += index_str;
				dst_code += ")";
				break;
			case SQLTYPE_STRING:
				dst_code += "$";
				dst_code += index_str;
				break;
			case SQLTYPE_DATETIME:
				dst_code += " *reinterpret_cast<time_t *>($";
				dst_code += index_str;
				dst_code += ")";
				break;
			}
		} else if (*iter == '#') {
			// Output variables
			iter++;
			if (*iter == '0') {
				iter++;
				switch (field_type) {
				case SQLTYPE_CHAR:
					dst_code += "*(char *)(#0)";
					break;
				case SQLTYPE_SHORT:
					dst_code += "*(short *)(#0)";
					break;
				case SQLTYPE_INT:
					dst_code += "*(int *)(#0)";
					break;
				case SQLTYPE_LONG:
					dst_code += "*(long *)(#0)";
					break;
				case SQLTYPE_FLOAT:
					dst_code += "*(float *)(#0)";
					break;
				case SQLTYPE_DOUBLE:
					dst_code += "*(double *)(#0)";
					break;
				case SQLTYPE_STRING:
					dst_code += "#0";
					break;
				case SQLTYPE_DATETIME:
					dst_code += "*(time_t *)(#0)";
					break;
				}
			} else if (::isalnum(*iter) || *iter == '_') {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: Only #0 is allowed for busi_code {1}, rule_busi {2}") % busi_code % rule_busi).str(APPLOCALE));
				return retval;
			}
		} else {
			// Other case
			dst_code += *iter;
			++iter;
		}
	}

	retval = status;
	return retval;
}

void cal_tree::check_dbc(bool enlarge)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, (_("enlarge={1}") % enlarge).str(APPLOCALE), NULL);
#endif
	map<int, dbc_info_t>::iterator table_info_iter;
	vector<dbc_field_t>::iterator field_iter;

	// 检查所有关键字，是否能找到来源
CHECK_AGAIN:
	for (table_info_iter = table_info_map.begin(); table_info_iter != table_info_map.end(); ++table_info_iter) {
		dbc_info_t& dbc_info = table_info_iter->second;
		if (!dbc_info.valid)
			continue;

		vector<dbc_field_t>& keys = dbc_info.keys;

		for (field_iter = keys.begin(); field_iter != keys.end(); ++field_iter) {
			if (!has_orign(*field_iter, enlarge)) {
				GPP->write_log(__FILE__, __LINE__, (_("WARNING: Table {1} is removed since field_name {2} is not defined.") % dbc_info.table_name % field_iter->field_name).str(APPLOCALE));
				dbc_info.valid = false;
				goto CHECK_AGAIN;
			}

			if (field_iter->field_name == PAY_CHNL_ID)
				dbc_info.dbc_flag |= DBC_FLAG_POLICY_LEVEL;
		}
	}
}

void cal_tree::dump(const string& field_name, int field_type, const char *data)
{
#if defined(DEBUG)
	dout << "" << field_name << " = [";
	switch (field_type) {
	case SQLTYPE_CHAR:
		dout << data[0];
		break;
	case SQLTYPE_SHORT:
		dout << *reinterpret_cast<const short *>(data);
		break;
	case SQLTYPE_INT:
		dout << *reinterpret_cast<const int *>(data);
		break;
	case SQLTYPE_LONG:
		dout << *reinterpret_cast<const long *>(data);
		break;
	case SQLTYPE_FLOAT:
		dout << *reinterpret_cast<const float *>(data);
		break;
	case SQLTYPE_DOUBLE:
		dout << *reinterpret_cast<const double *>(data);
		break;
	case SQLTYPE_STRING:
		dout << data;
		break;
	case SQLTYPE_DATETIME:
		dout << *reinterpret_cast<const time_t *>(data);
		break;
	}
	dout << "]\n";
#endif
}

}
}

