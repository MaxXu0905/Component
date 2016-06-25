#include "cal_base.h"
#include "sdc_api.h"
#include "t_ai_complex_busi_deposit.h"
#include "t_ai_complex_acct_busi_deposit.h"
#include "datastruct_structs.h"

namespace ai
{
namespace app
{

using namespace std;
using namespace ai::sg;
using namespace ai::scci;

int const DUP_TABLE_IDX = 0;
int const DUP_ID_IDX = 1;
int const DUP_KEY_IDX = 2;

const char DUP_TABLE_NAME[] = "dup_table";
const char DUP_ID_NAME[] = "dup_id";
const char DUP_KEY_NAME[] = "dup_key";

int const DUP_TABLE_SIZE = 64;
int const DUP_ID_SIZE = 128;
int const DUP_KEY_SIZE = 256;

int const MIN_ROLLBACK_TIMEOUT = 600;

cal_base::cal_base(int sa_id_)
	: sa_base(sa_id_)
{
	CALP = calp_ctx::instance();
	CALT = calt_ctx::instance();
	input_delimiter = SAP->_SAP_adaptors[sa_id].input_records[0].delimiter;

	dup_output[0] = NULL;
	dup_output[1] = NULL;
	dup_output[2] = NULL;
}

cal_base::~cal_base()
{
	for (int i = 0; i < 3; i++)
		delete []dup_output[i];
}

bool cal_base::support_batch() const
{
	bool retval = false;
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", &retval);
#endif

	return retval;
}

bool cal_base::support_concurrency() const
{
	bool retval = false;
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", &retval);
#endif

	return retval;
}

bool cal_base::call()
{
	bool retval = false;
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", &retval);
#endif

	if (execute_count == 0) {
		retval = true;
		return retval;
	}

	cal_base *_this = this;
	BOOST_SCOPE_EXIT((&_this)) {
		_this->execute_count = 0;
	} BOOST_SCOPE_EXIT_END

	// 根据每个政策计算
	try {
		const cal_parms_t& parms = CALP->_CALP_parms;
		field_data_t **global_fields;
		time_t eff_date;
		cal_tree& tree_mgr = cal_tree::instance(CALT);
		string cur_channel_id;
		field_data_t *cur_svc_type;
		global_fields = CALT->_CALT_global_fields;
			
		switch (parms.stage) {
		case 1:
			{
			// 设置输入数据相关的指标
			reset_fields();
			set_input_fields();
			
			if (CALP->_CALP_parms.driver_data != "09") {
				cur_svc_type = global_fields[SVC_TYPE_GI];
				if (!(cur_svc_type->flag & FIELD_DATA_GOTTEN))
					tree_mgr.get_field_value(cur_svc_type);
				strcpy(svc_type, cur_svc_type->data);
			}else{
				strcpy(svc_type, "05");
			}
			//战略渠道移网用户才输出未出帐原因
			if(CALP->_CALP_inv_out){
				cur_channel_id = global_fields[CHANNEL_ID_GI]->data;
				set<string>::const_iterator strategic_chnl_iter = strategic_channel_set.find(cur_channel_id);
				if (strategic_chnl_iter != strategic_channel_set.end()) {
					if (CALP->_CALP_parms.driver_data == "09")
						is_write_inv = true;
					else if(strcmp(svc_type, SVC_TYPE_01) ==0
						|| strcmp(svc_type, SVC_TYPE_02) ==0
						|| strcmp(svc_type, SVC_TYPE_06) ==0)
						is_write_inv = true;
					else
						is_write_inv = false;
				}else{
					is_write_inv = false;
				}
			}else{
				is_write_inv = false;
			}
			
			eff_date = *reinterpret_cast<time_t *>(global_fields[EFF_DATE_GI]->data);
			//如果业务受理时间小于最小政策生效日期，则不再循环任何政策，直接纪录无效
			if (eff_date < min_eff_date) {
				dout << __FUNCTION__<< " expired, eff_date = " << eff_date
					<< ", min_eff_date = " << min_eff_date << endl;
#if defined(INV_DEBUG)
				if (CALP->_CALP_debug_inv) {
					SAT->_SAT_err_info = (boost::format("::errno=%1%,eff_date=%2% is less than min_eff_date=%3%")
							% EPOLICY_LESS_EFF_DATE
							% eff_date
							% min_eff_date).str();
					SAT->_SAT_enable_err_info = true;
					BOOST_SCOPE_EXIT((&SAT)) {
						SAT->_SAT_enable_err_info = false;
					} BOOST_SCOPE_EXIT_END
        			}

				oinv_mgr->write(0);
#endif
				master.record_invalid++;
			}else{
				do_policy();
			}
			break;
			}
		case 2:
			do_merge();
			master.record_normal++;
			break;
		default:
			{
			// 设置输入数据相关的指标
			reset_fields();
			set_input_fields();

			if (CALP->_CALP_parms.driver_data != "09") {
				cur_svc_type = global_fields[SVC_TYPE_GI];
				if (!(cur_svc_type->flag & FIELD_DATA_GOTTEN))
					tree_mgr.get_field_value(cur_svc_type);
				strcpy(svc_type, cur_svc_type->data);
			}else{
				strcpy(svc_type, "05");
			}
				
			if(CALP->_CALP_inv_out){
				cur_channel_id = global_fields[CHANNEL_ID_GI]->data;
				set<string>::const_iterator strategic_chnl_iter = strategic_channel_set.find(cur_channel_id);
				if (strategic_chnl_iter != strategic_channel_set.end()) {
					if (CALP->_CALP_parms.driver_data == "09")
						is_write_inv = true;
					else if(strcmp(svc_type, SVC_TYPE_01) ==0
						|| strcmp(svc_type, SVC_TYPE_02) ==0
						|| strcmp(svc_type, SVC_TYPE_06) ==0)
						is_write_inv = true;
					else
						is_write_inv = false;
				}else{
					is_write_inv = false;
				}
			}else{
				is_write_inv = false;
			}
			
			do_calculation();
			break;
			}
		}
	} catch (exception& ex) {
		GPP->write_log(__FILE__, __LINE__ , ex.what());
		oerr_mgr->write(0);
		master.record_error++;
		SGP->_SGP_shutdown = true;
		return retval;
	}

	retval = true;
	return retval;
}

void cal_base::pre_init()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	if (adaptor.input_maps.size() != 1)
		throw bad_msg(__FILE__, __LINE__, SGEINVAL, (_("ERROR: input should be defined once and only once.")).str(APPLOCALE));

	const sa_adaptor_t& adaptor = SAP->_SAP_adaptors[sa_id];
	const sa_parms_t& parms = adaptor.parms;

	if (!CALP->get_config(parms.svc_key, parms.version))
		throw bad_msg(__FILE__, __LINE__, SGEINVAL, (_("ERROR: Can't get configuration, svc_key={1}, version={2}") % parms.svc_key % parms.version).str(APPLOCALE));

	// 创建输入及全局相关的字段
	if (CALP->_CALP_parms.stage != 2) {
		create_input_fields();
		create_global_fields(false);
	}

	try {
		map<string, string> conn_info;
		database_factory& factory_mgr = database_factory::instance();
		factory_mgr.parse(CALP->_CALP_parms.openinfo, conn_info);
		CALT->_CALT_db = factory_mgr.create(CALP->_CALP_parms.dbname);
		CALT->_CALT_db->connect(conn_info);
	} catch (exception& ex) {
		throw bad_sql(__FILE__, __LINE__, SGEOS, 0, ex.what());
	}

	gpenv& env_mgr = gpenv::instance();
	char *ptr = env_mgr.getenv("UNICAL_KEEP_LOG");
	if (ptr == NULL || strcasecmp(ptr, "Y") != 0)
		GPP->_GPP_skip_log = true;

	BOOST_SCOPE_EXIT((&GPP)) {
		GPP->_GPP_skip_log = false;
	} BOOST_SCOPE_EXIT_END

	cal_tree& tree_mgr = cal_tree::instance(CALT);
	cal_policy& policy_mgr = cal_policy::instance(CALT);
	cal_rule& rule_mgr = cal_rule::instance(CALT);

	tree_mgr.load();
	policy_mgr.load();
	rule_mgr.load();

	//只有计算当月需要加载所有渠道，用来计算当月无复合指标数据的渠道
	if(!CALP->_CALP_run_complex && CALP->_CALP_parms.stage == 2){
		load_channel();		
	}

	//加载战略渠道
	if (CALP->_CALP_parms.stage != 2 && CALP->_CALP_inv_out) 
		load_strategic_channel();

	delete CALT->_CALT_db;
	CALT->_CALT_db = NULL;

	load_finish();

	// 创建特殊表需要的全局字段
	if (CALP->_CALP_parms.stage != 2) {
		create_global_fields(true);
		create_output_fields();
	}

	if (CALP->_CALP_collect_driver)
		exit(0);
}

void cal_base::post_init()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif
	const cal_parms_t& parms = CALP->_CALP_parms;

	if (parms.stage == 2) {
		map<string, string>::const_iterator iter = adaptor.args.find("is_busi");
		if (iter != adaptor.args.end() && iter->second == "0")
			is_busi = false;
		else
			is_busi = true;

		if (!is_busi) {
			const vector<input_field_t>& field_vector = SAP->_SAP_adaptors[sa_id].input_records[0].field_vector;

			accu_keys = field_vector.size()-2;
			/*
			BOOST_FOREACH(const input_field_t& field, field_vector) {
				map<string, sqltype_enum>::const_iterator type_iter = CALP->_CALP_field_types.find(field.field_name);
				if (type_iter != CALP->_CALP_field_types.end() && type_iter->second != SQLTYPE_STRING)
					break;

				accu_keys++;
			}
			*/
		}

		return;
	}

	compiler& cmpl = SAP->_SAP_cmpl;
	field_map_t output_map;

	if (parms.rule_dup.empty()) {
		dup_idx = -1;
	} else {
		try {
			output_map[DUP_TABLE_NAME] = DUP_TABLE_IDX;
			output_map[DUP_ID_NAME] = DUP_ID_IDX;
			output_map[DUP_KEY_NAME] = DUP_KEY_IDX;

			dup_idx = cmpl.create_function(parms.rule_dup, adaptor.global_map, adaptor.output_map, output_map);
		} catch (exception& ex) {
			throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Can't create function for rule_dup, {1}") % ex.what()).str(APPLOCALE));
		}
	}

	if (parms.rule_dup_crbo.empty()) {
		dup_crbo_idx = -1;
	} else {
		try {
			output_map[DUP_TABLE_NAME] = DUP_TABLE_IDX;
			output_map[DUP_ID_NAME] = DUP_ID_IDX;
			output_map[DUP_KEY_NAME] = DUP_KEY_IDX;

			dup_crbo_idx = cmpl.create_function(parms.rule_dup_crbo, adaptor.global_map, adaptor.output_map, output_map);
		} catch (exception& ex) {
			throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Can't create function for rule_dup_crbo, {1}") % ex.what()).str(APPLOCALE));
		}
	}

	if (!parms.rule_dup.empty() || !parms.rule_dup_crbo.empty()) {
		dup_output[0] = new char[DUP_TABLE_SIZE];
		dup_output[1] = new char[DUP_ID_SIZE];
		dup_output[2] = new char[DUP_KEY_SIZE];
	}
}

void cal_base::post_init2()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	if (CALP->_CALP_parms.stage != 2) {
		const vector<global_field_t>& global_fields = SAP->_SAP_global.global_fields;
		for (int i = 0; i < global_fields.size(); i++) {
			const global_field_t& global_field = global_fields[i];

			if (global_field.field_name == "pay_return")
				CALT->_CALT_global_pay_return = SAT->_SAT_global[i];
			else if (global_field.field_name == "busi_code")
				CALT->_CALT_global_busi_code = SAT->_SAT_global[i];
			else if (global_field.field_name == "busi_value")
				CALT->_CALT_global_busi_value = SAT->_SAT_global[i];
			else if (global_field.field_name == "is_accumulate")
				CALT->_CALT_global_is_accumulate = SAT->_SAT_global[i];
			else if (global_field.field_name == "is_limit_flag")
				CALT->_CALT_global_is_limit_flag= SAT->_SAT_global[i];
		}

		set_output_fields();

		// 设置指标相关的函数地址
		compiler& cmpl = SAP->_SAP_cmpl;
		field_data_map_t& data_map = CALT->_CALT_data_map;
		for (map<string, field_data_t *>::iterator iter = data_map.begin(); iter != data_map.end(); ++iter) {
			field_data_t *field_data = iter->second;

			if (field_data->func_idx != -1)
				field_data->func = cmpl.get_function(field_data->func_idx);
		}

		if (dup_idx != -1)
			dup_func = cmpl.get_function(dup_idx);
		else
			dup_func = NULL;

		if (dup_crbo_idx != -1)
			dup_crbo_func = cmpl.get_function(dup_crbo_idx);
		else
			dup_crbo_func = NULL;
	}

	sdc_api& sdc_mgr = sdc_api::instance();
	if (!sdc_mgr.connected())
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Missing connecting to SDC")).str(APPLOCALE));

	if (!sdc_mgr.get_meta())
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Can't connect to SDC - {1}") % sdc_mgr.strerror()).str(APPLOCALE));
}

void cal_base::post_open()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif
	const cal_parms_t& parms = CALP->_CALP_parms;

	if (parms.stage == 1) {
		char *calc_cycle = CALT->_CALT_global_fields[CALC_CYCLE_GI]->data;
		char *sett_cycle = CALT->_CALT_global_fields[SETT_CYCLE_GI]->data;

		memcpy(calc_cycle, SAT->_SAT_raw_file_name.c_str() + 10, 8);
		calc_cycle[8] = '\0';
		memcpy(sett_cycle, SAT->_SAT_raw_file_name.c_str() + 10, 8);
		sett_cycle[8] = '\0';

		set_calc_months();
	}
}

void cal_base::post_run()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif
	const cal_parms_t& parms = CALP->_CALP_parms;

	if (parms.stage == 2) {
		if (SAT->_SAT_raw_file_name.empty())
			return;

		const vector<input_field_t>& field_vector = SAP->_SAP_adaptors[sa_id].input_records[0].field_vector;

		if (!is_busi)
			write_accu();
		else if (field_vector.size() == FIXED_INPUT_SIZE + 7)
			write_busi();
		else if (field_vector.size() == FIXED_INPUT_SIZE + 8)
			write_acct_busi();
	}
}

void cal_base::pre_rollback()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	const cal_parms_t& parms = CALP->_CALP_parms;

	if (parms.stage == 2) {
		gpenv& env_mgr = gpenv::instance();

		map<string, string>::const_iterator iter = adaptor.args.find("dst_dir");
		if (iter == adaptor.args.end())
			throw bad_msg(__FILE__, __LINE__, SGEINVAL, (_("ERROR: Can't find dst_dir in entry args.arg.name")).str(APPLOCALE));

		string dst_dir;
		env_mgr.expand_var(dst_dir, iter->second);
		if (*dst_dir.rbegin() != '/')
			dst_dir += '/';

		iter = adaptor.args.find("busi_dir");
		if (iter == adaptor.args.end())
			throw bad_msg(__FILE__, __LINE__, SGEINVAL, (_("ERROR: Can't find busi_dir in entry args.arg.name")).str(APPLOCALE));

		string busi_dir;
		env_mgr.expand_var(busi_dir, iter->second);
		if (*busi_dir.rbegin() != '/')
			busi_dir += '/';

		const vector<input_field_t>& field_vector = SAP->_SAP_adaptors[sa_id].input_records[0].field_vector;
		string dst_pattern;
		string busi_pattern;

		const char *acct_cycle = env_mgr.getenv("ACCT_CYCLE");
		if (acct_cycle == NULL)
			acct_cycle = "";
		if (is_busi) {
			if (field_vector.size() == FIXED_INPUT_SIZE + 7) {
				dst_pattern = string("^ai_idx_busi_value\\.seq\\.[0-9]{10}") + acct_cycle + "$";
				busi_pattern = string("^[0-9]{10}") + acct_cycle + "[0-9a-zA-Z]{4}\\.G0$";
			} else {
				dst_pattern = string("^ai_idx_acct_busi_value\\.seq\\.[0-9]{10}") + acct_cycle + "$";
				busi_pattern = string("^[0-9]{10}") + acct_cycle + "[0-9a-zA-Z]{4}\\.G1$";
			}
		}
		else {
			dst_pattern = "^ai_idx_mod_limit\\.seq\\.[0-9]+$";
		}

		vector<string> dst_files;
		scan_file<> dst_scan(dst_dir, dst_pattern);
		dst_scan.get_files(dst_files);
		BOOST_FOREACH(const string& filename, dst_files) {
			string full_name = dst_dir + filename;
			(void)::unlink(full_name.c_str());
		}

		if (is_busi) {
			vector<string> busi_files;
			scan_file<> busi_scan(busi_dir, busi_pattern);
			busi_scan.get_files(busi_files);
			BOOST_FOREACH(const string& filename, busi_files) {
				string full_name = busi_dir + filename;
				(void)::unlink(full_name.c_str());
			}
		}
	}

	int partition_id = 0;
	int nodes = parms.dup_all_partitions / parms.dup_partitions;
	string tables = "dup_UNICAL";
	
	int loop_num = 1;
	//代办业务驱动源需要清理dup_UNICAL_CRBO表
	if (parms.driver_data == "07")
		loop_num = 2;
	
	for (int i = 0; i < loop_num; i++) {
		// 清理dup_UNICAL_CRBO表
		if (i == 1)
			tables = "dup_UNICAL_CRBO";
		
		vector<int> rhandles;
		vector<string> svc_names;
		message_pointer rqst_msg = sg_message::create();
		message_pointer rply_msg = sg_message::create();

		int datalen = sizeof(sa_rqst_t) + tables.length();
		rqst_msg->set_length(datalen);
		rqst = reinterpret_cast<sa_rqst_t *>(rqst_msg->data());
		strcpy(rqst->svc_key, parms.dup_svc_key.c_str());
		rqst->version = adaptor.parms.version;
		rqst->flags = SA_METAMSG;
		rqst->datalen = datalen;
		rqst->rows = 1;
		rqst->global_size = 0;
		rqst->input_size = 0;
		rqst->user_id = SAT->_SAT_user_id;

		rqst->placeholder = tables.length();
		memcpy(reinterpret_cast<char *>(&rqst->placeholder) + sizeof(int), tables.c_str(), tables.length());

		int blktime = api_mgr.get_blktime(0);
		api_mgr.set_blktime(blktime < MIN_ROLLBACK_TIMEOUT ? MIN_ROLLBACK_TIMEOUT : blktime, 0);

		BOOST_SCOPE_EXIT((&api_mgr)(&rhandles)(&blktime)) {
			api_mgr.set_blktime(blktime, 0);
			BOOST_FOREACH(int& handle, rhandles) {
				(void)api_mgr.cancel(handle);
			}
		} BOOST_SCOPE_EXIT_END

		// 调用要尽可能分散到各个节点上，因为回退是比较耗时的操作
		for (int i = 0; i < parms.dup_partitions; i++) {
			for (int j = 0; j < nodes; j++) {
				partition_id = j * parms.dup_partitions + i;

				svc_name = "DUP_";
				svc_name += parms.dup_svc_key;
				svc_name += "_";
				svc_name += boost::lexical_cast<string>(partition_id);

				int handle = api_mgr.acall(svc_name.c_str(), rqst_msg, 0);
				if (handle == -1)
					throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: acall failed, op_name {1} - {2}") % svc_name % SGT->strerror()).str(APPLOCALE));

				rhandles.push_back(handle);
				svc_names.push_back(svc_name);
				if (rhandles.size() == concurrency) {
					for (int k = 0; k < rhandles.size(); k++) {
						if (api_mgr.getrply(rhandles[k], rply_msg, 0))
							continue;

						svc_name = svc_names[k];

						if (SGT->_SGT_error_code != SGETIME)
							throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: getrply failed, op_name {1} - {2}") % svc_name % SGT->strerror()).str(APPLOCALE));

						// 忽略错误
						(void)api_mgr.cancel(rhandles[k]);

						while (!api_mgr.call(svc_name.c_str(), rqst_msg, rply_msg, 0)) {
							if (SGT->_SGT_error_code != SGETIME)
								throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: getrply failed, op_name {1} - {2}") % svc_name % SGT->strerror()).str(APPLOCALE));
						}
					}

					rhandles.clear();
					svc_names.clear();
				}
			}
		}

		if (rhandles.empty())
			continue;

		for (int k = 0; k < rhandles.size(); k++) {
			if (api_mgr.getrply(rhandles[k], rply_msg, 0))
				continue;

			svc_name = svc_names[k];

			if (SGT->_SGT_error_code != SGETIME)
				throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: getrply failed, op_name {1} - {2}") % svc_name % SGT->strerror()).str(APPLOCALE));

			// 忽略错误
			(void)api_mgr.cancel(rhandles[k]);

			while (!api_mgr.call(svc_name.c_str(), rqst_msg, rply_msg, 0)) {
				if (SGT->_SGT_error_code != SGETIME)
					throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: getrply failed, op_name {1} - {2}") % svc_name % SGT->strerror()).str(APPLOCALE));
			}
		}

		// 必须手工清除，这样就不需要调用cancel()函数了
		rhandles.clear();
	}
}

void cal_base::load_channel()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	const cal_parms_t& parms = CALP->_CALP_parms;
	string sql_stmt = (boost::format("SELECT distinct channel_id{char[%1%]} "
		"FROM AI_COMPLEX_BUSI_DEPOSIT WHERE (province_code = :province_code{char[%2%]} ) ")
		% CHANNEL_ID_LEN % PROVINCE_CODE_LEN).str();
	
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
			string channel_id = rset->getString(1);
			channel_set.insert(channel_id);
		}
	} catch (bad_sql& ex) {
		throw bad_msg(__FILE__, __LINE__, 0, ex.what());
	}
}

void cal_base::load_strategic_channel()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	const cal_parms_t& parms = CALP->_CALP_parms;
	string sql_stmt = (boost::format("SELECT distinct chnl_id{char[%1%]} "
		"FROM V_TF_CHL_SUPER a "
		"WHERE EXISTS (SELECT 1 FROM tf_chl_channel b "
			"WHERE (a.super_chnl_id = b.chnl_id OR a.CHNL_ID = b.chnl_id) "
			"AND b.chnl_kind_id IN ('2040100', '2040200') "
			"AND b.province_code = :province_code{char[%2%]} ) "
		"AND a.province_code = :province_code{char[%3%]} " )
		% CHANNEL_ID_LEN % PROVINCE_CODE_LEN % PROVINCE_CODE_LEN).str();
	
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
		stmt->setString(2, parms.province_code);

		auto_ptr<Generic_ResultSet> auto_rset(stmt->executeQuery());
		Generic_ResultSet *rset = auto_rset.get();

		while (rset->next()) {
			string channel_id = rset->getString(1);
			strategic_channel_set.insert(channel_id);
		}
	} catch (bad_sql& ex) {
		throw bad_msg(__FILE__, __LINE__, 0, ex.what());
	}
}


// 加载完成后，校验参数合法性，删除无效数据
void cal_base::load_finish()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif
	const cal_parms_t& parms = CALP->_CALP_parms;
	map<string, cal_busi_t>& busi_map = CALT->_CALT_busi_map;
	set<cal_policy_area_t>& policy_area_set = CALT->_CALT_policy_area_set;
	map<int, cal_policy_t>& policy_map = CALT->_CALT_policy_map;
	map<int, cal_rule_t>& rule_map = CALT->_CALT_rule_map;
	set<cal_rule_area_t>& rule_area_set = CALT->_CALT_rule_area_set;
	map<int, string>& busi_errors = CALT->_CALT_busi_errors;

	map<string, cal_busi_t>::iterator busi_iter;
	map<int, cal_policy_t>::iterator policy_iter;
	set<cal_policy_area_t>::iterator policy_area_iter;
	map<int, cal_rule_t>::iterator rule_iter;
	set<cal_rule_area_t>::iterator rule_area_iter;
	map<int, string>::iterator error_iter;
	int calc_cycle = get_calc_cycle();

	// 删除无效的指标
	for (busi_iter = busi_map.begin(); busi_iter != busi_map.end();) {
		cal_busi_t& busi_item = busi_iter->second;
		if (!busi_item.valid) {
			map<string, cal_busi_t>::iterator tmp_iter = busi_iter++;
			busi_map.erase(tmp_iter);
		} else {
			++busi_iter;
		}
	}

	// 对于合法的政策，如果规则上有错误信息等待输出，
	// 则需要判断该信息是否应该最终输出
	for (policy_iter = policy_map.begin(); policy_iter != policy_map.end(); ++policy_iter) {
		cal_policy_t& policy = policy_iter->second;
		vector<int>& rules = policy.rules;
		int rule_valid = 0;
		int rule_num = 0;
		bool is_valid = true;
		for (vector<int>::const_iterator iter = rules.begin(); iter != rules.end(); ++iter) {
			error_iter = busi_errors.find(*iter);
			if (error_iter != busi_errors.end()) {
				rule_iter = rule_map.find(*iter);
				if (rule_iter == rule_map.end()) {
					GPP->write_log(__FILE__, __LINE__, (_("ERROR: rule_id {1} for mod_id {2} is not defined in ai_rule_config.") % *iter % policy_iter->first).str(APPLOCALE));
					continue;
				}

				cal_rule_t& rule_cfg = rule_iter->second;

				if (match_driver(policy.flag, rule_cfg.flag)) {
					// 不是当前驱动源的规则，允许有错误
					GPP->write_log(error_iter->second);
					busi_errors.erase(error_iter);
					GPP->write_log(__FILE__, __LINE__, (_("ERROR: rule_id {1} for mod_id {2} has some error.") % *iter % policy_iter->first).str(APPLOCALE));
					rule_cfg.valid = false;
					break;
				}
			}

			//复合指标计算时间点判断
			rule_iter = rule_map.find(*iter);
			if (rule_iter == rule_map.end()) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: rule_id {1} for mod_id {2} is not defined in ai_rule_config.") % *iter % policy_iter->first).str(APPLOCALE));
				continue;
			}

			cal_rule_t& rule_cfg = rule_iter->second;
			cal_rule_calc_cfg_t complex_cal_cfg = rule_cfg.calc_cfg;

			//政策生效时间的int表示
			datetime eff_dt(policy.eff_date);
			int eff_date = eff_dt.year() * 12 + eff_dt.month();

			//政策失效时间的int表示
			datetime exp_dt(policy.exp_date);
			int exp_date = exp_dt.year() * 12 + exp_dt.month();
			
			//政策失效时间在v_ai_mod_config中是加1天处理的
			if (exp_dt.day() == 1)
				exp_date --;

			//引用复合指标的规则计算时间点判断，如果未到计算时间点则无效
			int complex_busi_num = 0;
			for (vector<cal_complex_busi_attr_t>::const_iterator complex_iter = rule_cfg.complex_attr.begin(); complex_iter != rule_cfg.complex_attr.end(); ++complex_iter) {
				const cal_complex_busi_attr_t& complex_attr = *complex_iter;
				double unit_multiple = (calc_cycle - complex_cal_cfg.calc_start_time + 1 - eff_date) / complex_attr.units;
				int unit_remainder = (calc_cycle - complex_cal_cfg.calc_start_time + 1 - eff_date) % complex_attr.units;
				complex_busi_num++;
				
				//超过最后一次计算时间点则无效
				if(calc_cycle - complex_cal_cfg.calc_start_time > exp_date){
					GPP->write_log(__FILE__, __LINE__, (_("ERROR: mix_busi_code {1} of rule_id {2} for mod_id {3} is out of cycle. exp_date={4}") % complex_attr.busi_code % *iter % policy_iter->first % policy.exp_date).str(APPLOCALE));
					//rule_cfg.valid = false;
					is_valid = false;
					break;
				}

				switch (complex_attr.complex_type) {
				case COMPLEX_TYPE_GROWTH:
					if (unit_multiple < 2 || unit_remainder != 0) {
						GPP->write_log(__FILE__, __LINE__, (_("ERROR: mix_busi_code {1} of rule_id {2} for mod_id {3} is not arrive calc_cycle. unit_multiple={4}, unit_remainder={5}") % complex_attr.busi_code % *iter % policy_iter->first % unit_multiple % unit_remainder).str(APPLOCALE));
						//rule_cfg.valid = false;
						is_valid = false;
					}
					break;
				case COMPLEX_TYPE_SUM:
					if (unit_multiple < 1 || unit_remainder != 0) {
						GPP->write_log(__FILE__, __LINE__, (_("ERROR: mix_busi_code {1} of rule_id {2} for mod_id {3} is not arrive calc_cycle. unit_multiple={4}, unit_remainder={5}") % complex_attr.busi_code % *iter % policy_iter->first % unit_multiple % unit_remainder).str(APPLOCALE));
						//rule_cfg.valid = false;
						is_valid = false;
					}
					break;
				case COMPLEX_TYPE_SGR:
					if (unit_multiple < 2 || unit_remainder != 0) {
						GPP->write_log(__FILE__, __LINE__, (_("ERROR: mix_busi_code {1} of rule_id {2} for mod_id {3} is not arrive calc_cycle. unit_multiple={4}, unit_remainder={5}") % complex_attr.busi_code % *iter % policy_iter->first % unit_multiple % unit_remainder).str(APPLOCALE));
						//rule_cfg.valid = false;
						is_valid = false;
					}
					break;
				case COMPLEX_TYPE_GR:
					if (unit_multiple < 2 || unit_remainder != 0) {
						GPP->write_log(__FILE__, __LINE__, (_("ERROR: mix_busi_code {1} of rule_id {2} for mod_id {3} is not arrive calc_cycle. unit_multiple={4}, unit_remainder={5}") % complex_attr.busi_code % *iter % policy_iter->first % unit_multiple % unit_remainder).str(APPLOCALE));
						//rule_cfg.valid = false;
						is_valid = false;
					}
					break;
				case COMPLEX_TYPE_AVG:
					if (unit_multiple < 1 || unit_remainder != 0) {
						GPP->write_log(__FILE__, __LINE__, (_("ERROR: mix_busi_code {1} of rule_id {2} for mod_id {3} is not arrive calc_cycle. unit_multiple={4}, unit_remainder={5}") % complex_attr.busi_code % *iter % policy_iter->first % unit_multiple % unit_remainder).str(APPLOCALE));
						//rule_cfg.valid = false;
						is_valid = false;
					}
				}
				cal_mod_rule_complex_t mod_rule_item;
				if (is_valid){
					mod_rule_item.mod_id = policy_iter->first;
					mod_rule_item.rule_id = *iter;
					mod_rule_item.busi_code = complex_attr.busi_code;
					set<cal_mod_rule_complex_t>::const_iterator mod_rule_complex_iter = mod_rule_complex_set.find(mod_rule_item);
					if (mod_rule_complex_iter == mod_rule_complex_set.end()) {
					mod_rule_complex_set.insert(mod_rule_item);
					}
				}

			}

			// 如果是为了跑复合指标历史数据，则未引用复合指标的规则无效
			if (CALP->_CALP_run_complex && complex_busi_num == 0 && rule_cfg.cycles == 0){
				rule_cfg.valid = false;
				continue;
			}

			//计算政策失效时间+计算起始点+计算期数，如果比当前账期还要早，则删除规则
			if (parms.stage == 1 
			&& rule_cfg.calc_cfg.calc_cycle != -1 
			&& (exp_date + rule_cfg.calc_cfg.calc_start_time + (rule_cfg.calc_cfg.calc_cycle -1) * rule_cfg.calc_cfg.cycle_time) < calc_cycle){
				rule_valid ++;
			        GPP->write_log(__FILE__, __LINE__, (_("ERROR: rule_id {1} for mod_id {2} is invalid.") % *iter % policy_iter->first).str(APPLOCALE));
			}
			if(rule_valid == rules.size())
				policy.valid = false;		
			//防止同一个引用复合指标的规则被多个政策引用的情况
			if (!is_valid) {
				GPP->write_log(__FILE__, __LINE__, (_("INFO: rule_id {1} for mod_id {2} is not arrive calc_cycle, releation is deleted") % *iter % policy_iter->first).str(APPLOCALE));
				iter = rules.erase(rules.begin() + rule_num);
				iter --;
			}else {
				rule_num ++;
			}
		}
	}

	busi_errors.clear();

	// 删除无效的规则
	for (rule_iter = rule_map.begin(); rule_iter != rule_map.end();) {
		cal_rule_t& rule_cfg = rule_iter->second;
		bool valid = rule_cfg.valid;
		if (valid) {
			if (parms.stage == 3 && rule_cfg.calc_detail.empty()) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: rule_id {1} is removed since no cal_para_value is defined.") % rule_iter->first).str(APPLOCALE));
				valid = false;
			} else if (rule_cfg.pay_cfg.pay_type != PAY_TYPE_ONCE && rule_cfg.pay_cfg.pay_detail.empty()) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: rule_id {1} is removed since pay detail can be empty only for pay_type PAY_TYPE_ONCE.") % rule_iter->first).str(APPLOCALE));
				valid = false;
			}
		}

		if (!valid) {
			map<int, cal_rule_t>::iterator tmp_iter = rule_iter++;
			rule_map.erase(tmp_iter);
		} else {
			++rule_iter;
		}
	}

	// 删除失效的规则相关的地域数据
	for (rule_area_iter = rule_area_set.begin(); rule_area_iter != rule_area_set.end();) {
		rule_iter = rule_map.find(rule_area_iter->rule_id);
		if (rule_iter == rule_map.end()) {
			set<cal_rule_area_t>::iterator tmp_iter = rule_area_iter++;
			rule_area_set.erase(tmp_iter);
		} else {
			++rule_area_iter;
		}
	}

	// 对规则中的条件排序，只对阶梯计算进行排序，以保证当超出范围时，
	// 后面的条件规则都不会满足
	for (rule_iter = rule_map.begin(); rule_iter != rule_map.end(); ++rule_iter) {
		cal_rule_t& rule_cfg = rule_iter->second;
		if (rule_cfg.flag & RULE_FLAG_STEP) {
			vector<cal_rule_calc_detail_t>& calc_detail = rule_cfg.calc_detail;
			std::sort(calc_detail.begin(), calc_detail.end());
		}
	}

	// 删除无效的政策
	for (policy_iter = policy_map.begin(); policy_iter != policy_map.end();) {
		cal_policy_t& policy = policy_iter->second;
		vector<int>& rules = policy.rules;
		bool valid = policy.valid;
		if (policy_iter == policy_map.begin())
			min_eff_date = policy.eff_date;
	
		if (valid) {
			if (rules.empty()) {
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: mod_id {1} is removed since no rule is defined.") % policy_iter->first).str(APPLOCALE));
				valid = false;
			} else {
				for (vector<int>::const_iterator iter = rules.begin(); iter != rules.end(); ++iter) {
					rule_iter = rule_map.find(*iter);
					if (rule_iter == rule_map.end()) {
						GPP->write_log(__FILE__, __LINE__, (_("INFO: rule_id {1} for mod_id {2} is not defined, or defined wrongly.") % *iter % policy_iter->first).str(APPLOCALE));
						valid = false;
						break;
					}
				}
			}
		}

		if (!valid) {
			map<int, cal_policy_t>::iterator tmp_iter = policy_iter++;
			policy_map.erase(tmp_iter);
		} else {
			//保存政策最小生效时间
            		if (policy.eff_date < min_eff_date)
				min_eff_date = policy.eff_date;
			++policy_iter;
		}
	}

	// 删除不需要计算的政策
	for (map<int, cal_policy_t>::iterator policy_iter = policy_map.begin(); policy_iter != policy_map.end();) {
		cal_policy_t& policy = policy_iter->second;
		vector<int>& rules = policy.rules;

		bool ok_flag = true;
		for (int i = rules.size() - 1; i >= 0; i--) {
			map<int, cal_rule_t>::iterator rule_iter = rule_map.find(rules[i]);
			cal_rule_t& rule_cfg = rule_iter->second;
			vector<cal_accu_cond_t>& accu_cond = rule_cfg.accu_cond;
			vector<cal_accu_cond_t>& acct_busi_cond = rule_cfg.acct_busi_cond;
			if (match_driver(policy.flag, rule_cfg.flag))
							continue;

			if (accu_cond.empty() && acct_busi_cond.empty()) {
				if ((policy.flag & POLICY_FLAG_NULL_DRIVER)
					&& (rule_cfg.flag & RULE_FLAG_NULL_DRIVER)
					&& !(rule_cfg.flag & RULE_FLAG_MATCH_ONE_DRIVER)
					&& rule_cfg.calc_cfg.calc_method != CALC_METHOD_FIXED) {
					// 指标不能确定驱动源
					GPP->write_log(__FILE__, __LINE__, (_("ERROR: rule_id {1} of mod_id {2} doesn't specify driver_data.") % rules[i] % policy_iter->first).str(APPLOCALE));
					rules.erase(rules.begin() + i);
				} else if (!(rule_cfg.flag & RULE_FLAG_NO_CALC_BUSI) && (rule_cfg.flag & RULE_FLAG_OPEN_TIME) && (rule_cfg.flag & RULE_FLAG_NULL_DRIVER)) {
					// 规则配置了开户时间，非用户资料驱动源，不需要计算
					GPP->write_log(__FILE__, __LINE__, (_("ERROR: rule_id {1} of mod_id {2} is removed since it's driver is 01.") % rules[i] % policy_iter->first).str(APPLOCALE));
					rules.erase(rules.begin() + i);
				} else if (!match_driver(policy.flag, rule_cfg.flag)) {
					// 规则与当前驱动源不匹配，不需要计算
					GPP->write_log(__FILE__, __LINE__, (_("ERROR: rule_id {1} of mod_id {2} is removed since there are no accumulation busi_codes.") % rules[i] % policy_iter->first).str(APPLOCALE));
					rules.erase(rules.begin() + i);
				}
			} else {
				bool rule_break = false;
				BOOST_FOREACH(const cal_accu_cond_t& cond_item, accu_cond) {
					//如果规则选择了起始点为开户时间，而当前计算的规则
					//不是用户资料，则不累计
					if ((rule_cfg.flag & RULE_FLAG_OPEN_TIME)
						&& (rule_cfg.flag & RULE_FLAG_NULL_DRIVER))
						continue;

					// 累计指标的驱动源不匹配当前驱动源，也不是未指定驱动源
					if (!(cond_item.flag & (ACCU_FLAG_NULL_DRIVER | ACCU_FLAG_MATCH_DRIVER)))
						continue;

					// 如果政策上指定了驱动源，而累计指标未指定驱动源，
					// 则不需要累计，由指定驱动源进行累计
					if ((policy.flag & (POLICY_FLAG_NULL_DRIVER | POLICY_FLAG_MATCH_DRIVER))
						|| !(cond_item.flag & ACCU_FLAG_NULL_DRIVER)) {
						rule_break = true;
						break;
					}
				}

				if (!rule_break) {
					BOOST_FOREACH(const cal_accu_cond_t& cond_item, acct_busi_cond) {
						//如果规则选择了起始点为开户时间，而当前计算的规则
						//不是用户资料，则不累计
						if ((rule_cfg.flag & RULE_FLAG_OPEN_TIME)
							&& (rule_cfg.flag & RULE_FLAG_NULL_DRIVER))
							continue;

						// 累计指标的驱动源不匹配当前驱动源，也不是未指定驱动源
						if (!(cond_item.flag & (ACCU_FLAG_NULL_DRIVER | ACCU_FLAG_MATCH_DRIVER)))
							continue;

						// 如果政策上指定了驱动源，而累计指标未指定驱动源，
						// 则不需要累计，由指定驱动源进行累计
						if ((policy.flag & (POLICY_FLAG_NULL_DRIVER | POLICY_FLAG_MATCH_DRIVER))
							|| !(cond_item.flag & ACCU_FLAG_NULL_DRIVER)) {
							rule_break = true;
							break;
						}
					}
				}
				if (!rule_break) {
					GPP->write_log(__FILE__, __LINE__, (_("INFO: rule_id {1} for mod_id {2} has no accumulation busi_code") % rules[i] % policy_iter->first).str(APPLOCALE));
					rules.erase(rules.begin() + i);
				}
			}
		}

		if (!ok_flag) {
			GPP->write_log(__FILE__, __LINE__, (_("ERROR: mod_id {1} is removed since its related rules has some error.") % policy_iter->first).str(APPLOCALE));
			map<int, cal_policy_t>::iterator tmp_iter = policy_iter++;
			policy_map.erase(tmp_iter);
		} else if (rules.empty()) {
			GPP->write_log(__FILE__, __LINE__, (_("INFO: mod_id {1} is removed since no related rules exist.") % policy_iter->first).str(APPLOCALE));
			map<int, cal_policy_t>::iterator tmp_iter = policy_iter++;
			policy_map.erase(tmp_iter);
		} else {
			++policy_iter;
		}
	}

	// 删除失效的政策相关的地域数据
	for (policy_area_iter = policy_area_set.begin(); policy_area_iter != policy_area_set.end();) {
		policy_iter = policy_map.find(policy_area_iter->mod_id);
		if (policy_iter == policy_map.end()) {
			set<cal_policy_area_t>::iterator tmp_iter = policy_area_iter++;
			policy_area_set.erase(tmp_iter);
		} else {
			++policy_area_iter;
		}
	}

	if (CALP->_CALP_collect_driver)
		collect_driver();
}

// 创建全局指标数据结构
void cal_base::create_global_fields(bool table_loaded)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, (_("table_loaded={1}") % table_loaded).str(APPLOCALE), NULL);
#endif
	const cal_parms_t& parms = CALP->_CALP_parms;
	field_data_map_t& data_map = CALT->_CALT_data_map;
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
		if (parms.driver_data == "09" && strcmp(global_item.field_name, SVC_TYPE) == 0)
			continue;
		if (parms.driver_data != "01" && 
			(strcmp(global_item.field_name, SUBS_STATUS) == 0 || strcmp(global_item.field_name, UPDATE_TIME) == 0))
			continue;
		if (parms.driver_data != "03" && strcmp(global_item.field_name, PRODUCT_TYPE) == 0)
			continue;
		
		if ((table_loaded && !(global_item.flag & BUSI_FLAG_FROM_TABLE))
			|| (!table_loaded && (global_item.flag & BUSI_FLAG_FROM_TABLE)))
			continue;

		field_data_map_t::iterator data_iter = data_map.find(global_item.field_name);
		if (global_item.flag & BUSI_FLAG_NO_CREATE) {
			if (data_iter == data_map.end()) {
				if (global_item.flag & BUSI_FLAG_NULL_OK)
					continue;

				if (global_item.flag & BUSI_FLAG_FROM_TABLE) {
					GPP->write_log(__FILE__, __LINE__, (_("WARNING: field_name {1} is not defined in dbc.") % global_item.field_name).str(APPLOCALE));
					continue;
				} else {
					throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: field_name {1} should be defined in input section.") % global_item.field_name).str(APPLOCALE));
				}
			}

			if (global_item.field_type != data_iter->second->field_type)
				throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: field_type for field_name {1} doesn't match.") % global_item.field_name).str(APPLOCALE));

			CALT->_CALT_global_fields[global_item.field_index] = data_iter->second;
		} else if (global_item.flag & BUSI_FLAG_NULL_CREATE) {
			if (data_iter == data_map.end()) {
				int flag;
				if (global_item.flag & BUSI_FLAG_RESET)
					flag = FIELD_DATA_NO_DEFAULT | FIELD_DATA_RESET;
				else
					flag = FIELD_DATA_NO_DEFAULT | FIELD_DATA_GOTTEN;

				field_data_t *item = new field_data_t(flag, "", "", global_item.field_type,
					global_item.field_size, -1, -1, -1, DEAL_TYPE_DIRECT);

				data_map[global_item.field_name] = item;
				CALT->_CALT_global_fields[global_item.field_index] = item;
			} else {
				if (global_item.field_type != data_iter->second->field_type)
					throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: field_type for field_name {1} doesn't match.") % global_item.field_name).str(APPLOCALE));

				CALT->_CALT_global_fields[global_item.field_index] = data_iter->second;
			}

		} else {
			if (data_iter != data_map.end())
				throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: field_name {1} should not be defined in input section.") % global_item.field_name).str(APPLOCALE));

			int flag;
			if (global_item.flag & BUSI_FLAG_RESET)
				flag = FIELD_DATA_NO_DEFAULT | FIELD_DATA_RESET;
			else
				flag = FIELD_DATA_NO_DEFAULT | FIELD_DATA_GOTTEN;

			field_data_t *item = new field_data_t(flag, "", "", global_item.field_type,
				global_item.field_size, -1, -1, -1, DEAL_TYPE_DIRECT);

			data_map[global_item.field_name] = item;
			CALT->_CALT_global_fields[global_item.field_index] = item;
		}
	}
}

// 创建输入指标数据结构
void cal_base::create_input_fields()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif
	field_data_map_t& data_map = CALT->_CALT_data_map;
	const vector<input_field_t>& field_vector = SAP->_SAP_adaptors[sa_id].input_records[0].field_vector;
	CALT->_CALT_input_fields = new field_data_t *[field_vector.size()];

	int i = 0;
	BOOST_FOREACH(const input_field_t& field, field_vector) {
		string field_name;
		string::size_type pos = field.field_name.find(".");
		if (pos == string::npos)
			field_name = field.field_name;
		else
			field_name = field.field_name.substr(pos + 1);

		sqltype_enum field_type;
		map<string, sqltype_enum>::const_iterator type_iter = CALP->_CALP_field_types.find(field_name);
		if (type_iter == CALP->_CALP_field_types.end())
			field_type = SQLTYPE_STRING;
		else
			field_type = type_iter->second;

		int field_size;
		switch (field_type) {
		case SQLTYPE_CHAR:
			field_size = sizeof(char);
			break;
		case SQLTYPE_SHORT:
			field_size = sizeof(short);
			break;
		case SQLTYPE_INT:
			field_size = sizeof(int);
			break;
		case SQLTYPE_LONG:
			field_size = sizeof(long);
			break;
		case SQLTYPE_FLOAT:
			field_size = sizeof(float);
			break;
		case SQLTYPE_DOUBLE:
			field_size = sizeof(double);
			break;
		case SQLTYPE_STRING:
			field_size = field.factor2 + 1;
			break;
		case SQLTYPE_TIME:
			field_size = sizeof(time_t);
			break;
		default:
			throw bad_msg(__FILE__, __LINE__, 449, (_("ERROR: unsupported field_type {1} given.") % field_type).str(APPLOCALE));
		}

		// 不要对busi_code赋值
		field_data_t *item = new field_data_t(FIELD_DATA_NO_DEFAULT | FIELD_DATA_GOTTEN, "", "", field_type,
			field_size, -1, -1, -1, DEAL_TYPE_DIRECT);

		data_map[field_name] = item;
		CALT->_CALT_input_fields[i++] = item;
	}
}

// 设置输出相关的指标列表
void cal_base::create_output_fields()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif
	field_data_map_t& data_map = CALT->_CALT_data_map;
	const vector<output_field_t>& output_fields = SAP->_SAP_adaptors[sa_id].output_fields;
	CALT->_CALT_output_fields = new field_data_t *[output_fields.size()];
	int i = 0;

	BOOST_FOREACH(const output_field_t& field, output_fields) {
		string field_name;
		string::size_type pos = field.field_name.find(".");
		if (pos == string::npos)
			field_name = field.field_name;
		else
			field_name = field.field_name.substr(pos + 1);

		map<string, string>::const_iterator iter = CALP->_CALP_default_values.find(field_name);
		if (iter != CALP->_CALP_default_values.end()) {
			if (iter->second.length() > field.field_size)
				throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: default value for field {1} too long") % field_name).str(APPLOCALE));

			CALT->_CALT_output_fields[i++] = NULL;
			continue;
		}

		field_data_map_t::iterator data_iter = data_map.find(field_name);
		if (data_iter == data_map.end())
			throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Can't find field_name {1} in busi code map.") % field_name).str(APPLOCALE));

		CALT->_CALT_output_fields[i++] = data_iter->second;
	}
}

// 设置输出相关的指标列表
void cal_base::set_output_fields()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif
	const vector<output_field_t>& output_fields = SAP->_SAP_adaptors[sa_id].output_fields;
	int i = 0;

	BOOST_FOREACH(const output_field_t& field, output_fields) {
		string field_name;
		string::size_type pos = field.field_name.find(".");
		if (pos == string::npos)
			field_name = field.field_name;
		else
			field_name = field.field_name.substr(pos + 1);

		map<string, string>::const_iterator iter = CALP->_CALP_default_values.find(field_name);
		if (iter != CALP->_CALP_default_values.end())
			strcpy(output[i], iter->second.c_str());

		i++;
	}
}


// 重新初始化数据对象
void cal_base::reset_fields()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif
	field_data_map_t& data_map = CALT->_CALT_data_map;

	for (field_data_map_t::iterator iter = data_map.begin(); iter != data_map.end(); ++iter) {
		field_data_t *field_data = iter->second;
		if (field_data->flag & FIELD_DATA_RESET) {
			field_data->data_size = 0;
			field_data->flag &= ~(FIELD_DATA_SET_DEFAULT | FIELD_DATA_GOTTEN);
		}
	}
}

// 把输入字段转化成指标
void cal_base::set_input_fields()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif
	const cal_parms_t& parms = CALP->_CALP_parms;
	const vector<input_field_t>& field_vector = SAP->_SAP_adaptors[sa_id].input_records[0].field_vector;
	char **input = get_input(0);

	inv_line = "";
	for (int i = 0; i < field_vector.size(); i++) {
		field_data_t *src_field = CALT->_CALT_input_fields[i];
		char *field_data = input[i];
		char *data = src_field->data;

		if (field_vector[i].inv_flag)
			inv_line = inv_line + field_data +  input_delimiter;

		switch (src_field->field_type) {
		case SQLTYPE_CHAR:
			data[0] = field_data[0];
			break;
		case SQLTYPE_SHORT:
			*reinterpret_cast<short *>(data) = static_cast<short>(::atoi(field_data));
			break;
		case SQLTYPE_INT:
			*reinterpret_cast<int *>(data) = ::atoi(field_data);
			break;
		case SQLTYPE_LONG:
			*reinterpret_cast<long *>(data) = ::atol(field_data);
			break;
		case SQLTYPE_FLOAT:
			*reinterpret_cast<float *>(data) = static_cast<float>(::atof(field_data));
			break;
		case SQLTYPE_DOUBLE:
			*reinterpret_cast<double *>(data) = static_cast<double>(::atof(field_data));
			break;
		case SQLTYPE_STRING:
			strcpy(data, field_data);
			break;
		case SQLTYPE_TIME:
			*reinterpret_cast<time_t *>(data) = static_cast<time_t>(::atol(field_data));
			break;
		default:
			break;
		}
	}

	// 设置eff_date
	datetime dt(CALT->_CALT_global_fields[DEAL_DATE_GI]->data);
	*reinterpret_cast<time_t *>(CALT->_CALT_global_fields[EFF_DATE_GI]->data) = dt.duration();

	// 设置计算周期的月数
	if (parms.stage == 3)
		set_calc_months();
}

void cal_base::set_kpi_date_type(int rule_id)
{
	cal_rule& rule_mgr = cal_rule::instance(CALT);
	cal_rule_t& rule_cfg = rule_mgr.get_rule(rule_id);
	int calc_cycle = get_calc_cycle();

	int real_date;
	int tmp_year;
	int tmp_month;
	int tmp_quarter;
	int tmp_half;
	char real_year[4+1];
	char real_month[2+1];
	char real_quarter[1+1];
	char real_half[1+1];

	char curr_kpi_date_type[6+1];
	//判断任务量指标时间类型
	for (vector<cal_kpi_busi_attr_t>::const_iterator kpi_iter = rule_cfg.kpi_attr.begin(); kpi_iter != rule_cfg.kpi_attr.end(); ++kpi_iter) {
		cal_kpi_busi_attr_t kpi_attr = *kpi_iter;
		cal_rule_calc_cfg_t kpi_cal_cfg = rule_cfg.calc_cfg;

			real_date = calc_cycle - kpi_cal_cfg.calc_start_time;
			tmp_year = real_date / 12;
			tmp_month = real_date % 12;

		if(tmp_month == 0){
				tmp_year = tmp_year -1;
			tmp_month = 12;
		}

		sprintf(real_year, "%d", tmp_year);
		sprintf(real_month, "%02d", tmp_month);

		memset( curr_kpi_date_type, 0, sizeof(curr_kpi_date_type) );

		switch (kpi_attr.kpi_unit) {
		case KPI_DATE_TYPE_MONTH:		//月
				memcpy(curr_kpi_date_type, real_year, 4);
				memcpy(curr_kpi_date_type+4, real_month, 2);
				curr_kpi_date_type[6] = '\0';
				break;
		case KPI_DATE_TYPE_QUARTER: 	  //季
				tmp_quarter = (tmp_month + 2) / 3 ;
					sprintf(real_quarter, "%d", tmp_quarter);
					memcpy(curr_kpi_date_type, real_year, 4);
				memcpy(curr_kpi_date_type+4, "Q", 1);
				memcpy(curr_kpi_date_type+5, real_quarter, 1);
				curr_kpi_date_type[6] = '\0';
					break;
		case KPI_DATE_TYPE_HALF:	   //半年
			tmp_half = (tmp_month + 5) / 6 ;
			sprintf(real_half, "%d", tmp_half);
			memcpy(curr_kpi_date_type, real_year, 4);
			memcpy(curr_kpi_date_type+4, "H", 1);
			memcpy(curr_kpi_date_type+5, real_half, 1);
			curr_kpi_date_type[6] = '\0';
			break;
		case KPI_DATE_TYPE_YEAR:	   //年
				memcpy(curr_kpi_date_type, real_year, 4);
				curr_kpi_date_type[4] = '\0';
					break;
			default:
					GPP->write_log(__FILE__, __LINE__, (_("ERROR: busi_code {1} of rule_id {2} has error kpi_unit. kpi_unit is {3}") % kpi_attr.busi_code % CALT->_CALT_rule_id % kpi_attr.kpi_unit ).str(APPLOCALE));
					break;
			}

		dout << "curr_kpi_date_type = " << curr_kpi_date_type << std::endl;

			strcpy(CALT->_CALT_global_fields[KPI_DATE_TYPE_GI]->data, curr_kpi_date_type);
	}

}


// 计算
void cal_base::do_calculation()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif
	cal_rule& rule_mgr = cal_rule::instance(CALT);
	CALT->_CALT_mod_id = *reinterpret_cast<int *>(CALT->_CALT_global_fields[MOD_ID_GI]->data);
	CALT->_CALT_rule_id = *reinterpret_cast<int *>(CALT->_CALT_global_fields[RULE_ID_GI]->data);
	char *calc_cycle = CALT->_CALT_global_fields[CALC_CYCLE_GI]->data;
	char *sett_cycle = CALT->_CALT_global_fields[SETT_CYCLE_GI]->data;

	cal_policy& policy_mgr = cal_policy::instance(CALT);
	cal_policy_t& policy = policy_mgr.get_policy(CALT->_CALT_mod_id);
	strcpy(CALT->_CALT_global_fields[IS_LIMIT_FLAG_GI]->data, policy.is_limit_flag.c_str());
	strcpy(CALT->_CALT_global_is_limit_flag, policy.is_limit_flag.c_str());

	cal_rule_t& rule_cfg = rule_mgr.get_rule(CALT->_CALT_rule_id);

	set_kpi_date_type(CALT->_CALT_rule_id);

	const cal_parms_t& parms = CALP->_CALP_parms;


	int rule_calc_cycle;
	if (parms.channel_level)
		rule_calc_cycle = rule_cfg.calc_cfg.calc_cycle;
	else
		rule_calc_cycle = 1;

	dout << "rule_calc_cycle = " << rule_calc_cycle << endl;
	bool is_match = false;
	string comm_track;
    comm_track = CALT->_CALT_global_fields[COMM_TRACK_GI]->data;

	for (int i = 0; i < rule_calc_cycle; i++)  {

		if (parms.channel_level){
			*reinterpret_cast<int *>(CALT->_CALT_global_fields[CYCLE_ID_GI]->data) = i;
			if (i != 0){
			reset_fields();
			strcpy(CALT->_CALT_global_fields[COMM_TRACK_GI]->data, comm_track.c_str());
		    }
		}
		// 计算帐期与结算帐期相等时，表示是当期，需要计算佣金，
		// 否则是解冻的，不需计算佣金
		if (strcmp(calc_cycle, sett_cycle) == 0) {
			if (rule_mgr.do_calculate(rule_cfg) == -1) {
				if (!parms.channel_level || (parms.channel_level && i == rule_calc_cycle -1)) {
					if (is_write_inv && CALT->_CALT_rule_error_no != ERULE_TOO_MANY_VALUES){
						SAT->_SAT_err_info = (boost::format("%1%%2%%3%%4%%5%%6%%7%%8%%9%%10%%11%")
							% CALT->_CALT_global_fields[PAY_CHNL_ID_GI]->data 
							% input_delimiter
							% svc_type
							% input_delimiter
							% CALT->_CALT_mod_id
							% input_delimiter
							% CALT->_CALT_rule_id
							% input_delimiter
							% CALT->_CALT_rule_error_no
							% input_delimiter
							% CALT->_CALT_rule_error_string).str();
						SAT->_SAT_enable_err_info = true;
						BOOST_SCOPE_EXIT((&SAT)) {
							SAT->_SAT_enable_err_info = false;
						} BOOST_SCOPE_EXIT_END
						oinv_mgr->write(inv_line, SAT->_SAT_err_info);
					}else if (!CALP->_CALP_inv_out){
						SAT->_SAT_err_info = (boost::format("::errno=%1%,mod_id=%2%,rule_id=%3%")
							% ECOMM_CALCULATION
							% CALT->_CALT_mod_id
							% CALT->_CALT_rule_id).str();
						SAT->_SAT_enable_err_info = true;
						BOOST_SCOPE_EXIT((&SAT)) {
							SAT->_SAT_enable_err_info = false;
						} BOOST_SCOPE_EXIT_END
						oinv_mgr->write(0);
					}else{
#if defined(INV_DEBUG)
						SAT->_SAT_err_info = (boost::format("::errno=%1%,mod_id=%2%,rule_id=%3%")
							% ECOMM_CALCULATION
							% CALT->_CALT_mod_id
							% CALT->_CALT_rule_id).str();
						SAT->_SAT_enable_err_info = true;
						BOOST_SCOPE_EXIT((&SAT)) {
							SAT->_SAT_enable_err_info = false;
						} BOOST_SCOPE_EXIT_END
						oinv_mgr->write(0);
#endif
					}
					if (!is_match)
						master.record_invalid++;
					return;
				}else{
					continue;
				}
			}else{
				is_match = true;
			}

			set_track(TRACK_STAGE_CALC);
		}

		set_track(TRACK_STAGE_PAY);

		// 设置支付条件
		int pay_ret = rule_mgr.do_pay(rule_cfg);
		sprintf(CALT->_CALT_global_pay_return, "%d", pay_ret);

		pre_write();
		otgt_mgr->write(0);
		odistr_mgr->write(0);
		master.record_normal++;
	}
}

// 执行规则相关的操作
bool cal_base::do_rule(const cal_policy_t& policy)
{
	bool retval = false;
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", &retval);
#endif
	const cal_parms_t& parms = CALP->_CALP_parms;
	cal_rule& rule_mgr = cal_rule::instance(CALT);
	cal_rule_t& rule_cfg = rule_mgr.get_rule(CALT->_CALT_rule_id);

	//当是产品订购驱动源时，如果订购记录是非主产品订购，则结算给服务渠道的规则不计算
	if (rule_cfg.calc_cfg.settlement_to_service == 1 
		&& parms.driver_data == "03" 
		&& strcmp(CALT->_CALT_global_fields[PRODUCT_TYPE_GI]->data,"1") != 0) {
		if (is_write_inv){
			CALT->_CALT_rule_error_string = string("product_type=") + CALT->_CALT_global_fields[PRODUCT_TYPE_GI]->data;
			SAT->_SAT_err_info = (boost::format("%1%%2%%3%%4%%5%%6%%7%%8%%9%%10%%11%")
				% CALT->_CALT_global_fields[PAY_CHNL_ID_GI]->data 
				% input_delimiter
				% svc_type
				% input_delimiter
				% CALT->_CALT_mod_id
				% input_delimiter
				% CALT->_CALT_rule_id
				% input_delimiter
				% ERULE_ONLY_MATCH_MAIN_PRODUCT
				% input_delimiter
				% CALT->_CALT_rule_error_string).str();
			SAT->_SAT_enable_err_info = true;
			BOOST_SCOPE_EXIT((&SAT)) {
				SAT->_SAT_enable_err_info = false;
			} BOOST_SCOPE_EXIT_END
			oinv_mgr->write(inv_line, SAT->_SAT_err_info);
			master.record_invalid++;
		}else{
#if defined(INV_DEBUG)
			if (CALP->_CALP_debug_inv) {
				SAT->_SAT_err_info = (boost::format("::errno=%1%,mod_id=%2%,rule_id=%3%,settlement_to_service=%4%,product_type=%5%")
					% ERULE_ONLY_MATCH_MAIN_PRODUCT
					% CALT->_CALT_mod_id
					% CALT->_CALT_rule_id
					% rule_cfg.calc_cfg.settlement_to_service
					% CALT->_CALT_global_fields[PRODUCT_TYPE_GI]->data).str();
				SAT->_SAT_enable_err_info = true;
				BOOST_SCOPE_EXIT((&SAT)) {
					SAT->_SAT_enable_err_info = false;
				} BOOST_SCOPE_EXIT_END
				oinv_mgr->write(0);
				master.record_invalid++;
			}
#endif
		}
		return retval;
	}
	
	if (!rule_mgr.check_rule(CALT->_CALT_rule_id, rule_cfg)) {
		if (is_write_inv){
			SAT->_SAT_err_info = (boost::format("%1%%2%%3%%4%%5%%6%%7%%8%%9%%10%%11%")
				% CALT->_CALT_global_fields[PAY_CHNL_ID_GI]->data 
				% input_delimiter
				% svc_type
				% input_delimiter
				% CALT->_CALT_mod_id
				% input_delimiter
				% CALT->_CALT_rule_id
				% input_delimiter
				% CALT->_CALT_rule_error_no
				% input_delimiter
				% CALT->_CALT_rule_error_string).str();
			SAT->_SAT_enable_err_info = true;
			BOOST_SCOPE_EXIT((&SAT)) {
				SAT->_SAT_enable_err_info = false;
			} BOOST_SCOPE_EXIT_END
			oinv_mgr->write(inv_line, SAT->_SAT_err_info);
			master.record_invalid++;
		}else{
#if defined(INV_DEBUG)
			if (CALP->_CALP_debug_inv) {
				SAT->_SAT_err_info = (boost::format("::errno=%1%,mod_id=%2%,rule_id=%3%,message=%4%")
					% CALT->_CALT_rule_error_no
					% CALT->_CALT_mod_id
					% CALT->_CALT_rule_id
					% CALT->_CALT_rule_error_string).str();
				SAT->_SAT_enable_err_info = true;
				BOOST_SCOPE_EXIT((&SAT)) {
					SAT->_SAT_enable_err_info = false;
				} BOOST_SCOPE_EXIT_END
				oinv_mgr->write(0);
				master.record_invalid++;
			}
#endif
		}
		return retval;
	}

	if (!rule_mgr.check_cond(rule_cfg.ref_cond)) {
		if (is_write_inv && CALT->_CALT_rule_error_no != ERULE_TOO_MANY_VALUES){
			SAT->_SAT_err_info = (boost::format("%1%%2%%3%%4%%5%%6%%7%%8%%9%%10%%11%")
				% CALT->_CALT_global_fields[PAY_CHNL_ID_GI]->data 
				% input_delimiter
				% svc_type
				% input_delimiter
				% CALT->_CALT_mod_id
				% input_delimiter
				% CALT->_CALT_rule_id
				% input_delimiter
				% CALT->_CALT_rule_error_no
				% input_delimiter
				% CALT->_CALT_rule_error_string).str();
			SAT->_SAT_enable_err_info = true;
			BOOST_SCOPE_EXIT((&SAT)) {
				SAT->_SAT_enable_err_info = false;
			} BOOST_SCOPE_EXIT_END
			oinv_mgr->write(inv_line, SAT->_SAT_err_info);
			master.record_invalid++;
		}else{
#if defined(INV_DEBUG)
			if (CALP->_CALP_debug_inv) {
				SAT->_SAT_err_info = (boost::format("::errno=%1%,mod_id=%2%,rule_id=%3%,message=%4%")
					% CALT->_CALT_rule_error_no
					% CALT->_CALT_mod_id
					% CALT->_CALT_rule_id
					% CALT->_CALT_rule_error_string).str();
				SAT->_SAT_enable_err_info = true;
				BOOST_SCOPE_EXIT((&SAT)) {
					SAT->_SAT_enable_err_info = false;
				} BOOST_SCOPE_EXIT_END
				oinv_mgr->write(0);
				master.record_invalid++;
			}
#endif
		}
		return retval;
	}

	if(!accumulate(policy, rule_cfg))
		return retval;
	
	//用于累计多期发展用户收入的记录不输出第三步驱动源
	int cycle_id = *reinterpret_cast<int *>(CALT->_CALT_global_fields[CYCLE_ID_GI]->data);
	const cal_rule_calc_cfg_t& calc_cfg = rule_cfg.calc_cfg;
	if (cycle_id < 0 || (calc_cfg.calc_cycle>0 && cycle_id >= calc_cfg.calc_cycle)) 
		return false;
	
	bool write_record_flag = match_driver(policy.flag, rule_cfg.flag);
	if (!write_record_flag) {
#if defined(INV_DEBUG)
		if (CALP->_CALP_debug_inv) {
			SAT->_SAT_err_info = (boost::format("::errno=%1%,mod_id=%2%,rule_id=%3%,policy_flag=%4%,rule_flag=%5%")
				% ERULE_NOT_DRIVER
				% CALT->_CALT_mod_id
				% CALT->_CALT_rule_id
				% policy.flag
				% rule_cfg.flag).str();
			SAT->_SAT_enable_err_info = true;
			BOOST_SCOPE_EXIT((&SAT)) {
				SAT->_SAT_enable_err_info = false;
			} BOOST_SCOPE_EXIT_END
			oinv_mgr->write(0);
			master.record_invalid++;
		}
#endif
		return retval;
	}

	// 如果计算指标设置了缺省值，则不输出
	if (rule_cfg.sum_field_data != NULL
		&& rule_cfg.sum_field_data->deal_type == DEAL_TYPE_FILTER
		&& (rule_cfg.sum_field_data->flag & FIELD_DATA_SET_DEFAULT)) {
		if (is_write_inv){
			ostringstream fmt;
			fmt << *rule_cfg.sum_field_data;
			CALT->_CALT_rule_error_string = fmt.str();
			SAT->_SAT_err_info = (boost::format("%1%%2%%3%%4%%5%%6%%7%%8%%9%%10%%11%")
				% CALT->_CALT_global_fields[PAY_CHNL_ID_GI]->data 
				% input_delimiter
				% svc_type
				% input_delimiter
				% CALT->_CALT_mod_id
				% input_delimiter
				% CALT->_CALT_rule_id
				% input_delimiter
				% ERULE_CALC_NO_DATA
				% input_delimiter
				% CALT->_CALT_rule_error_string).str();
			SAT->_SAT_enable_err_info = true;
			BOOST_SCOPE_EXIT((&SAT)) {
				SAT->_SAT_enable_err_info = false;
			} BOOST_SCOPE_EXIT_END
			oinv_mgr->write(inv_line, SAT->_SAT_err_info);
			master.record_invalid++;
		}else{
#if defined(INV_DEBUG)
			if (CALP->_CALP_debug_inv) {
				ostringstream fmt;
				fmt << *rule_cfg.sum_field_data;
				SAT->_SAT_err_info = (boost::format("::errno=%1%,mod_id=%2%,rule_id=%3%,sum_field_data=%4%")
					% ERULE_CALC_NO_DATA
					% CALT->_CALT_mod_id
					% CALT->_CALT_rule_id
					% fmt.str()).str();
				SAT->_SAT_enable_err_info = true;
				BOOST_SCOPE_EXIT((&SAT)) {
					SAT->_SAT_enable_err_info = false;
				} BOOST_SCOPE_EXIT_END
				oinv_mgr->write(0);
				master.record_invalid++;
			}
#endif
		}
		return retval;
	}

	set_track(TRACK_STAGE_COND);

	if (rule_cfg.flag & RULE_FLAG_ACCT) {
		strcpy(CALT->_CALT_global_fields[RECORD_LEVEL_GI]->data, "A");
		if (!insert_dup(DUP_ACCT_LEVEL))
			return retval;
	} else {
		if (parms.driver_data == "09")  //渠道驱动源
			strcpy(CALT->_CALT_global_fields[RECORD_LEVEL_GI]->data, "H");
		else if  (parms.driver_data == "05") 	//卡品驱动源
			strcpy(CALT->_CALT_global_fields[RECORD_LEVEL_GI]->data, "C");
		else
			strcpy(CALT->_CALT_global_fields[RECORD_LEVEL_GI]->data, "U");
	}

	if (rule_cfg.calc_cfg.calc_cycle > 1) // 分成佣金
		strcpy(CALT->_CALT_global_fields[CALC_TYPE_GI]->data, "02");
	else // 一次佣金
		strcpy(CALT->_CALT_global_fields[CALC_TYPE_GI]->data, "01");

	/*结算给服务渠道规则并且服务渠道不为空，则发展渠道结算比例为1 - 服务渠道比例；
	否则，发展渠道结算比例为1*/
	cal_tree& tree_mgr = cal_tree::instance(CALT);
	if (parms.driver_data != "05" 
	&& parms.driver_data != "09" 
	&& rule_cfg.calc_cfg.settlement_to_service == 1 
	&& !(CALT->_CALT_global_fields[SERV_CHNL_ID_GI]->flag & FIELD_DATA_GOTTEN))
		tree_mgr.get_field_value(CALT->_CALT_global_fields[SERV_CHNL_ID_GI]);

	*reinterpret_cast<int *>(CALT->_CALT_global_fields[SERV_FLAG_GI]->data) = 0; //服务渠道佣金标志
	strcpy(CALT->_CALT_global_fields[RELA_CHNL_GI]->data, ""); //关联渠道，当是服务渠道佣金时填写发展渠道
	*reinterpret_cast<int *>(CALT->_CALT_global_fields[COST_FLAG_GI]->data) = 0; //网络运营成本标志
    
	//跑复合指标历史数据时，不输出计算第三步驱动源
	if (!CALP->_CALP_run_complex) {
		 //第一步输出:如果规则是结算给服务渠道并且服务渠道不为空，则输出两条记录
		 //卡片驱动源和渠道驱动源不能结算给服务渠道
		int out_record_num;
		if (parms.driver_data != "05" 
		&& parms.driver_data != "09" 
		&& rule_cfg.calc_cfg.settlement_to_service == 1 
		&& strcmp(CALT->_CALT_global_fields[SERV_CHNL_ID_GI]->data, "") != 0){
			*reinterpret_cast<double *>(CALT->_CALT_global_fields[CHNL_SEPARATE_GI]->data) = 1 - rule_cfg.calc_cfg.share_ratio/100;
			out_record_num = 2;
		}else{
			*reinterpret_cast<double *>(CALT->_CALT_global_fields[CHNL_SEPARATE_GI]->data) = 1;
			out_record_num = 1;
		}
		
		string org_channel_id , org_pay_chnl_id, org_pay_comm_flag;
		for (int i = 0; i < out_record_num; i++) {
			if (i == 1){//服务渠道拆分记录，修改相关字段
				org_channel_id=CALT->_CALT_global_fields[CHANNEL_ID_GI]->data;
				org_pay_chnl_id = CALT->_CALT_global_fields[PAY_CHNL_ID_GI]->data;
				org_pay_comm_flag = CALT->_CALT_global_fields[PAY_COMM_FLAG_GI]->data;	
				//重置服务渠道相关字段
				reset_svc_chnl_fields();
				//服务渠道结算比例
				*reinterpret_cast<double *>(CALT->_CALT_global_fields[CHNL_SEPARATE_GI]->data) = rule_cfg.calc_cfg.share_ratio/100;
				*reinterpret_cast<int *>(CALT->_CALT_global_fields[SERV_FLAG_GI]->data) = 1;  //服务渠道佣金标志
				strcpy(CALT->_CALT_global_fields[RELA_CHNL_GI]->data, CALT->_CALT_global_fields[CHANNEL_ID_GI]->data); //关联渠道，当是服务渠道佣金时填写发展渠道
				*reinterpret_cast<int *>(CALT->_CALT_global_fields[COST_FLAG_GI]->data) = rule_cfg.calc_cfg.as_operating_costs; //网络运营成本标志
				//发展渠道、结算渠道替换为服务渠道
				strcpy(CALT->_CALT_global_fields[CHANNEL_ID_GI]->data, CALT->_CALT_global_fields[SERV_CHNL_ID_GI]->data);
				strcpy(CALT->_CALT_global_fields[PAY_CHNL_ID_GI]->data, CALT->_CALT_global_fields[SERV_CHNL_ID_GI]->data);
				strcpy(CALT->_CALT_global_fields[PAY_COMM_FLAG_GI]->data, "0");
			}
			pre_write();
			otgt_mgr->write(0);
			odistr_mgr->write(0);
			if (i == 1) {
				reset_svc_chnl_fields();
				strcpy(CALT->_CALT_global_fields[CHANNEL_ID_GI]->data, org_channel_id.c_str());
				strcpy(CALT->_CALT_global_fields[PAY_CHNL_ID_GI]->data, org_pay_chnl_id.c_str());
				strcpy(CALT->_CALT_global_fields[PAY_COMM_FLAG_GI]->data, org_pay_comm_flag.c_str());
				CALT->_CALT_global_fields[PAY_CHNL_ID_GI]->flag &= ~FIELD_DATA_GOTTEN;
			}
		}
	}

	retval = true;
	return retval;
}

// 执行政策相关的操作
void cal_base::do_policy()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif
	cal_tree& tree_mgr = cal_tree::instance(CALT);
	cal_policy& policy_mgr = cal_policy::instance(CALT);
	int matches = 0;	
	field_data_t *pay_chnl_id = CALT->_CALT_global_fields[PAY_CHNL_ID_GI];

	//获取用户业务类型
	bool check_busi_type = false;
	if (CALP->_CALP_parms.driver_data != "09") {
		check_busi_type = true;
		field_data_t *svc_type = CALT->_CALT_global_fields[SVC_TYPE_GI];
		if (!(svc_type->flag & FIELD_DATA_GOTTEN))
			tree_mgr.get_field_value(svc_type);
	}
	//获取用户状态和更新时间
	bool check_subs_status = false;
	if (CALP->_CALP_parms.driver_data == "01") {
		check_subs_status = true;
		field_data_t *subs_status = CALT->_CALT_global_fields[SUBS_STATUS_GI];
		if (!(subs_status->flag & FIELD_DATA_GOTTEN))
			tree_mgr.get_field_value(subs_status);
		field_data_t *update_time = CALT->_CALT_global_fields[UPDATE_TIME_GI];
		if (!(update_time->flag & FIELD_DATA_GOTTEN))
			tree_mgr.get_field_value(update_time);
	}

	for (cal_policy::const_iterator policy_iter = policy_mgr.begin(); policy_iter != policy_mgr.end(); ++policy_iter) {
		const cal_policy_t& policy = policy_iter->second;
		const vector<int>& rules = policy.rules;
		CALT->_CALT_mod_id = policy_iter->first;
		set<string>::const_iterator cp;
	if (!(pay_chnl_id->flag & FIELD_DATA_GOTTEN)) {
		tree_mgr.get_field_value(pay_chnl_id);
		pay_chnl_id->flag &= ~FIELD_DATA_POLICY_RESET;
		if (pay_chnl_id->data_size == 0 || pay_chnl_id->data[0] == '\0') {
			// 如果来源于政策中的支付渠道，则每次切换时需要重新设置
			if (CALT->_CALT_global_fields[POLICY_CHNL_ID_GI]->data[0] == '\0')
				strcpy(pay_chnl_id->data, CALT->_CALT_global_fields[CHANNEL_ID_GI]->data);
			else
				strcpy(pay_chnl_id->data, CALT->_CALT_global_fields[POLICY_CHNL_ID_GI]->data);

			pay_chnl_id->flag |= FIELD_DATA_POLICY_RESET;
			pay_chnl_id->data_size = 1;
		}
	}

		//比较用户和政策的业务类型
		if (check_busi_type && policy.busi_type != "00"){
			if (strcmp(policy.busi_type.c_str(), svc_type)){
				CALT->_CALT_policy_error_string = string("policy_busi_type=") + svc_type;
				if (CALP->_CALP_debug_inv) {
					if (is_write_inv){
						SAT->_SAT_err_info = (boost::format("%1%%2%%3%%4%%5%%6%%7%%8%%9%%10%%11%")
							% CALT->_CALT_global_fields[PAY_CHNL_ID_GI]->data 
							% input_delimiter
							% svc_type
							% input_delimiter
							% CALT->_CALT_mod_id
							% input_delimiter
							% ""
							% input_delimiter
							% EPOLICY_NOT_MATCH
							% input_delimiter
							% CALT->_CALT_policy_error_string).str();
						SAT->_SAT_enable_err_info = true;
						BOOST_SCOPE_EXIT((&SAT)) {
							SAT->_SAT_enable_err_info = false;
						} BOOST_SCOPE_EXIT_END
						oinv_mgr->write(inv_line, SAT->_SAT_err_info);
						master.record_invalid++;
					}else{
#if defined(INV_DEBUG)
						if (CALP->_CALP_debug_inv) {
							SAT->_SAT_err_info = (boost::format("::errno=%1%,mod_id=%2%,message=%3%")
								% EPOLICY_NOT_MATCH
								% CALT->_CALT_mod_id
								% CALT->_CALT_policy_error_string).str();
							SAT->_SAT_enable_err_info = true;
							BOOST_SCOPE_EXIT((&SAT)) {
								SAT->_SAT_enable_err_info = false;
							} BOOST_SCOPE_EXIT_END
							oinv_mgr->write(0);
							master.record_invalid++;
						}
#endif
					}
				}
				continue;
			}
		}
		//比较用户状态和时间，往月销户用户不计算销售佣金
		if (check_subs_status && policy.comm_type == "01")
			if (strcmp(CALT->_CALT_global_fields[SUBS_STATUS_GI]->data,"5")==0 && 
				strncmp(CALT->_CALT_global_fields[UPDATE_TIME_GI]->data, CALT->_CALT_global_fields[CALC_CYCLE_GI]->data, 6)<0)
				continue;

		//先清空政策的结算渠道，如果政策有在match()中赋值
		CALT->_CALT_global_fields[POLICY_CHNL_ID_GI]->data[0] = '\0';
		strcpy(CALT->_CALT_global_fields[ACCESS_FLAG_GI]->data, policy_iter->second.access_flag.c_str());
		if (!policy_mgr.match(policy_iter->first, policy_iter->second)) {
			if (is_write_inv && CALT->_CALT_policy_error_no != EPOLICY_EXPIRED){
				SAT->_SAT_err_info = (boost::format("%1%%2%%3%%4%%5%%6%%7%%8%%9%%10%%11%")
					% CALT->_CALT_global_fields[PAY_CHNL_ID_GI]->data 
					% input_delimiter
					% svc_type
					% input_delimiter
					% CALT->_CALT_mod_id
					% input_delimiter
					% ""
					% input_delimiter
					% CALT->_CALT_policy_error_no
					% input_delimiter
					% CALT->_CALT_policy_error_string).str();
				SAT->_SAT_enable_err_info = true;
				BOOST_SCOPE_EXIT((&SAT)) {
					SAT->_SAT_enable_err_info = false;
				} BOOST_SCOPE_EXIT_END
				oinv_mgr->write(inv_line, SAT->_SAT_err_info);
				master.record_invalid++;
			}else{
#if defined(INV_DEBUG)
				if (CALP->_CALP_debug_inv) {
					SAT->_SAT_err_info = (boost::format("::errno=%1%,mod_id=%2%,message=%3%")
						% CALT->_CALT_policy_error_no
						% CALT->_CALT_mod_id
						% CALT->_CALT_policy_error_string).str();
					SAT->_SAT_enable_err_info = true;
					BOOST_SCOPE_EXIT((&SAT)) {
						SAT->_SAT_enable_err_info = false;
					} BOOST_SCOPE_EXIT_END
					oinv_mgr->write(0);
					master.record_invalid++;
				}
#endif
			}
			continue;
		}

		// 设置政策ID
		*reinterpret_cast<int *>(CALT->_CALT_global_fields[MOD_ID_GI]->data) = CALT->_CALT_mod_id;
		strcpy(CALT->_CALT_global_fields[DEPT_ID_GI]->data, policy.dept_id.c_str());
		strcpy(CALT->_CALT_global_fields[COMM_TYPE_GI]->data, policy.comm_type.c_str());		
		strcpy(CALT->_CALT_global_fields[MOD_TYPE_GI]->data, policy.mod_type.c_str());

		 if (pay_chnl_id->flag & FIELD_DATA_POLICY_RESET) {
			// 支付渠道有变化，与支付渠道相关的字段都必须标识为无效
			const char *data;
			if (CALT->_CALT_global_fields[POLICY_CHNL_ID_GI]->data[0] == '\0')
				data = CALT->_CALT_global_fields[CHANNEL_ID_GI]->data;
			else
				data = CALT->_CALT_global_fields[POLICY_CHNL_ID_GI]->data;

			if (strcmp(pay_chnl_id->data, data) != 0) {
				strcpy(pay_chnl_id->data, data);
				tree_mgr.reset_fields();
			}
		}
		strcpy(CALT->_CALT_global_fields[REAL_PAYCHNL_GI]->data, pay_chnl_id->data);

		// 判断当前指标政策是否需要过滤
		field_data_t *inv_field_data = CALT->_CALT_global_fields[INV_BUSI_GI];
		if (inv_field_data != NULL) {
			//每次都执行inv_busi, 因为存在一些修改数据的功能
			tree_mgr.get_field_value(inv_field_data);

			// 如果该指标值为1，表示需要过滤
			if (*reinterpret_cast<int *>(inv_field_data->data) == 1) {
				if (is_write_inv){
					ostringstream fmt;
					fmt << *inv_field_data;
					CALT->_CALT_rule_error_string = fmt.str();
					SAT->_SAT_err_info = (boost::format("%1%%2%%3%%4%%5%%6%%7%%8%%9%%10%%11%")
						% CALT->_CALT_global_fields[PAY_CHNL_ID_GI]->data 
						% input_delimiter
						% svc_type
						% input_delimiter
						% CALT->_CALT_mod_id
						% input_delimiter
						% ""
						% input_delimiter
						% EPOLICY_FILTER
						% input_delimiter
						% CALT->_CALT_policy_error_string).str();
					SAT->_SAT_enable_err_info = true;
					BOOST_SCOPE_EXIT((&SAT)) {
						SAT->_SAT_enable_err_info = false;
					} BOOST_SCOPE_EXIT_END
					oinv_mgr->write(inv_line, SAT->_SAT_err_info);
					master.record_invalid++;
				}else{
#if defined(INV_DEBUG)
					if (CALP->_CALP_debug_inv) {
						SAT->_SAT_err_info = (boost::format("::errno=%1%,mod_id=%2%")
							% EPOLICY_FILTER
							% CALT->_CALT_mod_id).str();
						SAT->_SAT_enable_err_info = true;
						BOOST_SCOPE_EXIT((&SAT)) {
							SAT->_SAT_enable_err_info = false;
						} BOOST_SCOPE_EXIT_END
						oinv_mgr->write(0);
						master.record_invalid++;
					}
#endif
				}
				continue;
			}
		}

		for (vector<int>::const_iterator iter = rules.begin(); iter != rules.end(); ++iter) {
			// 设置规则ID
			CALT->_CALT_rule_id = *iter;
			*reinterpret_cast<int *>(CALT->_CALT_global_fields[RULE_ID_GI]->data) = CALT->_CALT_rule_id;
			if (matches > 0){
				if (!(pay_chnl_id->flag & FIELD_DATA_GOTTEN)) {
					tree_mgr.get_field_value(pay_chnl_id);
					pay_chnl_id->flag &= ~FIELD_DATA_POLICY_RESET;
					if (pay_chnl_id->data_size == 0 || pay_chnl_id->data[0] == '\0') {
						// 如果来源于政策中的支付渠道，则每次切换时需要重新设置
						if (CALT->_CALT_global_fields[POLICY_CHNL_ID_GI]->data[0] == '\0')
							strcpy(pay_chnl_id->data, CALT->_CALT_global_fields[CHANNEL_ID_GI]->data);
						else
							strcpy(pay_chnl_id->data, CALT->_CALT_global_fields[POLICY_CHNL_ID_GI]->data);

						pay_chnl_id->flag |= FIELD_DATA_POLICY_RESET;
						pay_chnl_id->data_size = 1;
					}
				}
			}
			
			if (do_rule(policy))
				matches++;
		}
	}

	if (matches == 0) {
#if !defined(INV_DEBUG)
		if (!CALP->_CALP_inv_out){
			SAT->_SAT_err_info = (boost::format("::errno=%1%") % ECOMM_NO_MATCH).str();
			SAT->_SAT_enable_err_info = true;
			BOOST_SCOPE_EXIT((&SAT)) {
				SAT->_SAT_enable_err_info = false;
			} BOOST_SCOPE_EXIT_END
			oinv_mgr->write(0);
			master.record_invalid++;
		}
#else
		if (!CALP->_CALP_debug_inv) {
			SAT->_SAT_err_info = (boost::format("::errno=%1%") % ECOMM_NO_MATCH).str();
			SAT->_SAT_enable_err_info = true;
			BOOST_SCOPE_EXIT((&SAT)) {
				SAT->_SAT_enable_err_info = false;
			} BOOST_SCOPE_EXIT_END
			oinv_mgr->write(0);
			master.record_invalid++;
		}
#endif
	} else {
		master.record_normal++;
	}
}

// 按政策和规则累计
bool cal_base::accumulate(const cal_policy_t& policy, const cal_rule_t& rule_cfg)
{
	bool retval = false;
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", &retval);
#endif
	const cal_parms_t& parms = CALP->_CALP_parms;
	cal_tree& tree_mgr = cal_tree::instance(CALT);
	bool is_set = false;
	int cycle_id = *reinterpret_cast<int *>(CALT->_CALT_global_fields[CYCLE_ID_GI]->data);

	// 调整cycle_id为0
	//*reinterpret_cast<int *>(CALT->_CALT_global_fields[CYCLE_ID_GI]->data) = 0;

	BOOST_SCOPE_EXIT((&CALT)(&cycle_id)) {
		strcpy(CALT->_CALT_global_is_accumulate, "0");
		*reinterpret_cast<int *>(CALT->_CALT_global_fields[CYCLE_ID_GI]->data) = cycle_id;
	} BOOST_SCOPE_EXIT_END

	bool ret_value = true;
	for (int i = 0; i < 2; i++) {
		const vector<cal_accu_cond_t> *accu_cond;

		if (i == 0) {
			strcpy(CALT->_CALT_global_is_accumulate, "1");
			accu_cond = &rule_cfg.accu_cond;
		} else {
			strcpy(CALT->_CALT_global_is_accumulate, "2");
			accu_cond = &rule_cfg.acct_busi_cond;
		}

		BOOST_FOREACH(const cal_accu_cond_t& cond_item, *accu_cond) {
			if (cond_item.cycles > 1)
				*reinterpret_cast<int *>(CALT->_CALT_global_fields[CYCLE_ID_GI]->data) = 0;
			else
				*reinterpret_cast<int *>(CALT->_CALT_global_fields[CYCLE_ID_GI]->data) = cycle_id;
			
			if (!match_busi(policy.flag, rule_cfg.flag, cond_item.flag))
				continue;

			if (cond_item.cycles > 1 && cycle_id >= cond_item.cycles)
				continue;

			strcpy(CALT->_CALT_global_busi_code, cond_item.busi_code.c_str());

			if (!is_set) {
				pre_write();
				is_set = true;
			}

			field_data_t *field_data = cond_item.field_data;
			if (!(field_data->flag & FIELD_DATA_GOTTEN))
				tree_mgr.get_field_value(field_data);

			if (field_data->table_id == -1) { // 来自于输入
				if (field_data->subject_index == -1) {
					sprintf(CALT->_CALT_global_busi_value, "%lf", field_data->get_double());
					odistr_mgr->write(0);
				} else {
					string subject_id = CALT->_CALT_input_fields[field_data->subject_index]->data;
					

					if (tree_mgr.in_subject(field_data->ref_busi_code, subject_id)) {
						//代办业务驱动源查重
    					if (parms.driver_data == "07" && rule_cfg.calc_cfg.calc_symbol == 1) {
        					if (!insert_dup(DUP_CRBO_LEVEL)) {
            						ret_value=false;
									continue;
        						}
    					}
						sprintf(CALT->_CALT_global_busi_value, "%lf", field_data->get_double());
						odistr_mgr->write(0);
					}
				}
			} else { // 来自于表
				sprintf(CALT->_CALT_global_busi_value, "%lf", field_data->get_double());
				odistr_mgr->write(0);
			}
		}
	}

	return ret_value;
}

// 设置计算轨迹
void cal_base::set_track(int track_stage)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, (_("track_stage={1}") % track_stage).str(APPLOCALE), NULL);
#endif
	// 设置政策相关的轨迹
	cal_policy& policy_mgr = cal_policy::instance(CALT);
	cal_rule& rule_mgr = cal_rule::instance(CALT);
	cal_tree& tree_mgr = cal_tree::instance(CALT);
	const cal_rule_t& rule = rule_mgr.get_rule(CALT->_CALT_rule_id);
	vector<cal_cond_para_t>::const_iterator cond_iter;
	field_data_t *comm_track_field = CALT->_CALT_global_fields[COMM_TRACK_GI];
	char *comm_track = comm_track_field->data;
	int& field_size = comm_track_field->field_size;
	field_data_t *field_data;
	bool first = true;
	ostringstream fmt;

	switch (track_stage) {
	case TRACK_STAGE_COND:
		{
			const cal_policy_t& policy = policy_mgr.get_policy(CALT->_CALT_mod_id);
			const vector<cal_policy_detail_t>& details = policy.details;
			vector<cal_policy_detail_t>::const_iterator field_iter;
			for (field_iter = details.begin(); field_iter != details.end(); ++field_iter) {
				field_data = field_iter->field_data;
				if (!(field_data->flag & FIELD_DATA_GOTTEN))
					continue;
				if (field_iter->next)   //同一类中应该取或关系的，只取最后一个，否则会重复
					continue;

				if (first)
					first = false;
				else
					fmt << ';';

				fmt << *field_data;
			}

			BOOST_FOREACH(field_data_t *track_field, rule.track_fields[0]) {
				if (!(track_field->flag & FIELD_DATA_GOTTEN))
					continue;

				if (first)
					first = false;
				else
					fmt << ';';

				fmt << *track_field;
			}

			string track_str = fmt.str();

			// 如果预分配的内存不足，则扩展该内存
			if (track_str.length() >= field_size) {
				delete []comm_track_field->data;
				comm_track_field->field_size = track_str.length() + 1;
				comm_track_field->data = new char[comm_track_field->field_size];
				comm_track = comm_track_field->data;
			}

			memcpy(comm_track, track_str.c_str(), track_str.length() + 1);
			break;
		}
	case TRACK_STAGE_CALC:
		{
			BOOST_FOREACH(field_data_t *track_field, rule.track_fields[1]) {
				if (!(track_field->flag & FIELD_DATA_GOTTEN))
					continue;

				if (first)
					first = false;
				else
					fmt << ';';

				fmt << *track_field;
			}

			string track_str = fmt.str();
			if (!track_str.empty()) {
				int track_len = strlen(comm_track);
				if (track_len > 0) {
					comm_track[track_len] = ';';
					track_len++;
				}

				// 如果预分配的内存不足，则扩展该内存
				if (track_len + track_str.length() >= field_size) {
					char *old_data = comm_track_field->data;
					comm_track_field->field_size = track_len + track_str.length() + 1;
					comm_track_field->data = new char[comm_track_field->field_size];
					comm_track = comm_track_field->data;
					memcpy(comm_track, old_data, track_len);
					delete []old_data;
				}

				memcpy(comm_track + track_len, track_str.c_str(), track_str.length() + 1);
			}
			break;
		}
	case TRACK_STAGE_PAY:
		{
			BOOST_FOREACH(field_data_t *track_field, rule.track_fields[2]) {
				if (!(track_field->flag & FIELD_DATA_GOTTEN))
					tree_mgr.get_field_value(track_field);

				if (first)
					first = false;
				else
					fmt << ';';

				fmt << *track_field;
			}

			string track_str = fmt.str();
			if (!track_str.empty()) {
				int track_len;
				char *ptr = strstr(comm_track, ";;");
				if (ptr != NULL)
					track_len = ptr - comm_track;
				else
					track_len = strlen(comm_track);

				// 如果预分配的内存不足，则扩展该内存
				if (track_len + 2 + track_str.length() >= field_size) {
					char *old_data = comm_track_field->data;
					comm_track_field->field_size = track_len + track_str.length() + 3;
					comm_track_field->data = new char[comm_track_field->field_size];
					comm_track = comm_track_field->data;
					memcpy(comm_track, old_data, track_len);
					delete []old_data;
				}

				comm_track[track_len] = ';';
				comm_track[track_len + 1] = ';';
				memcpy(comm_track + track_len + 2, track_str.c_str(), track_str.length() + 1);
			}
			break;
		}
	default:
		break;
	}
}

// 重新初始化服务渠道相关对象 ，并重新设置发展渠道及发展人相关信息
void cal_base::reset_svc_chnl_fields()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif
	field_data_map_t& data_map = CALT->_CALT_data_map;
	for (field_data_map_t::iterator iter = data_map.begin(); iter != data_map.end(); ++iter) {
		string field_name = iter->first;
		field_data_t *field_data = iter->second;
		//渠道表查找的字段需要按服务渠道重新查找
		if (strcmp(field_name.c_str(), "sett_province_code") == 0 
			|| strcmp(field_name.c_str(), "sett_city_code") == 0 
			|| strcmp(field_name.c_str(), "pay_chnl_name") == 0 
			|| strcmp(field_name.c_str(), "manager_dept_id") == 0 
			|| strcmp(field_name.c_str(), "super_chnl_id") == 0 
			|| strcmp(field_name.c_str(), "pay_manager_dept_id") == 0 
			|| strcmp(field_name.c_str(), "manager_dept_type") == 0 
			|| strcmp(field_name.c_str(), "pay_chnl_kind_id") == 0 
			|| strcmp(field_name.c_str(), "chnl_kind_id") == 0 
			|| strcmp(field_name.c_str(), "chnl_province_code") == 0 
			|| strcmp(field_name.c_str(), "chnl_city_code") == 0 
			|| strcmp(field_name.c_str(), "chnl_name") == 0 
			|| strcmp(field_name.c_str(), "manager_dept_id") == 0) {
			field_data->data_size = 0;
			field_data->flag &= ~(FIELD_DATA_SET_DEFAULT | FIELD_DATA_GOTTEN);
		}
		
/*
		//发展人表查找的字段初始化
		if ( field_data->table_id == 38 && (field_data->flag & FIELD_DATA_RESET)) {
			char *data = field_data->data;
			switch (field_data->field_type) {
				case SQLTYPE_CHAR:
					data[0] = '\0';
					break;
				case SQLTYPE_SHORT:
					*reinterpret_cast<short *>(data) = 0;
					break;
				case SQLTYPE_INT:
					*reinterpret_cast<int *>(data) = 0;
					break;
				case SQLTYPE_LONG:
					*reinterpret_cast<long *>(data) = 0;
					break;
				case SQLTYPE_FLOAT:
					*reinterpret_cast<float *>(data) = 0.0;
					break;
				case SQLTYPE_DOUBLE:
					*reinterpret_cast<double *>(data) = 0.0;
					break;
				case SQLTYPE_STRING:
					strcpy(data, "");
					break;
				case SQLTYPE_TIME:
					*reinterpret_cast<time_t *>(data) = 0;
					break;
			}
		}
		//发展人置空
		if (strcmp(field_name.c_str(), "dev_id") == 0 || strcmp(field_name.c_str(), "default_dev") == 0)
			strcpy(field_data->data, "");
		
		//服务渠道发展人不可支付
		if (strcmp(field_name.c_str(), "pay_comm_flag") == 0)
			strcpy(field_data->data, "0");
*/
	}
}

void cal_base::pre_write()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif
	cal_tree& tree_mgr = cal_tree::instance(CALT);
	const vector<output_field_t>& output_fields = SAP->_SAP_adaptors[sa_id].output_fields;

	for (int i = 0; i < output_fields.size(); i++) {
		const output_field_t& output_field = output_fields[i];
		field_data_t *field_data = CALT->_CALT_output_fields[i];

		if (field_data == NULL)
			continue;

		if (!(field_data->flag & FIELD_DATA_GOTTEN))
			tree_mgr.get_field_value(field_data);

		if (field_data->data_size > 1)
			throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: expect one value at most for field_name {1}, gotten {2} values") % output_field.field_name % field_data->data_size).str(APPLOCALE));

		if (field_data->data_size == 0) {
			output[i][0] = '\0';
		} else {
			char buf[128];
			int len;
			const char *data = field_data->data;

			switch(field_data->field_type) {
			case SQLTYPE_CHAR:
				len = sprintf(buf, "%c", data[0]);
				if (len <= output_field.field_size) {
					memcpy(output[i], buf, len + 1);
				} else {
					memcpy(output[i], buf, output_field.field_size);
					output[i][output_field.field_size] = '\0';
				}
				break;
			case SQLTYPE_SHORT:
				len = sprintf(buf, "%d", *reinterpret_cast<const short *>(data));
				if (len <= output_field.field_size) {
					memcpy(output[i], buf, len + 1);
				} else {
					memcpy(output[i], buf, output_field.field_size);
					output[i][output_field.field_size] = '\0';
				}
				break;
			case SQLTYPE_INT:
				len = sprintf(buf, "%d", *reinterpret_cast<const int *>(data));
				if (len <= output_field.field_size) {
					memcpy(output[i], buf, len + 1);
				} else {
					memcpy(output[i], buf, output_field.field_size);
					output[i][output_field.field_size] = '\0';
				}
				break;
			case SQLTYPE_LONG:
				len = sprintf(buf, "%ld", *reinterpret_cast<const long *>(data));
				if (len < output_field.field_size) {
					memcpy(output[i], buf, len + 1);
				} else {
					memcpy(output[i], buf, output_field.field_size);
					output[i][output_field.field_size] = '\0';
				}
				break;
			case SQLTYPE_FLOAT:
				len = sprintf(buf, "%.6f", *reinterpret_cast<const float *>(data));
				if (len < output_field.field_size) {
					memcpy(output[i], buf, len + 1);
				} else {
					memcpy(output[i], buf, output_field.field_size);
					output[i][output_field.field_size] = '\0';
				}
				break;
			case SQLTYPE_DOUBLE:
				len = sprintf(buf, "%.6lf", *reinterpret_cast<const double *>(data));
				if (len < output_field.field_size) {
					memcpy(output[i], buf, len + 1);
				} else {
					memcpy(output[i], buf, output_field.field_size);
					output[i][output_field.field_size] = '\0';
				}
				break;
			case SQLTYPE_STRING:
				len = strlen(data);
				if (len < output_field.field_size) {
					memcpy(output[i], data, len + 1);
				} else {
					memcpy(output[i], data, output_field.field_size);
					output[i][output_field.field_size] = '\0';
				}
				break;
			case SQLTYPE_TIME:
				len = sprintf(buf, "%ld", *reinterpret_cast<const time_t *>(data));
				if (len < output_field.field_size) {
					memcpy(output[i], buf, len + 1);
				} else {
					memcpy(output[i], buf, output_field.field_size);
					output[i][output_field.field_size] = '\0';
				}
				break;
			default:
				output[i][0] = '\0';
				break;
			}
		}
	}
}

// 计算当前帐期的int表示
int cal_base::get_calc_cycle()
{
	int calc_cycle;
#if defined(DEBUG)
	scoped_debug<int> debug(500, __PRETTY_FUNCTION__, "", &calc_cycle);
#endif

	gpenv& env_mgr = gpenv::instance();
	char buf[5];
	char *acct_cycle;

	if (CALP->_CALP_run_complex){
		//复合指标计算时间点判断
		acct_cycle= env_mgr.getenv("MIX_ACCT_CYCLE");
		if (acct_cycle == NULL)
			throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: environment variable MIX_ACCT_CYCLE not set").str(APPLOCALE)));
	} else {
		acct_cycle= env_mgr.getenv("ACCT_CYCLE");
		if (acct_cycle == NULL)
			throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: environment variable ACCT_CYCLE not set").str(APPLOCALE)));
	}

	memcpy(buf, acct_cycle, 4);
	buf[4] = '\0';
	calc_cycle = ::atoi(buf) * 12;

	memcpy(buf, acct_cycle + 4, 2);
	buf[2] = '\0';
	calc_cycle += ::atoi(buf);

	return calc_cycle;
}

// 设置计算账期
void cal_base::set_calc_months()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	char buf[32];
	char *calc_cycle = CALT->_CALT_global_fields[CALC_CYCLE_GI]->data;
	char *arre_cycle = CALT->_CALT_global_fields[ARRE_CYCLE_GI]->data;
	// 欠费账期初始化为计算账期
	memcpy(arre_cycle, calc_cycle, ARRE_CYCLE_LEN);
	arre_cycle[ARRE_CYCLE_LEN] = '\0';

	int& calc_months = *reinterpret_cast<int *>(CALT->_CALT_global_fields[CALC_MONTHS_GI]->data);

	memcpy(buf, calc_cycle, 4);
	buf[4] = '\0';
	int calc_year = atoi(buf);

	memcpy(buf, calc_cycle + 4, 2);
	buf[2] = '\0';
	int calc_month = atoi(buf);

	calc_months = calc_year * 12 + calc_month;
}

// 驱动源匹配关系
bool cal_base::match_driver(int policy_flag, int rule_flag)
{
	bool retval = false;
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, (_("policy_flag={1}, rule_flag={2}") % policy_flag % rule_flag).str(APPLOCALE), &retval);
#endif
	const cal_parms_t& parms = CALP->_CALP_parms;

	if (rule_flag & RULE_FLAG_UNKNOWN_CALC_BUSI)
		return retval;

	if (!parms.channel_level) { // 当前驱动源为用户级
		if ((rule_flag & RULE_FLAG_CHANNEL) && !(rule_flag & RULE_FLAG_ACCT)) {
			// 如果规则是阶梯计算的，则必须结算在渠道上
			if (rule_flag & RULE_FLAG_STEP)
				return retval;

			// 如果规则上的计算指标不可摊分，则必须结算在渠道上
			if (!(rule_flag & RULE_FLAG_ALLOCATABLE))
				return retval;
		}
		//如果规则选择了起始点为开户时间，而当前计算的规则
        //不是用户资料，则不需计算
        if ((rule_flag & RULE_FLAG_OPEN_TIME)
            && (rule_flag & RULE_FLAG_NULL_DRIVER)) 
            return false;

		// 如果规则匹配并且表达式的计算指标不包含政策指定驱动源的指标，则以规则上的驱动源为准
		if (rule_flag & RULE_FLAG_MATCH_DRIVER && !(rule_flag & RULE_FLAG_CAL_EXPR_SEPCIFY_DRIVER)) {
			retval = true;
			return retval;
		}

		// 如果规则没有指定计算指标，则必须结算在渠道上
		if (rule_flag & RULE_FLAG_NO_CALC_BUSI)
			return retval;

		// 如果规则指定驱动源，则以指定驱动源为准
		//if (!(rule_flag & RULE_FLAG_NULL_DRIVER) || (rule_flag & RULE_FLAG_MATCH_ONE_DRIVER))
		if (!(rule_flag & RULE_FLAG_NULL_DRIVER) )
			return retval;

		// 如果规则未指定驱动源，而且政策上的驱动源匹配
		if (policy_flag & POLICY_FLAG_MATCH_DRIVER) {
			retval = true;
			return retval;
		}

		return retval;
	} else {
		if (rule_flag & RULE_FLAG_CHANNEL) {
			// 如果规则是阶梯计算的，则必须结算在渠道上
			if (rule_flag & RULE_FLAG_STEP) {
				retval = true;
				return retval;
			}

			// 如果规则上的驱动源不可摊分，则必须结算在渠道上
			if (!(rule_flag & RULE_FLAG_ALLOCATABLE)) {
				retval = true;
				return retval;
			}
		}

		// 如果规则匹配并且表达式的计算指标不包含政策指定驱动源的指标，则以规则上的驱动源为准
		if (rule_flag & RULE_FLAG_MATCH_DRIVER && !(rule_flag & RULE_FLAG_CAL_EXPR_SEPCIFY_DRIVER)) {
			retval = true;
			return retval;
		}

		// 如果规则没有指定计算指标，则必须结算在渠道上
		if (rule_flag & RULE_FLAG_NO_CALC_BUSI) {
			retval = true;
			return retval;
		}

		// 如果规则指定驱动源，则以指定驱动源为准
		//if (!(rule_flag & RULE_FLAG_NULL_DRIVER) || (rule_flag & RULE_FLAG_MATCH_ONE_DRIVER))
		if (!(rule_flag & RULE_FLAG_NULL_DRIVER) )
			return retval;

		// 如果规则未指定驱动源，而且政策上的驱动源匹配
		if (policy_flag & POLICY_FLAG_MATCH_DRIVER)
			return retval;

		// 政策上指定了驱动源，则以政策上的驱动源为准
		if (!(policy_flag & POLICY_FLAG_NULL_DRIVER))
			return retval;

		retval = true;
		return retval;
	}
}

// 判断是否匹配指标
bool cal_base::match_busi(int policy_flag, int rule_flag, int accu_flag)
{
	bool retval = false;
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, (_("policy_flag={1}, rule_flag={2}, accu_flag={3}") % policy_flag % rule_flag % accu_flag).str(APPLOCALE), &retval);
#endif

	// 如果政策上指定了驱动源，而累计指标未指定驱动源，
	// 则不需要累计，由指定驱动源进行累计
	if (!(policy_flag & (POLICY_FLAG_NULL_DRIVER | POLICY_FLAG_MATCH_DRIVER))
		&& (accu_flag & ACCU_FLAG_NULL_DRIVER))
		return retval;

	//如果规则选择了起始点为开户时间，而当前计算的规则
	//不是用户资料，则不累计
	if ((rule_flag & RULE_FLAG_OPEN_TIME)
		&& (rule_flag & RULE_FLAG_NULL_DRIVER))
		return retval;

	// 累计指标的驱动源不匹配当前驱动源，也不是未指定驱动源
	if (!(accu_flag & (ACCU_FLAG_NULL_DRIVER | ACCU_FLAG_MATCH_DRIVER)))
		return retval;

	retval = true;
	return retval;
}

// 收集驱动源政策，写入表中
void cal_base::collect_driver()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif
	const cal_parms_t& parms = CALP->_CALP_parms;
	const map<int, cal_policy_t>& policy_map = CALT->_CALT_policy_map;
	cal_rule& rule_mgr = cal_rule::instance(CALT);

	if (SAT->_SAT_db == NULL) {
		try {
			map<string, string> conn_info;
			database_factory& factory_mgr = database_factory::instance();
			factory_mgr.parse(SAP->_SAP_resource.openinfo, conn_info);
			SAT->_SAT_db = factory_mgr.create(SAP->_SAP_resource.dbname);
			SAT->_SAT_db->connect(conn_info);
		} catch (exception& ex) {
			delete SAT->_SAT_db;
			SAT->_SAT_db = NULL;
			throw bad_sql(__FILE__, __LINE__, SGEOS, 0, ex.what());
		}
	}

	Generic_Database *db = SAT->_SAT_db;

	try {
		string sql_stmt = "delete from unical_driver_info where driver_data = :driver_data{char[31]} and province_code = :province_code{char[31]}";

		auto_ptr<Generic_Statement> auto_stmt(db->create_statement());
		Generic_Statement *stmt = auto_stmt.get();

		auto_ptr<struct_dynamic> auto_data(db->create_data(false, 1));
		struct_dynamic *data = auto_data.get();
		if (data == NULL) {
			GPP->write_log(__FILE__, __LINE__, (_("ERROR: Memory allocation failure")).str(APPLOCALE));
			exit(1);
		}

		data->setSQL(sql_stmt);
		stmt->bind(data);

		stmt->setString(1, parms.driver_data);
		stmt->setString(2, parms.province_code);
		stmt->executeUpdate();
	} catch (bad_sql& ex) {
		throw bad_msg(__FILE__, __LINE__, 0, ex.what());
	}

	try {
		string sql_stmt = "insert into unical_driver_info(driver_data, mod_id, rule_id, flag, province_code) "
			"values(:driver_data{char[31]}, :mod_id{int}, :rule_id{int}, :flag{int}, :province_code{char[31]})";

		auto_ptr<Generic_Statement> auto_stmt(db->create_statement());
		Generic_Statement *stmt = auto_stmt.get();

		auto_ptr<struct_dynamic> auto_data(db->create_data());
		struct_dynamic *data = auto_data.get();
		if (data == NULL) {
			GPP->write_log(__FILE__, __LINE__, (_("ERROR: Memory allocation failure")).str(APPLOCALE));
			exit(1);
		}

		data->setSQL(sql_stmt);
		stmt->bind(data);

		int update_count = 0;
		for (map<int, cal_policy_t>::const_iterator policy_iter = policy_map.begin(); policy_iter != policy_map.end(); ++policy_iter) {
			const cal_policy_t& policy = policy_iter->second;
			const vector<int>& rules = policy.rules;

			for (vector<int>::const_iterator rule_iter = rules.begin(); rule_iter != rules.end(); ++rule_iter) {
				cal_rule_t& rule = rule_mgr.get_rule(*rule_iter);
				int flag;

				if (parms.stage == 1) {
					if (match_driver(policy.flag, rule.flag)) {
						flag = 1;
					} else {
						bool should_accu = false;
						BOOST_FOREACH(const cal_accu_cond_t& cond_item, rule.accu_cond) {
							//如果规则选择了起始点为开户时间，而当前计算的规则
							//不是用户资料，则不累计
							if ((rule.flag & RULE_FLAG_OPEN_TIME)
								&& (rule.flag & RULE_FLAG_NULL_DRIVER))
								continue;

							// 累计指标的驱动源不匹配当前驱动源，也不是未指定驱动源
							if (!(cond_item.flag & (ACCU_FLAG_NULL_DRIVER | ACCU_FLAG_MATCH_DRIVER)))
								continue;

							// 如果政策上指定了驱动源，而累计指标未指定驱动源，
							// 则不需要累计，由指定驱动源进行累计
							if ((policy.flag & (POLICY_FLAG_NULL_DRIVER | POLICY_FLAG_MATCH_DRIVER))
								|| !(cond_item.flag & ACCU_FLAG_NULL_DRIVER)) {
								should_accu = true;
								break;
							}
						}

						BOOST_FOREACH(const cal_accu_cond_t& cond_item, rule.acct_busi_cond) {
							//如果规则选择了起始点为开户时间，而当前计算的规则
							//不是用户资料，则不累计
							if ((rule.flag & RULE_FLAG_OPEN_TIME)
								&& (rule.flag & RULE_FLAG_NULL_DRIVER))
								continue;

							// 累计指标的驱动源不匹配当前驱动源，也不是未指定驱动源
							if (!(cond_item.flag & (ACCU_FLAG_NULL_DRIVER | ACCU_FLAG_MATCH_DRIVER)))
								continue;

							// 如果政策上指定了驱动源，而累计指标未指定驱动源，
							// 则不需要累计，由指定驱动源进行累计
							if ((policy.flag & (POLICY_FLAG_NULL_DRIVER | POLICY_FLAG_MATCH_DRIVER))
								|| !(cond_item.flag & ACCU_FLAG_NULL_DRIVER)) {
								should_accu = true;
								break;
							}
						}

						if (!should_accu) {
							GPP->write_log(__FILE__, __LINE__, (_("INFO: rule_id {1} for mod_id {2} has no accumulation busi_code") % *rule_iter % policy_iter->first).str(APPLOCALE));
							continue;
						}

						flag = 0;
					}
				} else {
					if (match_driver(policy.flag, rule.flag))
						flag = 1;
					else
						flag = 0;
				}

				if (update_count > 0)
					stmt->addIteration();

				stmt->setString(1, parms.driver_data);
				stmt->setInt(2, policy_iter->first);
				stmt->setInt(3, *rule_iter);
				stmt->setInt(4, flag);
				stmt->setString(5, parms.province_code);

				if (++update_count == data->size()) {
					stmt->executeUpdate();
					update_count = 0;
				}
			}
		}

		if (update_count > 0)
			stmt->executeUpdate();
	} catch (bad_sql& ex) {
		throw bad_msg(__FILE__, __LINE__, 0, ex.what());
	}

	db->commit();
}

bool cal_base::insert_dup(int dup_flag)
{
	bool retval = false;
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", &retval);
#endif

	if (dup_func == NULL) {
		retval = true;
		return retval;
	}
	if (dup_flag == DUP_ACCT_LEVEL)
		(void)(*dup_func)(NULL, SAT->_SAT_global, const_cast<const char **>(output), dup_output);
	else if  (dup_flag == DUP_CRBO_LEVEL)
		(void)(*dup_crbo_func)(NULL, SAT->_SAT_global, const_cast<const char **>(output), dup_output);

	char *dup_key = dup_output[DUP_KEY_IDX];
	int len = strlen(dup_key);

	int partition_id = static_cast<int>(boost::hash_range(dup_key, dup_key + len) % CALP->_CALP_parms.dup_all_partitions);
	svc_name = "DUP_";
	svc_name += CALP->_CALP_parms.dup_svc_key;
	svc_name += "_";
	svc_name += boost::lexical_cast<string>(partition_id);

	packup_dup();

	bool call_logged = false;
	while (!api_mgr.call(svc_name.c_str(), snd_msgs[0], rcv_msgs[0], 0)) {
		if (!call_logged) {
			api_mgr.write_log(__FILE__, __LINE__, (_("WARN: call failed, op_name {1}, urcode {2} - {3}") % svc_name % api_mgr.get_ur_code() % api_mgr.strerror()).str(APPLOCALE));
			call_logged = true;
		}

		if (!check_policy())
			return retval;
	}

	extract_dup();

	if (status == 0) {
		retval = true;
		return retval;
	}

	return retval;
}

void cal_base::packup_dup()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	sg_message& msg = *snd_msgs[0];
	rqst = reinterpret_cast<sa_rqst_t *>(msg.data());
	strcpy(rqst->svc_key, CALP->_CALP_parms.dup_svc_key.c_str());
	rqst->version = CALP->_CALP_parms.dup_version;
	rqst->flags = 0;
	rqst->rows = 1;
	rqst->global_size = 0;
	rqst->input_size = 3;
	rqst->user_id = SAT->_SAT_user_id;
	rqst_data = reinterpret_cast<char *>(&rqst->placeholder);

	int *record_serial = reinterpret_cast<int *>(rqst_data);
	*record_serial = 0;

	int *input_sizes = record_serial + 1;

	rqst_data = reinterpret_cast<char *>(input_sizes + rqst->input_size);
	for (int i = 0; i < rqst->input_size; i++) {
		int data_size = strlen(dup_output[i]) + 1;

		input_sizes[i] = data_size;
		memcpy(rqst_data, dup_output[i], data_size);
		rqst_data += data_size;
	}

	rqst->datalen = rqst_data - reinterpret_cast<char *>(rqst);
	msg.set_length(rqst->datalen);
}

// 解包
void cal_base::extract_dup()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	message_pointer& rcv_msg = rcv_msgs[0];

	rply = reinterpret_cast<sa_rply_t *>(rcv_msg->data());
	results = &rply->placeholder;

	// 设置状态
	status = *results;
}

void cal_base::do_merge()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif
	const vector<input_field_t>& field_vector = SAP->_SAP_adaptors[sa_id].input_records[0].field_vector;
	char **input = get_input(0);

	if (is_busi) {
		cal_busi_key_t key;
		int i;

		for (i = FIXED_INPUT_SIZE; i < field_vector.size() - 2; i++)
			key.push_back(input[i]);

		double busi_value = atof(input[i]);
		int busi_count = atoi(input[i + 1]);

		map<cal_busi_key_t, cal_busi_value_t>::iterator iter = merge_map.find(key);
		if (iter == merge_map.end()) {
			merge_map.insert(std::make_pair(key, cal_busi_value_t(busi_value, busi_count)));
		} else {
			cal_busi_value_t& value = iter->second;
			value.busi_value += busi_value;
			value.busi_count += busi_count;
		}
	} else {
		cal_accu_key_t key;
		int i;

		for (i = FIXED_INPUT_SIZE; i < field_vector.size() - 1; i++)
			key.push_back(input[i]);

		map<cal_accu_key_t, cal_accu_value_t>::iterator iter = accu_map.find(key);
		if (iter == accu_map.end()) {
			cal_accu_value_t value;
			for (; i < field_vector.size(); i++)
				value.push_back(atof(input[i]));

			accu_map.insert(std::make_pair(key, value));
		} else {
			cal_accu_value_t& value = iter->second;
			for (int j = 0; j < value.size(); j++)
				value[j] += atof(input[i + j]);
		}
	}
}

void cal_base::write_busi()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif
	char *acct_cycle;
	gpenv& env_mgr = gpenv::instance();
	acct_cycle= env_mgr.getenv("ACCT_CYCLE");
	if (acct_cycle == NULL)
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: environment variable ACCT_CYCLE not set").str(APPLOCALE)));

	map<string, string>::const_iterator iter = adaptor.args.find("dst_dir");
	if (iter == adaptor.args.end())
		throw bad_msg(__FILE__, __LINE__, SGEINVAL, (_("ERROR: Can't find dst_dir in entry args.arg.name")).str(APPLOCALE));

	string dst_dir;
	env_mgr.expand_var(dst_dir, iter->second);
	if (*dst_dir.rbegin() != '/')
		dst_dir += '/';

	iter = adaptor.args.find("busi_dir");
	if (iter == adaptor.args.end())
		throw bad_msg(__FILE__, __LINE__, SGEINVAL, (_("ERROR: Can't find busi_dir in entry args.arg.name")).str(APPLOCALE));

	string busi_dir;
	env_mgr.expand_var(busi_dir, iter->second);
	if (*busi_dir.rbegin() != '/')
		busi_dir += '/';

	string acct_month = SAT->_SAT_raw_file_name.substr(10, 6);
	string file_name = (boost::format("%1%ai_idx_busi_value.seq.%|2$010|%3%") % dst_dir % SAT->_SAT_global[GLOBAL_FILE_SN_SERIAL] % acct_cycle).str();
	ofstream ofs(file_name.c_str());
	if (!ofs)
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Can't open file {1} to write out busi data.") % file_name).str(APPLOCALE));

	// 文件名为: 10位序列号+6位帐期+4位文件类型+ ".G0"
	string his_file_name = (boost::format("%1%%|2$010|%3%%4%.G0") % busi_dir % SAT->_SAT_global[GLOBAL_FILE_SN_SERIAL] % acct_cycle % adaptor.parms.svc_key.substr(0, 4)).str();
	ofstream his_ofs(his_file_name.c_str());
	if (!his_ofs)
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Can't open file {1} to write out busi history data.") % his_file_name).str(APPLOCALE));

	// 计算当前帐期的int表示
	int calc_cycle;
	char buf[5];

	memcpy(buf, acct_cycle, 4);
	buf[4] = '\0';
	calc_cycle = atoi(buf) * 12;

	memcpy(buf, acct_cycle + 4, 2);
	buf[2] = '\0';
	calc_cycle += atoi(buf);

	set<cal_rule_complex_t>& rule_complex_set = CALT->_CALT_rule_complex_set;
	int const MAX_COMPLEX_RECORDS = 256;
	t_ai_complex_busi_deposit complex_results[MAX_COMPLEX_RECORDS];
	sdc_api& sdc_mgr = sdc_api::instance();

	//防止遗漏计算当月无复合指标数据的渠道，增加对所有渠道的循环判断
	if(!CALP->_CALP_run_complex){
		set<string>::iterator chnl_iter;
		map<cal_busi_key_t, cal_busi_value_t>::const_iterator merge_map_iter;
	
		for (chnl_iter = channel_set.begin(); chnl_iter != channel_set.end(); ++chnl_iter){
			string cur_chnl_id = *chnl_iter;
			set<cal_mod_rule_complex_t>::const_iterator complex_set_iter;
		
			for (complex_set_iter = mod_rule_complex_set.begin(); complex_set_iter != mod_rule_complex_set.end(); ++complex_set_iter) {
				cal_busi_key_t merge_item;
				cal_mod_rule_complex_t cur_complex = *complex_set_iter;
				ostringstream fmt;
				string str_mod_id;
				string str_rule_id;
				string str_cycle_id = "0";
				double busi_value = 0.0;
				int busi_count = 0;
				
				merge_item.push_back(cur_chnl_id);
				
				fmt.str("");
				fmt << cur_complex.mod_id;
				str_mod_id = fmt.str();				
				merge_item.push_back(str_mod_id);
				
				fmt.str("");
				fmt << cur_complex.rule_id;
				str_rule_id = fmt.str();
				merge_item.push_back(str_rule_id);
				
				merge_item.push_back(str_cycle_id);
				merge_item.push_back(cur_complex.busi_code);
				
				merge_map_iter = merge_map.find(merge_item);
				if (merge_map_iter == merge_map.end()) {					
					merge_map.insert(std::make_pair(merge_item, cal_busi_value_t(busi_value, busi_count)));
				}
			}
		}
	}

	map<cal_busi_key_t, cal_busi_value_t>::const_iterator merge_iter;
	for (merge_iter = merge_map.begin(); merge_iter != merge_map.end(); ++merge_iter) {
		const cal_busi_key_t& key = merge_iter->first;
		const cal_busi_value_t& value = merge_iter->second;
		int mod_id = atoi(key[1].c_str());
		int rule_id = atoi(key[2].c_str());

		cal_rule_complex_t item;
		item.rule_id = rule_id;
		item.busi_code = key[4];

		set<cal_rule_complex_t>::const_iterator complex_iter = rule_complex_set.find(item);
		if (complex_iter == rule_complex_set.end()) {
			// 不是复合指标
			if (CALP->_CALP_run_complex) {
				map<string, cal_busi_t>& busi_map = CALT->_CALT_busi_map;
				map<string, cal_busi_t>::iterator busi_iter = busi_map.find(item.busi_code);
				cal_busi_t& busi_item = busi_iter->second;
				if (busi_item.busi_type > 0) {
					ofs << std::setw(CHANNEL_ID_LEN) << key[0]
						<< std::setw(MOD_ID_LEN) << key[1]
						<< std::setw(RULE_ID_LEN) << key[2]
						<< std::setw(CYCLE_ID_LEN) << key[3]
						<< std::setw(BUSI_CODE_LEN) << key[4]
						<< std::setw(BUSI_VALUE_LEN) << std::fixed << std::setprecision(6) << value.busi_value << '\n';
				}
			}else{
				ofs << std::setw(CHANNEL_ID_LEN) << key[0]
					<< std::setw(MOD_ID_LEN) << key[1]
					<< std::setw(RULE_ID_LEN) << key[2]
					<< std::setw(CYCLE_ID_LEN) << key[3]
					<< std::setw(BUSI_CODE_LEN) << key[4]
					<< std::setw(BUSI_VALUE_LEN) << std::fixed << std::setprecision(6) << value.busi_value << '\n';
			}
		} else {
			// 需要保留原始数据
			if(value.busi_value != 0 || value.busi_count != 0) {
				his_ofs << key[0] << ','
					<< key[1] << ','
					<< key[2] << ','
					<< key[4] << ','
					<< value.busi_value << ','
					<< value.busi_count << '\n';
			}

			// 正常计算才执行，只是为了跑复合指标的历史数据则不计算
			if (!CALP->_CALP_run_complex) {
				const cal_rule_complex_t& complex_cfg = *complex_iter;

				// 获取两个时间段的分界点
				int cycle_end = calc_cycle + 1;
				int cycle_begin = cycle_end - complex_cfg.units;
				int last_cycle_begin = cycle_begin - complex_cfg.units;
				int last_cycle_end = cycle_begin - 1;

				t_ai_complex_busi_deposit complex_item;
				memcpy(complex_item.channel_id, key[0].c_str(), key[0].length() + 1);
				complex_item.mod_id = mod_id;
				complex_item.rule_id = rule_id;
				memcpy(complex_item.busi_code, key[4].c_str(), key[4].length() + 1);
				complex_item.acct_month = last_cycle_begin;

				double cur_data = value.busi_value;
				int cur_count = value.busi_count;
				double last_data = 0.0;
				int last_count = 0;

				double cur_end_data = cur_data;
				int cur_end_count = cur_count;
				double last_end_data = 0.0;
				int last_end_count = 0;

				int count = sdc_mgr.find(T_AI_COMPLEX_BUSI_DEPOSIT, &complex_item, complex_results, MAX_COMPLEX_RECORDS);
				for (int i = 0; i < count; i++) {
					t_ai_complex_busi_deposit& record = complex_results[i];
					if (record.acct_month >= cycle_end)
						continue;

					if (record.acct_month >= cycle_begin) {
						cur_data += record.busi_value;
						cur_count += record.busi_count;
					} else {
						last_data += record.busi_value;
						last_count += record.busi_count;
						if (record.acct_month == last_cycle_end) {
							last_end_data += record.busi_value;
							last_end_count += record.busi_count;
						}
					}
				}

				if (cur_data == 0 && count == 0)
					continue;

				if (complex_cfg.complex_type == COMPLEX_TYPE_SGR && last_end_data == 0)
					continue;

				ofs << std::setw(CHANNEL_ID_LEN) << key[0]
					<< std::setw(MOD_ID_LEN) << key[1]
					<< std::setw(RULE_ID_LEN) << key[2]
					<< std::setw(CYCLE_ID_LEN) << key[3]
					<< std::setw(BUSI_CODE_LEN) << key[4]
					<< std::setw(BUSI_VALUE_LEN) << std::fixed << std::setprecision(6);

				switch (complex_cfg.complex_type) {
				case COMPLEX_TYPE_GROWTH:
					ofs << (cur_end_data - last_end_data);
					break;
				case COMPLEX_TYPE_SUM:
					ofs << cur_data;
					break;
				case COMPLEX_TYPE_SGR:
					ofs << (cur_end_data - last_end_data) / last_end_data;
					break;
				case COMPLEX_TYPE_GR:
					ofs << (cur_end_data - last_end_data) / cur_end_count;
					break;
				case COMPLEX_TYPE_AVG:
					ofs << cur_data / cur_count;
					break;
				}

				ofs << '\n';
			}
		}
	}
	ofs.close();
	his_ofs.close();
    struct stat st;
    if (::stat(file_name.c_str(), &st) < 0)    // Skips error.
        return;
    if (st.st_size == 0)
        ::remove(file_name.c_str());
	if (::stat(his_file_name.c_str(), &st) < 0)    // Skips error.
        return;
    if (st.st_size == 0)
        ::remove(his_file_name.c_str());
}

void cal_base::write_acct_busi()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	gpenv& env_mgr = gpenv::instance();

	map<string, string>::const_iterator iter = adaptor.args.find("dst_dir");
	if (iter == adaptor.args.end())
		throw bad_msg(__FILE__, __LINE__, SGEINVAL, (_("ERROR: Can't find dst_dir in entry args.arg.name")).str(APPLOCALE));

	string dst_dir;
	env_mgr.expand_var(dst_dir, iter->second);
	if (*dst_dir.rbegin() != '/')
		dst_dir += '/';

	iter = adaptor.args.find("busi_dir");
	if (iter == adaptor.args.end())
		throw bad_msg(__FILE__, __LINE__, SGEINVAL, (_("ERROR: Can't find busi_dir in entry args.arg.name")).str(APPLOCALE));

	string busi_dir;
	env_mgr.expand_var(busi_dir, iter->second);
	if (*busi_dir.rbegin() != '/')
		busi_dir += '/';

	string acct_month = SAT->_SAT_raw_file_name.substr(10, 6);
	string file_name = (boost::format("%1%ai_idx_acct_busi_value.seq.%|2$010|%3%") % dst_dir % SAT->_SAT_global[GLOBAL_FILE_SN_SERIAL] % acct_month).str();
	ofstream ofs(file_name.c_str());
	if (!ofs)
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Can't open file {1} to write out busi data.") % file_name).str(APPLOCALE));

	// 文件名为: 10位序列号+6位帐期+4位文件类型+ ".G1"
	file_name = (boost::format("%1%%|2$010|%3%%4%.G1") % busi_dir % SAT->_SAT_global[GLOBAL_FILE_SN_SERIAL] % acct_month % adaptor.parms.svc_key.substr(0, 4)).str();
	ofstream his_ofs(file_name.c_str());
	if (!his_ofs)
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Can't open file {1} to write out busi history data.") % file_name).str(APPLOCALE));

	// 计算当前帐期的int表示
	int calc_cycle;
	char buf[5];

	memcpy(buf, acct_month.c_str(), 4);
	buf[4] = '\0';
	calc_cycle = atoi(buf) * 12;

	memcpy(buf, acct_month.c_str() + 4, 2);
	buf[2] = '\0';
	calc_cycle += atoi(buf);

	set<cal_rule_complex_t>& rule_complex_set = CALT->_CALT_rule_complex_set;
	int const MAX_COMPLEX_RECORDS = 256;
	t_ai_complex_acct_busi_deposit complex_results[MAX_COMPLEX_RECORDS];
	sdc_api& sdc_mgr = sdc_api::instance();

	map<cal_busi_key_t, cal_busi_value_t>::const_iterator merge_iter;
	for (merge_iter = merge_map.begin(); merge_iter != merge_map.end(); ++merge_iter) {
		const cal_busi_key_t& key = merge_iter->first;
		const cal_busi_value_t& value = merge_iter->second;
		int mod_id = atoi(key[2].c_str());
		int rule_id = atoi(key[3].c_str());

		cal_rule_complex_t item;
		item.rule_id = rule_id;
		item.busi_code = key[5];

		set<cal_rule_complex_t>::const_iterator complex_iter = rule_complex_set.find(item);
		if (complex_iter == rule_complex_set.end()) {
			// 不是复合指标
			ofs << std::setw(ACCT_NO_LEN) << key[0]
				<< std::setw(CHANNEL_ID_LEN) << key[1]
				<< std::setw(MOD_ID_LEN) << key[2]
				<< std::setw(RULE_ID_LEN) << key[3]
				<< std::setw(CYCLE_ID_LEN) << key[4]
				<< std::setw(BUSI_CODE_LEN) << key[5]
				<< std::setw(BUSI_VALUE_LEN) << std::fixed << std::setprecision(6) << value.busi_value << '\n';
		} else {
			// 需要保留原始数据
			his_ofs << key[0] << ','
				<< key[1] << ','
				<< key[2] << ','
				<< key[3] << ','
				<< key[5] << ','
				<< value.busi_value << ','
				<< value.busi_count << '\n';

			const cal_rule_complex_t& complex_cfg = *complex_iter;

			// 获取两个时间段的分界点
			int cycle_end = calc_cycle + 1;
			int cycle_begin = cycle_end - complex_cfg.units;
			int last_cycle_begin = cycle_begin - complex_cfg.units;

			t_ai_complex_acct_busi_deposit complex_item;
			memcpy(complex_item.acct_no, key[0].c_str(), key[0].length() + 1);
			memcpy(complex_item.channel_id, key[1].c_str(), key[1].length() + 1);
			complex_item.mod_id = mod_id;
			complex_item.rule_id = rule_id;
			memcpy(complex_item.busi_code, key[5].c_str(), key[5].length() + 1);
			complex_item.acct_month = last_cycle_begin;

			double cur_data = value.busi_value;
			int cur_count = value.busi_count;
			double last_data = 0.0;
			int last_count = 0;

			int count = sdc_mgr.find(T_AI_COMPLEX_ACCT_BUSI_DEPOSIT, &complex_item, complex_results, MAX_COMPLEX_RECORDS);
			for (int i = 0; i < count; i++) {
				t_ai_complex_acct_busi_deposit& record = complex_results[i];
				if (record.acct_month >= cycle_end)
					continue;

				if (record.acct_month >= cycle_begin) {
					cur_data += record.busi_value;
					cur_count += record.busi_count;
				} else {
					last_data += record.busi_value;
					last_count += record.busi_count;
				}
			}

			if (complex_cfg.complex_type == COMPLEX_TYPE_SGR && last_data == 0)
				continue;

			ofs << std::setw(ACCT_NO_LEN) << key[0]
				<< std::setw(CHANNEL_ID_LEN) << key[1]
				<< std::setw(MOD_ID_LEN) << key[2]
				<< std::setw(RULE_ID_LEN) << key[3]
				<< std::setw(CYCLE_ID_LEN) << key[4]
				<< std::setw(BUSI_CODE_LEN) << key[5]
				<< std::setw(BUSI_VALUE_LEN) << std::fixed << std::setprecision(6);

			switch (complex_cfg.complex_type) {
			case COMPLEX_TYPE_GROWTH:
				ofs << (cur_data - last_data);
				break;
			case COMPLEX_TYPE_SUM:
				ofs << cur_data;
				break;
			case COMPLEX_TYPE_SGR:
				ofs << (cur_data - last_data) / last_data;
				break;
			case COMPLEX_TYPE_GR:
				ofs << (cur_data - last_data) / cur_count;
				break;
			case COMPLEX_TYPE_AVG:
				ofs << cur_data / cur_count;
				break;
			}

			ofs << '\n';
		}
	}
	ofs.close();
    struct stat st;
    if (::stat(file_name.c_str(), &st) < 0)    // Skips error.
        return;
    if (st.st_size == 0)
        ::remove(file_name.c_str());
}

void cal_base::write_accu()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	gpenv& env_mgr = gpenv::instance();
	const input_record_t& input_record = SAP->_SAP_adaptors[sa_id].input_records[0];

	map<string, string>::const_iterator iter = adaptor.args.find("dst_dir");
	if (iter == adaptor.args.end())
		throw bad_msg(__FILE__, __LINE__, SGEINVAL, (_("ERROR: Can't find dst_dir in entry args.arg.name")).str(APPLOCALE));

	string dst_dir;
	env_mgr.expand_var(dst_dir, iter->second);
	if (*dst_dir.rbegin() != '/')
		dst_dir += '/';

	string file_name = (boost::format("%1%/ai_idx_mod_limit.seq.%|2$010|") %  dst_dir % SAT->_SAT_global[GLOBAL_FILE_SN_SERIAL]).str();
	ofstream ofs(file_name.c_str());
	if (!ofs)
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Can't open file {1} to write out accumulated data.") % file_name).str(APPLOCALE));

	for (map<cal_accu_key_t, cal_accu_value_t>::const_iterator iter = accu_map.begin(); iter != accu_map.end(); ++iter) {
		const cal_accu_key_t& key = iter->first;
		const cal_accu_value_t& value = iter->second;
		bool first = true;

		BOOST_FOREACH(const string& item, key) {
			ofs << item << input_record.delimiter;
		}

		BOOST_FOREACH(const double& item, value) {
			if (!first)
				ofs << input_record.delimiter;
			else
				first = false;

			ofs << item;
		}
		ofs << '\n';
	}
}


DECLARE_SA_BASE("CAL", cal_base)

}
}

