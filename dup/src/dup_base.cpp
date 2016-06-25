#include "dup_base.h"

namespace ai
{
namespace app
{

int const MIN_ROLLBACK_TIMEOUT = 600;

using namespace ai::sg;
using namespace ai::scci;
typedef boost::tokenizer<boost::char_separator<char> > tokenizer;

dup_base::dup_base(int sa_id_)
	: sa_base(sa_id_)
{
	DUP = dup_ctx::instance();
	DUP->_DUP_svc_key = adaptor.parms.svc_key;

	if (!DUP->get_config())
		throw bad_msg(__FILE__, __LINE__, SGESYSTEM, (_("ERROR: get DUP config failed, svc_key={1}, version={2}") % adaptor.parms.svc_key % adaptor.parms.version).str(APPLOCALE));
}

dup_base::~dup_base()
{
}

bool dup_base::support_batch() const
{
	bool retval = false;
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", &retval);
#endif

	return retval;
}

bool dup_base::support_concurrency() const
{
	bool retval = true;
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", &retval);
#endif

	return retval;
}

bool dup_base::transfer_input() const
{
	return true;
}

void dup_base::pre_init()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	if (adaptor.input_maps.size() != 1)
		throw bad_msg(__FILE__, __LINE__, SGEINVAL, (_("ERROR: input should be defined once and only once.")).str(APPLOCALE));

	// 查重模块不需要全局变量的输入和输出
	adaptor.input_globals.clear();
	adaptor.output_globals.clear();
}

void dup_base::post_init()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	if (!DUP->_DUP_enable_cross) {
		input2_size = 3;
		inputs2 = new char **[per_call];
		for (int i = 0; i < per_call; i++) {
			inputs2[i] = new char *[input2_size];
			inputs2[i][0] = new char[DUP_TABLE_SIZE];
			inputs2[i][1] = new char[DUP_ID_SIZE];
			inputs2[i][2] = new char[DUP_KEY_SIZE];
		}
	} else {
		input2_size = 5;
		inputs2 = new char **[per_call];
		for (int i = 0; i < per_call; i++) {
			inputs2[i] = new char *[input2_size];
			inputs2[i][0] = new char[DUP_TABLE_SIZE];
			inputs2[i][1] = new char[DUP_ID_SIZE];
			inputs2[i][2] = new char[DUP_KEY_SIZE];
			inputs2[i][3] = new char[DUP_TIME_SIZE];
			inputs2[i][4] = new char[DUP_TIME_SIZE];
		}
	}

	for (int i = 0; i < per_call; i++) {
		for (int j = 0; j < input2_size; j++)
			inputs2[i][j][0] = '\0';
	}

	compiler& cmpl = SAP->_SAP_cmpl;

	map<string, string>::const_iterator iter = adaptor.args.find("rule_dup");
	if (iter == adaptor.args.end())
		throw bad_msg(__FILE__, __LINE__, SGEINVAL, (_("ERROR: Can't find rule_dup in entry args.arg.name")).str(APPLOCALE));

	try {
		field_map_t output_map;

		DUP->set_map(output_map);
		dup_idx = cmpl.create_function(iter->second, adaptor.global_map, adaptor.input_maps[0], output_map);
	} catch (exception& ex) {
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Can't create function for rule_dup, {1}") % ex.what()).str(APPLOCALE));
	}
}

void dup_base::post_init2()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif
	compiler& cmpl = SAP->_SAP_cmpl;

	dup_func = cmpl.get_function(dup_idx);
}

void dup_base::set_svcname(int input_idx)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, (_("input_idx={1}") % input_idx).str(APPLOCALE), NULL);
#endif
	char *dup_key = inputs2[input_idx][DUP_KEY_IDX];
	int len = strlen(dup_key);

	int partition_id = static_cast<int>(boost::hash_range(dup_key, dup_key + len) % DUP->_DUP_all_partitions);
	DUP->set_svcname(partition_id, svc_name);
}

void dup_base::set_input2(int input_idx)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, (_("input_idx={1}") % input_idx).str(APPLOCALE), NULL);
#endif
	const char **input = const_cast<const char **>(inputs[input_idx]);

	(void)(*dup_func)(NULL, SAT->_SAT_global, input, inputs2[input_idx]);
}

void dup_base::pre_rollback()
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, "", NULL);
#endif

	int partition_id = 0;
	int nodes = DUP->_DUP_all_partitions / DUP->_DUP_partitions;
	string tables;

	map<string, string>::const_iterator iter = adaptor.args.find("tables");
	if (iter == adaptor.args.end()) {
		tables = "dup_";
		tables += DUP->_DUP_svc_key;
		tables += '\0';
	} else {
		boost::char_separator<char> sep(" \t\b");
		string tables_str = iter->second;
		tokenizer tokens(tables_str, sep);
		for (tokenizer::iterator iter = tokens.begin(); iter != tokens.end(); ++iter) {
			tables += *iter;
			tables += '\0';
		}
	}

	vector<int> rhandles;
	vector<string> svc_names;
	message_pointer rqst_msg = sg_message::create();
	message_pointer rply_msg = sg_message::create();

	int datalen = sizeof(sa_rqst_t) + tables.length();
	rqst_msg->set_length(datalen);
	rqst = reinterpret_cast<sa_rqst_t *>(rqst_msg->data());
	strcpy(rqst->svc_key, adaptor.parms.svc_key.c_str());
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
	for (int i = 0; i < DUP->_DUP_partitions; i++) {
		for (int j = 0; j < nodes; j++) {
			partition_id = j * DUP->_DUP_partitions + i;
			set_rsvcname(partition_id);

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
		return;

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

void dup_base::set_rsvcname(int partition_id)
{
#if defined(DEBUG)
	scoped_debug<bool> debug(500, __PRETTY_FUNCTION__, (_("partition_id={1}") % partition_id).str(APPLOCALE), NULL);
#endif

	svc_name = "DUP_";
	svc_name += DUP->_DUP_svc_key;
	svc_name += "_";
	svc_name += boost::lexical_cast<string>(partition_id);
}

DECLARE_SA_BASE("DUP", dup_base)

}
}

