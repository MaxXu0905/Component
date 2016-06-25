#include "calt_ctx.h"
#include "cal_tree.h"
#include "cal_policy.h"
#include "cal_rule.h"

namespace ai
{
namespace app
{

using namespace ai::sg;

boost::thread_specific_ptr<calt_ctx> calt_ctx::instance_;

calt_ctx::calt_ctx()
{
	memset(_CALT_global_fields, '\0', sizeof(_CALT_global_fields));

	_CALT_global_pay_return = NULL;
	_CALT_global_busi_code = NULL;
	_CALT_global_busi_value = NULL;
	_CALT_global_is_accumulate = NULL;

	_CALT_db = NULL;

	for (int i = 0; i < CAL_TOTAL_MANAGER; i++)
		_CALT_mgr_array[i] = NULL;

	_CALT_gen_busi_idx = 0;
}

calt_ctx::~calt_ctx()
{
	delete _CALT_db;
	_CALT_db = NULL;

	for (int i = 0; i < CAL_TOTAL_MANAGER; i++) {
		if (_CALT_mgr_array[i] != NULL) {
			delete _CALT_mgr_array[i];
			_CALT_mgr_array[i] = NULL;
		}
	}
}

calt_ctx * calt_ctx::instance()
{
	calt_ctx *CALT = instance_.get();
	if (CALT == NULL) {
		CALT = new calt_ctx();
		instance_.reset(CALT);

		// 创建管理对象数组
		CALT->_CALT_mgr_array[CAL_TREE_MANAGER] = new cal_tree();
		CALT->_CALT_mgr_array[CAL_POLICY_MANAGER] = new cal_policy();
		CALT->_CALT_mgr_array[CAL_RULE_MANAGER] = new cal_rule();
	}
	return CALT;
}

}
}

