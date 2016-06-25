#if !defined(__DUP_BASE_H__)
#define __DUP_BASE_H__

#include "sa_internal.h"
#include "dup_ctx.h"

namespace ai
{
namespace app
{

using namespace ai::sg;

class dup_base : public sa_base
{
public:
	dup_base(int sa_id_);
	virtual ~dup_base();

private:
	virtual bool support_batch() const;
	virtual bool support_concurrency() const;
	virtual bool transfer_input() const;
	virtual void pre_init();
	virtual void post_init();
	virtual void post_init2();
	virtual void set_svcname(int input_idx);
	virtual void set_input2(int input_idx);
	virtual void pre_rollback();
	void set_rsvcname(int partition_id);

	dup_ctx *DUP;
	int dup_idx;
	compiler::func_type dup_func;
};

}
}

#endif

