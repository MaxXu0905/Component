#if !defined(__DUP_SVC_H__)
#define __DUP_SVC_H__

#include "sg_public.h"
#include "dup_ctx.h"
#include "svc_base.h"

using namespace ai::sg;

namespace ai
{
namespace app
{

class dup_svc : public svc_base
{
public:
	dup_svc();
	~dup_svc();

private:
	void pre_extract_input();
	void post_extract_input();
	bool handle_meta();
	bool handle_record(int output_idx);

	int check_data(sqlite3_stmt *stmt, const char *id, const char *key);
	int check_cross_data(sqlite3_stmt *stmt, time_t begin_time, time_t end_time, const char *id, const char *key);
	bool create_table(const string& table_name);
	void get_table(string &title);

	dup_ctx *DUP;
};

}
}

#endif

