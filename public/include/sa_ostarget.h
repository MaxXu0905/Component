#if !defined(__SA_OSTARGET_H__)
#define __SA_OSTARGET_H__

#include "sa_output.h"

namespace ai
{
namespace app
{

using namespace std;
using namespace ai::sg;

/* 说明
 * 该类负责把输出记录输出到网络中
 */
class sa_ostarget : public sa_output
{
public:
	sa_ostarget(sa_base& _sa, int _flags);
	~sa_ostarget();

	void open();
	int write(int input_idx);
	void flush(bool completed);
	void close();
	void clean();
	void recover();

private:
	sa_isocket *isrc_mgr;
};

}
}

#endif


