#if !defined(__SA_INTEGRATOR_H__)
#define __SA_INTEGRATOR_H__

#include "sa_struct.h"
#include "sa_manager.h"

namespace ai
{
namespace app
{

using namespace ai::sg;
using namespace ai::scci;

class sa_integrator : public sa_manager
{
public:
	sa_integrator();
	~sa_integrator();

	void run(int flags);

	static bool parse(sg_init_t& init_info, const string& sginfo);

private:
	bool create_adaptors();
	bool parse(string& usrname, string& passwd);
};

}
}

#endif

