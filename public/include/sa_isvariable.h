#if !defined(__SA_ISVARIABLE_H__)
#define __SA_ISVARIABLE_H__

#include "sa_isocket.h"

namespace ai
{
namespace app
{

class sa_isvariable : public sa_isocket
{
public:
	sa_isvariable(sa_base& _sa, int _flags);
	~sa_isvariable();

private:
	bool to_fields(const input_record_t& input_record);

	char escape;
};

}
}

#endif

