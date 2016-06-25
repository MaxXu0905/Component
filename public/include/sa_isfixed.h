#if !defined(__SA_ISFIXED_H__)
#define __SA_ISFIXED_H__

#include "sa_isocket.h"

namespace ai
{
namespace app
{

class sa_isfixed : public sa_isocket
{
public:
	sa_isfixed(sa_base& _sa, int _flags);
	~sa_isfixed();

protected:
	virtual bool to_fields(const input_record_t& input_record);
};

}
}

#endif

