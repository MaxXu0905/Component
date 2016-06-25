#if !defined(__SA_INPUT_H__)
#define __SA_INPUT_H__

#include "sa_manager.h"

namespace ai
{
namespace app
{

int const DFLT_SLEEP_TIME = 60;

class sa_input : public sa_manager
{
public:
	sa_input(sa_base& _sa, int _flags);
	virtual ~sa_input();

	virtual void init();
	virtual void init2();
	virtual bool open();
	virtual void flush(bool completed);
	virtual void close();
	virtual void complete();
	virtual void clean();
	virtual void recover();
	virtual bool validate();
	virtual int read();
	virtual void rollback(const std::string& raw_file_name, int file_sn);
	virtual void iclean();
	virtual void global_rollback();

protected:
	sa_base& sa;
	int flags;
	const sa_adaptor_t& adaptor;
	master_dblog_t& master;
};

struct sa_input_creator_t
{
	int flags;
	std::string class_name;
	sa_input * (*create)(sa_base& _sa, int _flags);
};

#define DECLARE_SA_INPUT(_flags, _class_name) \
namespace \
{ \
\
class sa_input_initializer \
{ \
public: \
	sa_input_initializer() { \
		sap_ctx *SAP = sap_ctx::instance(); \
		sa_input_creator_t item; \
\
		BOOST_FOREACH(const sa_input_creator_t& v, SAP->_SAP_input_creator) { \
			if (v.class_name == #_class_name) { \
				gpp_ctx *GPP = gpp_ctx::instance(); \
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: Duplicate sa_input creator for {1}") % #_class_name).str(APPLOCALE)); \
				exit(1); \
			} \
		} \
\
		item.flags = _flags; \
		item.class_name = #_class_name; \
		item.create = &sa_input_initializer::create; \
		SAP->_SAP_input_creator.push_back(item); \
	} \
\
private: \
	static sa_input * create(sa_base& sa, int flags) { \
		return new _class_name(sa, flags); \
	} \
} initializer; \
\
}

}
}

#endif

