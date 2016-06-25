#if !defined(__SA_OUTPUT_H__)
#define __SA_OUTPUT_H__

#include "sa_manager.h"

namespace ai
{
namespace app
{

class sa_output : public sa_manager
{
public:
	sa_output(sa_base& _sa, int _flags);
	virtual ~sa_output();

	virtual void init();
	virtual void init2();
	virtual void open();
	virtual int write(int input_idx);
	virtual int write(std::string& input_str, std::string& err_str);
	virtual void flush(bool completed);
	virtual void close();
	virtual void complete();
	virtual void clean();
	virtual void recover();
	virtual void dump(std::ostream& os);
	virtual void rollback(const std::string& raw_file_name, int file_sn);
	virtual void global_rollback();

protected:
	sa_base& sa;
	int flags;
	const sa_adaptor_t& adaptor;
	bool is_open;
};

struct sa_output_creator_t
{
	int flags;
	std::string class_name;
	sa_output * (*create)(sa_base& _sa, int _flags);
};

#define DECLARE_SA_OUTPUT(_flags, _class_name) \
namespace \
{ \
\
class sa_output_initializer \
{ \
public: \
	sa_output_initializer() { \
		sap_ctx *SAP = sap_ctx::instance(); \
		sa_output_creator_t item; \
\
		BOOST_FOREACH(const sa_output_creator_t& v, SAP->_SAP_output_creator) { \
			if (v.class_name == #_class_name) { \
				gpp_ctx *GPP = gpp_ctx::instance(); \
				GPP->write_log(__FILE__, __LINE__, (_("ERROR: Duplicate sa_output creator for {1}") % #_class_name).str(APPLOCALE)); \
				exit(1); \
			} \
		} \
\
		item.flags = _flags; \
		item.class_name = #_class_name; \
		item.create = &sa_output_initializer::create; \
		SAP->_SAP_output_creator.push_back(item); \
	} \
\
private: \
	static sa_output * create(sa_base& sa, int flags) { \
		return new _class_name(sa, flags); \
	} \
} initializer; \
\
}

}
}

#endif

