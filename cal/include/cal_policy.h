#if !defined(__CAL_POLICY_H__)
#define __CAL_POLICY_H__

#include "sa_internal.h"
#include "calp_ctx.h"
#include "calt_ctx.h"
#include "cal_struct.h"
#include "cal_manager.h"

namespace ai
{
namespace app
{

using namespace std;
using namespace ai::sg;

int const RANGE_TYPE_EXCLUDE = 0;
int const RANGE_TYPE_INCLUDE = 1;

struct cal_policy_area_t
{
	int mod_id;
	string city_code;

	bool operator<(const cal_policy_area_t& rhs) const;
};

struct cal_policy_detail_t
{
	cal_policy_detail_t();
	cal_policy_detail_t(const cal_policy_detail_t& rhs);
	~cal_policy_detail_t();

	string comm_range_id;
	int range_type;
	string comm_range_type;
	string comm_range_value;
	string pay_chnl_id;
	int field_count;	// 字段个数
	char *field_value;	// 经过加工后的字段值
	string field_name;	// 字段名称
	field_data_t *field_data;	// 参考值对象
	bool next;   //是否还有相同指标
};

int const POLICY_FLAG_MATCH_DRIVER = 0x1;	// 当前政策与驱动源一致
int const POLICY_FLAG_NULL_DRIVER = 0x2;	// 当前没有指定驱动源

struct cal_policy_t
{
	int flag;
	string mod_type;
	string comm_type;
	string dept_id;
	set<string> pay_chnl_id_set;
	time_t eff_date;
	time_t exp_date;
	// 过滤条件
	vector<cal_policy_detail_t> details;
	// 对应的规则数组
	vector<int> rules;
	bool valid;
	string policy_state;
	string access_flag;
	string is_limit_flag;
	string busi_type;

	cal_policy_t();
	~cal_policy_t();
};

class cal_policy : public cal_manager
{
public:
	typedef map<int, cal_policy_t>::const_iterator const_iterator;

	static cal_policy& instance(calt_ctx *CALT);

	// 加载政策相关的配置数据
	void load();
	const_iterator begin() const;
	const_iterator end() const;
	bool match(int mod_id, const cal_policy_t& policy);
	cal_policy_t& get_policy(int mod_id);

private:
	cal_policy();
	virtual ~cal_policy();

	void load_policy_area();
	void load_policy();
	void load_policy_detail();
	void load_rule();
	void add_item(int mod_id, cal_policy_detail_t& item, const vector<string>& value_vector);

	static bool is_integer(const string & str);
	static bool is_floating(const string & str);

	template<typename T>
	static bool search(const T& key, const void *base, size_t nmemb)
	{
#if defined(HPUX)
		return (::bsearch(&key, base, nmemb, sizeof(T), reinterpret_cast<int (*)(const void *,const void *)>(compare<T>)) != NULL);
#else
		return (::bsearch(&key, base, nmemb, sizeof(T), compare<T>) != NULL);
#endif
	}

	static bool search(const char *key, const void *base, size_t nmemb, size_t size)
	{
		return (::bsearch(key, base, nmemb, size, compare) != NULL);
	}

	template<typename T>
	static int compare(const void *left, const void *right)
	{
		const T& t_left = *reinterpret_cast<const T *>(left);
		const T& t_right = *reinterpret_cast<const T *>(right);

		if (t_left < t_right)
			return 1;
		else if (t_left > t_right)
			return -1;
		else
			return 0;
	}

	static int compare(const void *left, const void *right)
	{
		const char *t_left = reinterpret_cast<const char *>(left);
		const char *t_right = reinterpret_cast<const char *>(right);

		return ::strcmp(t_left, t_right);
	}

	friend class calt_ctx;
};

}
}

#endif

