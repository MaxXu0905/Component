#include <iostream>
#include "search.h"
#include "common.h"
#include "datastruct_structs.h"
#include "user_exception.h"
#include "perf_stat.h"
#include "dstream.h"
#include "sat_ctx.h"
#include <boost/lexical_cast.hpp>

using namespace ai::sg;
using namespace std;
using namespace ai::app;

// module_id = '13', so add 13 * 10,000,000
int const ERROR_OFFSET = 130000000;
int const MAX_BPS_RATE_CODE = 32;
int const MAX_BPS_FAV_DATE = 32;
int const MAX_BPS_FAV_TIME = 32;

enum rate_mode
{
	mode_duration = '0', // 按时长计费
	mode_flux, // 按流量计费
	mode_times // 按次数计费
};

extern "C"
{

void rtrim_char(char *number, char ch)
{
	int i;
	int len = strlen(number);
	for (i = len - 1; i >= 0; i--)
	{
		if (number[i] != ch)
			break;
	}
	number[i + 1] = 0;
}

void ltrim_char(char* src, char ch)
{
	int i ;
	int len = strlen(src) ;
	for (i = 0; i < len; i++) {
		if (src[i] != ch)
			break;
	}
	memmove(src, src + i, len - i + 1) ;
}

void ip_trans2hex(const char* dec_ip, char* hex_ip)
{
	char tmp[3];
	string num = "";
	ostringstream fmt;
	fmt.str("");
	hex_ip[0] = 0;

	for (int i = 0; i < (int)strlen(dec_ip); i++)
	{
		if (dec_ip[i] != '.')
    {
        fmt << dec_ip[i];
    }
    else
    {
    	num = fmt.str();
    	sprintf(tmp, "%02X", ::atoi(num.c_str()));
        strcat(hex_ip, tmp);
        num = "";
        fmt.str("");
        tmp[0] = 0;
    }
	}

	num = fmt.str();
	sprintf(tmp, "%02X", ::atoi(num.c_str()));
	strcat(hex_ip, tmp);
}

time_t get_time(const char* date, const char* time)
{
	string dt_str = date;
	dt_str += time;
	try
	{
		datetime dt(dt_str);
		return dt.duration();
	}
	catch (...)
	{
		return -1;
	}
}

int check_datetime(const char* datetime)
{
	char buff[5];

	if (strlen(datetime) != 14)
		return -1;

	memcpy(buff, datetime, 4);
	buff[4] = '\0';
	int year = atoi(buff);

	memcpy(buff, datetime + 4, 2);
	buff[2] = '\0';
	int month = atoi(buff);

	memcpy(buff, datetime + 6, 2);
	buff[2] = '\0';
	int day = atoi(buff);

	try {
		calendar::check_date(year, month, day);
	} catch (bad_datetime& ex) {
		return -1;
	}

	memcpy(buff, datetime + 8, 2);
	buff[2] = '\0';
	int hour = atoi(buff);

	memcpy(buff, datetime + 10, 2);
	buff[2] = '\0';
	int minute = atoi(buff);

	memcpy(buff, datetime + 12, 2);
	buff[2] = '\0';
	int second = atoi(buff);

	try {
		calendar::check_time(hour, minute, second);
	} catch (bad_datetime& ex) {
		return -1;
	}

	return 0;
}

int is_number(const char* number)
{
	for (int i = 0; number[i] != '\0'; i++) {
		if (number[i] > '9' || number[i] < '0')
			return -1;
	}

	return 0;
}

int is_integer(const char* number)
{
	for (int i = 0; number[i] != '\0'; i++) {
		if (i == 0 && (number[0] == '+' || number[0] == '-'))
			continue;

		if (number[i] > '9' || number[i] < '0')
			return -1;
	}

	return 0;
}

int is_double(const char* number)
{
	bool has_dot = false;

	for (int i = 0; number[i] != '\0'; i++) {
		if (i == 0 && (number[0] == '+' || number[0] == '-'))
			continue;

		if (number[i] == '.') {
			if (has_dot)
				return -1;

			has_dot = true;
			continue;
		}

		if (number[i] > '9' || number[i] < '0')
			return -1;
	}

	return 0;
}

void trim_alpha(char* number)
{
	char* p;
	char* first;
	char* last;
	for (p = number; *p != '\0'; p++)
	{
		if (::isdigit(*p))
			break;
	}
	first = p;
	last = p;
	for (; *p != '\0'; p++)
	{
		if (isdigit(*p))
			last = p + 1;
	}
	*last = '\0';
	if (first != number)
		memcpy(number, first, last - first + 1);
}

void trim_illegal_char(char* src)
{
	char dst[64];
	char* sp = src;
	char* dp = dst;

	while (*sp != 0)
	{
		if (*(unsigned char*)sp >= 0x80)
		{
			sp += 2;
		}
		else
		{
			*dp = *sp;
			sp++;
			dp++;
		}
	}
	dp = 0;
	strcpy(src, dst);
}

void str_add(const char *need_process, const char *add_ch, char *processed ,const char *delimiter,const int flag)
{
	char head[10+1];
	char delimit_tmp[1+1];
	string character;
	string temp_out;
	string temp_out1;
	string all_end_out;
	string end_out;
	const char *p1,*p2;
	int len;

	len =0;
	memset(head,0x00,sizeof(head));
	temp_out.clear();
	temp_out1.clear();
	character.clear();
	all_end_out.clear();
	end_out.clear();
	memset(delimit_tmp,0x00,sizeof(delimit_tmp));

	strcpy(head,add_ch);
	character = need_process;
	strcpy(delimit_tmp,delimiter);

	p1 = character.c_str();
	while(1)
	{
		if ((p2=(strstr(p1,delimiter))) != NULL)   /*point to frist delimiter*/
		{
			len = p2 - p1  ;
			temp_out.clear();
			temp_out1.clear();
			temp_out.append(string(p1),0,len);
			if(!flag)
			{
				temp_out1+=string(head);
				temp_out1+=string(temp_out);
			}
			else
			{
				temp_out1+=string(temp_out);
				temp_out1+=string(head);
			}
			temp_out1+=string(delimit_tmp);
			end_out+=temp_out1;
			p1 = p2 +1;
		}
	 else
	 {
			if(!flag)
			{
				all_end_out+=end_out;
				all_end_out+=head;
				all_end_out+=p1;
			}
			else
			{
				all_end_out+=end_out;
				all_end_out+=p1;
				all_end_out+=head;
			}
			break;
		}
	}
		strcpy(processed,all_end_out.c_str());
}

void str_cut(const char *need_process, const int number, char *processed,const char *delimiter, const int flag)
{
	char delimit_tmp[1+1];
	string character;
	string temp_out;
	string temp_out1;
	string end_out;
	string all_end_out;
	string medial_out;
	const char *p1,*p2;
	int num;
	int len;

	num = 0;
	len = 0;
	character.clear();
	temp_out.clear();
	temp_out1.clear();
	end_out.clear();
	all_end_out.clear();
	medial_out.clear();
	memset(delimit_tmp,0x00,sizeof(delimit_tmp));

	num = number;
	character = need_process;
	strcpy(delimit_tmp,delimiter);

	p1 = character.c_str();
	while(1)
	{
		if ((p2=(strstr(p1,delimiter))) != NULL)  /*point to frist delimiter*/
		{
			len = p2 - p1  ;
			temp_out.clear();
			temp_out1.clear();
			temp_out.append(string(p1),0,len);
			if(!flag)
			{
				temp_out1.append(temp_out,num,len-num);
			}
			else
			{
				temp_out1.append(temp_out,0,len-num);
			}
			temp_out1+=delimit_tmp;
			end_out+=temp_out1;
			p1 = p2 +1;
		}
		else
		{
			if(!flag)
			{
				all_end_out+=end_out;
				all_end_out.append(string(p1),num,strlen(p1)-num);
			}
			else
			{
				medial_out.append(string(p1),0,strlen(p1)-num);
				all_end_out+=end_out;
				all_end_out+=medial_out;
			}
			break;
		}
	}
	strcpy(processed,all_end_out.c_str());
}

void str_chg_delimit(const char *need_process, const char *old_delimiter, const char *instread_delimiter, char *processed)
{
	string char_in;
	string end_out;
	string all_end_out;
	string temp_out;
	string temp_out1;
	char delimit_symbol[1+1];
	char delimit_now[1+1];
	const char *p1,*p2;
	int len;

	char_in.clear();
	end_out.clear();
	all_end_out.clear();
	temp_out.clear();
	temp_out1.clear();
	memset(delimit_symbol,0x00,sizeof(delimit_symbol));
	memset(delimit_now,0x00,sizeof(delimit_now));

	char_in =  need_process;
	strcpy(delimit_now,old_delimiter);
	strcpy(delimit_symbol,instread_delimiter);

	//strlen(char_in);
	p1 = char_in.c_str();

	while(1)
	{
		if ((p2=(strstr(p1,delimit_now))) != NULL)
		{
			len = p2 - p1  ;
			temp_out.clear();
			temp_out1.clear();
			temp_out.append(string(p1),0,len);
			temp_out1=temp_out;
			temp_out1+=delimit_symbol;
			end_out+=temp_out1;
			p1 = p2 +1;
		}
		else
		{
			all_end_out+=end_out;
			all_end_out+=p1;
			break;
		}
	}
	strcpy(processed,all_end_out.c_str());
}

void str_sum(const char *process_ch, const char *delimiter, char *processed)
{
	string char_in;
	string temp_out;
	char delimit_now[1+1];
	const char *p1,*p2;
	long len;
	long sum_of_number = 0;

	char_in.clear();
	temp_out.clear();
	memset(delimit_now,0x00,sizeof(delimit_now));

	char_in =	process_ch;
	strcpy(delimit_now,delimiter);

	p1 = char_in.c_str();

	while(1)
	{
		if ((p2=(strstr(p1,delimit_now))) != NULL)
		{
			len = p2 - p1  ;
			temp_out.clear();
			temp_out.append(string(p1),0,len);
			sum_of_number+=atol(temp_out.c_str());
			p1 = p2 +1;

		}
		else
		{
			sum_of_number+=atol(p1);
			break;
		}
	}
	sprintf(processed,"%ld",sum_of_number);
}

void str_column_sum(const char *process_ch1,const char *process_ch2, const char *delimiter,char *processed)
{
	vector<string> v1;
	vector<string> v2;
	char buf[32];
	string result;
	int i;

	common::string_to_array(process_ch1, *delimiter, v1);
	common::string_to_array(process_ch2, *delimiter, v2);

	int min_size = std::min(v1.size(), v2.size());
	for (i = 0; i < min_size; i++) {
		sprintf(buf, "%d", atoi(v1[i].c_str()) + atoi(v2[i].c_str()));
		if (!result.empty())
			result += *delimiter;

		result += buf;
	}

	if (min_size < (int)v1.size()) {
		if (!result.empty())
			result += *delimiter;

		result += v1[i];
	} else if (min_size < (int)v2.size()) {
		if (!result.empty())
			result += *delimiter;

		result += v2[i];
	}

	memcpy(processed, result.c_str(), result.length());
	processed[result.length()] = '\0';
}

int get_duration_datetime(const char* duration, char* datetime)
{
	long time;
	struct tm tm;

	if (is_number(duration) != 0)
		return -1;

	time = atol(duration);
	localtime_r(&time, &tm);
	strftime(datetime, 15, "%Y%m%d%H%M%S", &tm);

	return 0;
}

void exchange_char(char *strp, char csource, char ctarget)
{
	int istrleng = 0;
	int icount = 0;
	istrleng = strlen(strp);
	for(; icount < istrleng; icount++ )
	{
		if (*strp == csource)
			*strp = ctarget;
		strp++;
	}
	*strp = '\0';
}

time_t ai_app_string_to_time_t(const char *data)
{
	string str = data;

	if (str.length() == 8) {
		date d(str);
		return d.duration();
	} else if (str.length() == 14) {
		datetime dt(str);
		return dt.duration();
	} else {
		throw bad_msg(__FILE__, __LINE__, 0, (_("ERROR: Unknown format for {1}") % data).str(APPLOCALE));
	}
}

const char * ai_app_char_to_string(char value)
{
	sat_ctx *SAT = sat_ctx::instance();

	SAT->_SAT_search_str = boost::lexical_cast<string>(value);
	return SAT->_SAT_search_str.c_str();
}

const char * ai_app_uchar_to_string(unsigned char value)
{
	sat_ctx *SAT = sat_ctx::instance();

	SAT->_SAT_search_str = boost::lexical_cast<string>(value);
	return SAT->_SAT_search_str.c_str();
}

const char * ai_app_short_to_string(short value)
{
	sat_ctx *SAT = sat_ctx::instance();

	SAT->_SAT_search_str = boost::lexical_cast<string>(value);
	return SAT->_SAT_search_str.c_str();
}

const char * ai_app_ushort_to_string(unsigned short value)
{
	sat_ctx *SAT = sat_ctx::instance();

	SAT->_SAT_search_str = boost::lexical_cast<string>(value);
	return SAT->_SAT_search_str.c_str();
}

const char * ai_app_int_to_string(int value)
{
	sat_ctx *SAT = sat_ctx::instance();

	SAT->_SAT_search_str = boost::lexical_cast<string>(value);
	return SAT->_SAT_search_str.c_str();
}

const char * ai_app_uint_to_string(unsigned int value)
{
	sat_ctx *SAT = sat_ctx::instance();

	SAT->_SAT_search_str = boost::lexical_cast<string>(value);
	return SAT->_SAT_search_str.c_str();
}

const char * ai_app_long_to_string(long value)
{
	sat_ctx *SAT = sat_ctx::instance();

	SAT->_SAT_search_str = boost::lexical_cast<string>(value);
	return SAT->_SAT_search_str.c_str();
}

const char * ai_app_ulong_to_string(unsigned long value)
{
	sat_ctx *SAT = sat_ctx::instance();

	SAT->_SAT_search_str = boost::lexical_cast<string>(value);
	return SAT->_SAT_search_str.c_str();
}

const char * ai_app_float_to_string(float value)
{
	sat_ctx *SAT = sat_ctx::instance();

	SAT->_SAT_search_str = boost::lexical_cast<string>(value);
	return SAT->_SAT_search_str.c_str();
}

const char * ai_app_double_to_string(double value)
{
	sat_ctx *SAT = sat_ctx::instance();

	SAT->_SAT_search_str = boost::lexical_cast<string>(value);
	return SAT->_SAT_search_str.c_str();
}

const char * ai_app_time_t_to_string(time_t value)
{
	sat_ctx *SAT = sat_ctx::instance();
	datetime dt(value);

	dt.iso_string(SAT->_SAT_search_str);
	return SAT->_SAT_search_str.c_str();
}

#if defined(__BPS_SEARCH__)

int get_service_code(void* client, const char* ss_code, const char* bearer_service,
		const char* tele_service, time_t eff_date, char* service_code)
{
	int ret = get_bds_service_code(client, '2', ss_code, eff_date, service_code);
	if (ret == 0)
		return ret;

	ret = get_bds_service_code(client, '1', bearer_service, eff_date, service_code);
	if (ret == 0)
		return ret;

	return get_bds_service_code(client, '0', tele_service, eff_date, service_code);
}

void standard_number(void* client, const char* file_type, time_t eff_date, char* number)
{
	char old_number_prefix[128];
	char new_number_prefix[128];
	int max_keep_length;
	short old_prefix_len;
	short new_prefix_len;

	int ret = get_bds_standard_number(client, file_type, number, strlen(number), eff_date, new_number_prefix,
			&max_keep_length, &old_prefix_len, &new_prefix_len);
	if (ret == 0) {
		strcpy(old_number_prefix, number);
		strcpy(number, new_number_prefix);
		memcpy(number + new_prefix_len, old_number_prefix + old_prefix_len, max_keep_length - new_prefix_len);
		number[max_keep_length] = '\0';
	}
}

int get_tele_info(void* client, const char* visit_area_code, char* number, time_t eff_date,
		char* area_code, char* local_net, char* busi_district, char* dealer_code, char* user_type,
		char* service_class)
{
	short area_code_len;
	char lower_tele_prefix[128];

	if (number[0] != '0') // Having no area code, so add with visit area code.
		sprintf(lower_tele_prefix, "%s%s", visit_area_code, number);
	else
		strcpy(lower_tele_prefix, number);

	int ret = get_bds_tele_prefix(client, lower_tele_prefix, eff_date, area_code, local_net, busi_district,
			dealer_code, user_type, service_class, &area_code_len);

	if (ret == 0) {
		if (number[0] == '0') // Having area code, so remove it.
			strcpy(number, number + area_code_len);
	}

	return ret;
}

int get_region_code(void* client, const char* area_code, const char* local_net,
		const char* visit_area_code, const char* visit_local_net, time_t eff_date,
		char* region_code)
{

	int ret = get_bds_region_code(client, area_code, local_net, visit_area_code, visit_local_net, eff_date, region_code);
	if (ret == 0)
		return ret;

	return get_bds_area_code(client, area_code, eff_date, region_code);
}

int get_voice_rating(void* client, const char* visit_local_net, const char* self_local_net, const char* self_service_class,
		const char* self_region_code, const char* self_user_type, const char* self_trunk_type, const char* org_trm_id,
		const char* opp_dealer_code, const char* opp_service_class, const char* opp_region_code,
		const char* opp_number_type, int number_type_flag, const char* same_city_flag,
		const char* service_code, const char* net_type, const char* call_type, int fee_item,
		char* item_fee_code, char* rate_dinner, int* base_flag, int* land_flag, int* other_flag, int* rebill_flag)
{
 	dout << "get_voice_rating:\n";
	dout << "number_type_flag = [" << number_type_flag << "]\n";
	dout << std::flush;

	if (rebill_flag)
		*rebill_flag = 0;

	if (fee_item != 3 && (number_type_flag <= 0 || number_type_flag >= 3)) // 不需要批价
	{
		if (item_fee_code)
			strcpy(item_fee_code, "");
		if (rate_dinner)
			strcpy(rate_dinner, "");

		return 0;
	}

	char opp_number_type_tmp[32];
	int base_flag_tmp;
	int land_flag_tmp;
	int other_flag_tmp;

	if (fee_item != 3 && number_type_flag == 2) // Use normal number_type.
		strcpy(opp_number_type_tmp, "000");
	else
		strcpy(opp_number_type_tmp, opp_number_type);

	int ret = get_bps_voice_rule(client, visit_local_net, self_local_net, self_service_class,
			self_region_code, self_user_type, self_trunk_type, org_trm_id[0], opp_dealer_code, opp_service_class,
			opp_region_code, opp_number_type_tmp, same_city_flag[0], service_code, net_type, call_type,
			fee_item, item_fee_code, rate_dinner, &base_flag_tmp, &land_flag_tmp, &other_flag_tmp, rebill_flag);

	if (ret == 0) {
		if (fee_item == 3) {
			if (base_flag)
				*base_flag = base_flag_tmp;
			if (land_flag)
				*land_flag = land_flag_tmp;
			if (other_flag)
				*other_flag = other_flag_tmp;
		}
	} else {
		if (item_fee_code)
			strcpy(item_fee_code, "");
		if (rate_dinner)
			strcpy(rate_dinner, "");
		if (fee_item == 3) {
			if (base_flag)
				*base_flag = 1;
			if (land_flag)
				*land_flag = 1;
			if (other_flag)
				*other_flag = 1;
		}
	}

	return 0;
}

static void get_voice_fee(void* client, const t_bps_rate_dinner& rate, const datetime& dt_,
		long duration, char* fee, char* fav_fee, char* bill_duration, char* times)
{
	perf_stat stat(__FUNCTION__);
	billshmclient* shm_client = reinterpret_cast<billshmclient*>(client);
 	int loop;

	dout << "In get_voice_fee, rate_dinner = [" << rate.rate_dinner << "]\n";
	dout << "rate_code = [" << rate.rate_code << "]\n";
	dout << "fav_code = [" << rate.fav_code << "]\n";
	dout << "precision = [" << rate.precision << "]\n";
	dout << "round_mode = [" << rate.round_mode << "]\n";
	dout << "rate_mode = [" << rate.rate_mode << "]\n";

	datetime dt(dt_); // Start time.
	datetime end_dt = dt; // End time.
	end_dt += duration;

	// Get rate code array.
	t_bps_rate_code rate_key;
	t_bps_rate_code rate_result[::MAX_BPS_RATE_CODE];
	int rate_count;
	strcpy(rate_key.rate_code, rate.rate_code);
	rate_key.begin_weekday = dt.week();
	rate_key.end_weekday = end_dt.week();
	rate_key.begin_time = dt.time();
	rate_key.end_time = end_dt.time();
	rate_key.eff_date = dt.duration();
	rate_key.exp_date = end_dt.duration();
	rate_count = shm_client->search(T_BPS_RATE_CODE, static_cast<void*>(&rate_key), rate_result, ::MAX_BPS_RATE_CODE);
	if (rate_count > ::MAX_BPS_RATE_CODE || rate_count < 1)
		throw T_BPS_RATE_CODE * 1000 + ERROR_OFFSET;

 	dout << "On search rate code, count = " << rate_count << "\n";
	for (loop = 0; loop < rate_count; loop++)
	{
		dout << "Index = " << loop << ":\n";
		dout << "rate_code = [" << rate_result[loop].rate_code << "]\n";
		dout << "begin_weekday = " << rate_result[loop].begin_weekday << "\n";
		dout << "end_weekday = " << rate_result[loop].end_weekday << "\n";
		dout << "begin_time = " << rate_result[loop].begin_time << "\n";
		dout << "end_time = " << rate_result[loop].end_time << "\n";
		dout << "lower_limit = " << rate_result[loop].lower_limit << "\n";
		dout << "upper_limit = " << rate_result[loop].upper_limit << "\n";
		dout << "unit_value = " << rate_result[loop].unit_value << "\n";
		dout << "unit_price = " << rate_result[loop].unit_price << "\n";
		dout << "eff_date = " << rate_result[loop].eff_date << "\n";
		dout << "exp_date = " << rate_result[loop].exp_date << "\n";
	}

	// Get fav date array.
	t_bps_rate_fav_date date_key;
	t_bps_rate_fav_date date_result[::MAX_BPS_FAV_DATE];
	int date_count;
	strcpy(date_key.fav_code, rate.fav_code);
	date_key.begin_date = dt.date();
	date_key.end_date = end_dt.date();
	date_key.begin_time = dt.time();
	date_key.end_time = end_dt.time();
	date_count = shm_client->search(T_BPS_RATE_FAV_DATE, static_cast<void*>(&date_key), date_result, ::MAX_BPS_FAV_DATE);
	if (date_count > ::MAX_BPS_FAV_DATE || date_count < 0)
		throw T_BPS_RATE_FAV_DATE * 1000 + ERROR_OFFSET;

 	dout << "On search rate fav date, count = " << date_count << "\n";
	for (loop = 0; loop < date_count; loop++)
	{
		dout << "Index = " << loop << ":\n";
		dout << "fav_code = [" << date_result[loop].fav_code << "]\n";
		dout << "begin_date = " << date_result[loop].begin_date << "\n";
		dout << "end_date = " << date_result[loop].end_date << "\n";
		dout << "begin_time = " << date_result[loop].begin_time << "\n";
		dout << "end_time = " << date_result[loop].end_time << "\n";
		dout << "dfav = " << date_result[loop].dfav << "\n";
	}

	// Get fav time array.
	t_bps_rate_fav_time time_key;
	t_bps_rate_fav_time time_result[::MAX_BPS_FAV_TIME];
	int time_count;
	strcpy(time_key.fav_code, rate.fav_code);
	time_key.begin_weekday = dt.week();
	time_key.end_weekday = end_dt.week();
	time_key.begin_time = dt.time();
	time_key.end_time = end_dt.time();
	time_key.eff_date = dt.duration();
	time_key.exp_date = end_dt.duration();
	time_count = shm_client->search(T_BPS_RATE_FAV_TIME, static_cast<void*>(&time_key), time_result, ::MAX_BPS_FAV_TIME);
	if (time_count > ::MAX_BPS_FAV_TIME || time_count < 0)
		throw T_BPS_RATE_FAV_TIME * 1000 + ERROR_OFFSET;

 	dout << "On search rate fav time, count = " << time_count << "\n";
	for (loop = 0; loop < time_count; loop++)
	{
		dout << "Index = " << loop << ":\n";
		dout << "fav_code = [" << time_result[loop].fav_code << "]\n";
		dout << "begin_weekday = " << time_result[loop].begin_weekday << "\n";
		dout << "end_weekday = " << time_result[loop].end_weekday << "\n";
		dout << "begin_time = " << time_result[loop].begin_time << "\n";
		dout << "end_time = " << time_result[loop].end_time << "\n";
		dout << "dfav = " << time_result[loop].dfav << "\n";
		dout << "eff_date = " << time_result[loop].eff_date << "\n";
		dout << "exp_date = " << time_result[loop].exp_date << "\n";
	}

 	// Start from 1 so in round operation, we can save an add operation.
	long deal_duration = 1;
	double money = 0.0;
	double fav_money = 0.0;
	long total_times = 0;
	long rate_duration;
	int rate_index;
	long rate_times;
	double fav;
	int i;

	while (deal_duration <= duration)
	{
		fav = INT_MAX;
		// Get rate code. It must have value.
		rate_key.begin_weekday = dt.week();
		rate_key.begin_time = dt.time();
		rate_key.lower_limit = deal_duration;
		rate_key.eff_date = dt.duration();


		for (i = 0; i < rate_count; i++)
		{
			if (rate_result[i].compare_linear(&rate_key) == 0)
				break;
		}

		// If we can't find rate_code, that's means parameter is not OK.
		if (i == rate_count) // Not found.
			throw T_BPS_RATE_CODE * 1000 + ERROR_OFFSET;
		else
			rate_index = i;
		if (duration >= rate_result[i].upper_limit)
			rate_duration = rate_result[i].upper_limit - deal_duration;
		else
			rate_duration = duration - deal_duration;
		// In between two date, it's real duration should add 1, and in this circumstance, we must subtract 1,
		// because the real duration of rate_duration is also need to add 1.
		// Below is the same reason.
		rate_duration = std::min(static_cast<long>(rate_result[i].end_time - dt.time()), rate_duration);
		rate_duration = std::min(static_cast<long>(rate_result[i].exp_date - dt.duration()), rate_duration);

		// Get fav date. If not found, that means the rest duration of this day has no fav.
		date_key.begin_date = dt.date();
		date_key.begin_time = dt.time();
		for(i = 0; i < date_count; i++)
		{
			// Find fav date that the current time is in or before the range of fav.
			if (date_result[i].compare_linear( &date_key) == 0)
				break;
		}
		if (i != date_count) // Found fav, it need next step check.
		{
			rate_duration = std::min(static_cast<long>(date_result[i].end_time - dt.time()), rate_duration);
			if (dt.time() >= date_result[i].begin_time) // In the range of fav.
				fav = date_result[i].dfav; // This is the first fav, so need to choose the smaller.
		}

		// Get fav time. If not found, that means the rest duration of this day has no fav.
		time_key.begin_weekday = dt.week();
		time_key.begin_time = dt.time();
		time_key.eff_date = dt.duration();
		for(i = 0; i < time_count; i++)
		{
			if (time_result[i].compare_linear(&time_key) == 0)
				break;
		}

		if (time_count > 0)
		{
			if (i != time_count)    // Found fav time.
			{
				rate_duration = std::min(static_cast<long>(time_result[i].end_time - dt.time()), rate_duration);
				rate_duration = std::min(static_cast<long>(time_result[i].exp_date - dt.duration()), rate_duration);
				if (dt.time() >= time_result[i].begin_time)
					fav = std::min(time_result[i].dfav, fav);

				// If the duration is in two or more than two days, we can only calculate to the end of this day.
				rate_duration = std::min(static_cast<long>(86400 - dt.time()), rate_duration);

				// rate_index is assigned above.
				// Calculator this range's money and times.
				rate_times = rate_duration / rate_result[rate_index].unit_value + 1;
			}
			else    // No found fav time.
			{
				rate_duration = rate_result[rate_index].unit_value;
				rate_times = rate_duration / rate_result[rate_index].unit_value;
			}
		}
		else
		{
			// If the duration is in two or more than two days, we can only calculate to the end of this day.
			rate_duration = std::min(static_cast<long>(86400 - dt.time()), rate_duration);

			// rate_index is assigned above.
			// Calculator this range's money and times.
			rate_times = rate_duration / rate_result[rate_index].unit_value + 1;
		}

		total_times += rate_times;
		double tmp_money = rate_result[rate_index].unit_price * rate_times;
		money += tmp_money;
		if (fav > INT_MAX - 1) // No fav
			fav_money += tmp_money;
		else
			fav_money += tmp_money * fav;

		// Add duration to next bound.
		// Not using rate_duration is because there may have a dead-cycling.
		long tmp_duration = rate_result[rate_index].unit_value * rate_times;
		deal_duration += tmp_duration;
		if (deal_duration <= 0) // prevent overflow
			throw T_BPS_RATE_CODE * 1000 + ERROR_OFFSET;

		dt += tmp_duration;
	}

	common::round(rate.round_mode, rate.precision, money);
	common::round(rate.round_mode, rate.precision, fav_money);
	ostringstream fmt;
	fmt.setf(ios::fixed);
	fmt << std::fixed << std::setprecision(rate.precision) << money;
	// Need a temp variable because ostringstream will release str().
	string tmp = fmt.str();
	strcpy(fee, tmp.c_str());

	fmt.str("");
	fmt << std::fixed << std::setprecision(rate.precision) << fav_money;
	tmp = fmt.str();
	strcpy(fav_fee, tmp.c_str());

	fmt.str("");
	fmt << deal_duration - 1;
	tmp = fmt.str();
	strcpy(bill_duration, tmp.c_str());

	fmt.str("");
	fmt << total_times;
	tmp = fmt.str();
	strcpy(times, tmp.c_str());

 	dout << "fee = [" << fee << "]\n";
	dout << "fav_fee = [" << fav_fee << "]\n";
	dout << "bill_duration = [" << bill_duration << "]\n";
	dout << "times = [" << times << "]\n";
	dout << std::flush;
}

static void get_stream_fee(void* client, const t_bps_rate_dinner& rate, const datetime& dt,
		long duration, char* fee, char* fav_fee, char* bill_duration, char* times)
{
	perf_stat stat(__FUNCTION__);
	billshmclient* shm_client = reinterpret_cast<billshmclient*>(client);

 	int loop;
	dout << "In get_stream_fee, rate_dinner = [" << rate.rate_dinner << "]\n";
	dout << "rate_code = [" << rate.rate_code << "]\n";
	dout << "fav_code = [" << rate.fav_code << "]\n";
	dout << "precision = [" << rate.precision << "]\n";
	dout << "round_mode = [" << rate.round_mode << "]\n";
	dout << "rate_mode = [" << rate.rate_mode << "]\n";

	// Get rate code array.
	t_bps_rate_code rate_key;
	t_bps_rate_code rate_result[::MAX_BPS_RATE_CODE];
	int rate_count;
	strcpy(rate_key.rate_code, rate.rate_code);
	rate_key.begin_weekday = dt.week();
	rate_key.end_weekday = dt.week();
	rate_key.begin_time = dt.time();
	rate_key.end_time = dt.time();
	rate_key.eff_date = dt.duration();
	rate_key.exp_date = dt.duration();
	rate_count = shm_client->search(T_BPS_RATE_CODE, static_cast<void*>(&rate_key), rate_result, ::MAX_BPS_RATE_CODE);
	if (rate_count > ::MAX_BPS_RATE_CODE || rate_count < 1)
		throw T_BPS_RATE_CODE * 1000 + ERROR_OFFSET;

 	dout << "On search rate code, count = " << rate_count << "\n";
	for (loop = 0; loop < rate_count; loop++)
	{
		dout << "Index = " << loop << ":\n";
		dout << "rate_code = [" << rate_result[loop].rate_code << "]\n";
		dout << "begin_weekday = " << rate_result[loop].begin_weekday << "\n";
		dout << "end_weekday = " << rate_result[loop].end_weekday << "\n";
		dout << "begin_time = " << rate_result[loop].begin_time << "\n";
		dout << "end_time = " << rate_result[loop].end_time << "\n";
		dout << "lower_limit = " << rate_result[loop].lower_limit << "\n";
		dout << "upper_limit = " << rate_result[loop].upper_limit << "\n";
		dout << "unit_value = " << rate_result[loop].unit_value << "\n";
		dout << "unit_price = " << rate_result[loop].unit_price << "\n";
		dout << "eff_date = " << rate_result[loop].eff_date << "\n";
		dout << "exp_date = " << rate_result[loop].exp_date << "\n";
	}

 	// Get fav date array.
	t_bps_rate_fav_date date_key;
	t_bps_rate_fav_date date_result;
	int date_count;
	strcpy(date_key.fav_code, rate.fav_code);
	date_key.begin_date = dt.date();
	date_key.end_date = dt.date();
	date_key.begin_time = dt.time();
	date_key.end_time = dt.time();
	date_count = shm_client->search(T_BPS_RATE_FAV_DATE, static_cast<void*>(&date_key), &date_result);

 	dout << "On search rate fav date, count = " << date_count << "\n";
	if (date_count > 0)
	{
		dout << "fav_code = [" << date_result.fav_code << "]\n";
		dout << "begin_date = " << date_result.begin_date << "\n";
		dout << "end_date = " << date_result.end_date << "\n";
		dout << "begin_time = " << date_result.begin_time << "\n";
		dout << "end_time = " << date_result.end_time << "\n";
		dout << "dfav = " << date_result.dfav << "\n";
	}

 	// Get fav time array.
	t_bps_rate_fav_time time_key;
	t_bps_rate_fav_time time_result;
	int time_count;
	strcpy(time_key.fav_code, rate.fav_code);
	time_key.begin_weekday = dt.week();
	time_key.end_weekday = dt.week();
	time_key.begin_time = dt.time();
	time_key.end_time = dt.time();
	time_key.eff_date = dt.duration();
	time_key.exp_date = dt.duration();
	time_count = shm_client->search(T_BPS_RATE_FAV_TIME, static_cast<void*>(&time_key), &time_result);

 	dout << "On search rate fav time, count = " << time_count << "\n";
	if (time_count > 0)
	{
		dout << "fav_code = [" << time_result.fav_code << "]\n";
		dout << "begin_weekday = " << time_result.begin_weekday << "\n";
		dout << "end_weekday = " << time_result.end_weekday << "\n";
		dout << "begin_time = " << time_result.begin_time << "\n";
		dout << "end_time = " << time_result.end_time << "\n";
		dout << "dfav = " << time_result.dfav << "\n";
		dout << "eff_date = " << time_result.eff_date << "\n";
		dout << "exp_date = " << time_result.exp_date << "\n";
	}

 	// Start from 1 so in round operation, we can save an add operation.
	long deal_duration = 1;
	double money = 0.0;
	double fav_money = 0.0;
	long total_times = 0;
	long rate_duration;
	int rate_index;
	int i;
	while (deal_duration <= duration)
	{
		// Get rate code. It must have value.
		rate_key.lower_limit = deal_duration;
		for(i = 0; i < rate_count; i++)
		{
			if (rate_result[i].compare_linear(&rate_key) == 0)
				break;
		}
		// If we can't find rate_code, that's means parameter is not OK.
		if (i == rate_count) // Not found.
			throw T_BPS_RATE_CODE * 1000 + ERROR_OFFSET;
		else
			rate_index = i;
		if (duration >= rate_result[i].upper_limit)
			rate_duration = rate_result[i].upper_limit - deal_duration;
		else
			rate_duration = duration - deal_duration;

		// rate_index is assigned above.
		// Calculator this range's money and times.
		long rate_times = rate_duration / rate_result[rate_index].unit_value + 1;
		total_times += rate_times;
		money += rate_result[rate_index].unit_price * rate_times;
		// Add duration to next bound.
		// Not using rate_duration is because there may have a dead-cycling.
		deal_duration += rate_result[rate_index].unit_value * rate_times;
		if (deal_duration <= 0) // prevent overflow
			throw T_BPS_RATE_CODE * 1000 + ERROR_OFFSET;
	}

	if (date_count > 0)
	{
		if (time_count > 0)
			fav_money = money * std::min(date_result.dfav, time_result.dfav);
		else
			fav_money = money * date_result.dfav;
	}
	else if (time_count > 0)
	{
		fav_money = money * time_result.dfav;
	}
	else
	{
		fav_money = money;
	}
	common::round(rate.round_mode, rate.precision, money);
	common::round(rate.round_mode, rate.precision, fav_money);
	ostringstream fmt;
	fmt.setf(ios::fixed);
	fmt << std::fixed << std::setprecision(rate.precision) << money;
	// Need a temp variable because ostringstream will release str().
	string tmp = fmt.str();
	strcpy(fee, tmp.c_str());
	fmt.str("");
	fmt << std::fixed << std::setprecision(rate.precision) << fav_money;
	tmp = fmt.str();
	strcpy(fav_fee, tmp.c_str());
	fmt.str("");
	fmt << deal_duration - 1;
	tmp = fmt.str();
	strcpy(bill_duration, tmp.c_str());
	fmt.str("");
	fmt << total_times;
	tmp = fmt.str();
	strcpy(times, tmp.c_str());

 	dout << "fee = [" << fee << "]\n";
	dout << "fav_fee = [" << fav_fee << "]\n";
	dout << "bill_duration = [" << bill_duration << "]\n";
	dout << "times = [" << times << "]\n";
	dout << std::flush;
}

int rating(void* client, const char* rate_dinner, time_t eff_date, const char* call_duration, char* fav_fee, char* times)
{
	perf_stat stat(__FUNCTION__);
	billshmclient* shm_client = reinterpret_cast<billshmclient*>(client);

 	dout << "rating:\n";
	dout << "rate_dinner = [" << rate_dinner << "]\n";
	dout << "eff_date = [" << eff_date << "]\n";
	dout << "call_duration = [" << call_duration << "]\n";
	dout << std::flush;

	try
	{
		// If rate_dinner is "", we need not to rate.
		if (rate_dinner[0] == '\0')
		{
			strcpy(fav_fee, "0");
			strcpy(times, "0");
			return 0;
		}

		char fee[12];
		char bill_duration[12];
		datetime dt(eff_date);
		long duration = atol(call_duration);
		t_bps_rate_dinner key;
		t_bps_rate_dinner result;
		int count;

		key.add_tail();
		memcpy(key.rate_dinner, rate_dinner, sizeof(key.rate_dinner) - 1);
		if (shm_client->search(T_BPS_RATE_DINNER, static_cast<void*>(&key), &result) != 1) {
			throw T_BPS_RATE_DINNER * 1000 + ERROR_OFFSET;
		}
		else
		{
			switch (result.rate_mode)
			{
				case mode_duration:
					get_voice_fee(shm_client, result, dt, duration, fee, fav_fee, bill_duration, times);
					break;
				case mode_flux:
					get_stream_fee(shm_client, result, dt, duration, fee, fav_fee, bill_duration, times);
					break;
				default:
					assert(0);
					break;
			}
		}
	}
	catch (int& ex)
	{
		return ex;
	}

	return 0;
}

int rating_fee(void* client, const char* rate_dinner, time_t eff_date, const char* call_duration, char* fee, char* fav_fee, char* bill_duration, char* times)
{
	perf_stat stat(__FUNCTION__);
	billshmclient* shm_client = reinterpret_cast<billshmclient*>(client);

 	dout << "rating_fee:\n";
	dout << "rate_dinner = [" << rate_dinner << "]\n";
	dout << "eff_date = [" << eff_date << "]\n";
	dout << "call_duration = [" << call_duration << "]\n";
	dout << std::flush;

	try
	{
		// If rate_dinner is "", we need not to rate.
		if (rate_dinner[0] == '\0')
		{
			strcpy(fee, "0");
			strcpy(fav_fee, "0");
			strcpy(bill_duration, "0");
			strcpy(times, "0");
			return 0;
		}

		datetime dt(eff_date);
		long duration = atol(call_duration);
		t_bps_rate_dinner key;
		t_bps_rate_dinner result;
		int count;

		key.add_tail();
		memcpy(key.rate_dinner, rate_dinner, sizeof(key.rate_dinner) - 1);
		if (shm_client->search(T_BPS_RATE_DINNER, static_cast<void*>(&key), &result) != 1)
		{
			throw T_BPS_RATE_DINNER * 1000 + ERROR_OFFSET;
		}
		else
		{
			switch (result.rate_mode)
			{
				case mode_duration:
					get_voice_fee(shm_client, result, dt, duration, fee, fav_fee, bill_duration, times);
					break;
				case mode_flux:
					get_stream_fee(shm_client, result, dt, duration, fee, fav_fee, bill_duration, times);
					break;
				default:
					assert(0);
					break;
			}
		}
	}
	catch (int& ex)
	{
		return ex;
	}

	return 0;
}

int get_smg_code(void* client, const char* platform_id, const char* approach_code, const char* service_code,
		const char* org_prefix, const char* trm_prefix, const char* src_in_smg,
		const char* src_out_smg, time_t eff_date, char* in_smg, char* out_smg)
{
	int ret = get_bds_smg_code(client, platform_id, approach_code, service_code, org_prefix, trm_prefix,
	 	src_in_smg, src_out_smg, eff_date, in_smg, out_smg);

	if (ret != 0) {
		ret = get_bds_smg_code(client, platform_id, approach_code, service_code, org_prefix, trm_prefix,
				"*", "*", eff_date, in_smg, out_smg);
	}

	return ret;
}

int is_billing_trunk(void* client, const char* rate_service_class, const char* msc_id,
		const char* in_trunk_id, const char* out_trunk_id, time_t eff_date)
{
	int ret = get_bds_prc_trunk(client, rate_service_class[0], msc_id, in_trunk_id, '0', eff_date);
	if (ret != 0) {
		ret = get_bds_prc_trunk(client, rate_service_class[0], msc_id, out_trunk_id, '1', eff_date);
	}

	return ret;
}

int get_net_type(void* client, const char* rate_service_class, const char* org_trm_id,
		const char* opp_number, time_t eff_date, char* net_type, char* opp_number_suffix)
{
	int trim_prefix_len;

	int ret = get_bds_net_type(client, rate_service_class[0], org_trm_id[0], opp_number, strlen(opp_number),
			eff_date, net_type, &trim_prefix_len);
	if (ret == 0)
		strcpy(opp_number_suffix, opp_number + trim_prefix_len);

	return ret;
}

int is_same_city(void* client, const char* rate_service_class,
		const char* msrn, const char* self_service_class,
		const char* self_region_code, const char* self_user_type,
		const char* opp_dealer_code, const char* opp_service_class,
		const char* opp_region_code, const char* opp_number_type,
		const char* service_code, const char* visit_area_code,
		const char* visit_local_net, const char* org_trm_id, time_t eff_date)
{
	char area_code[8 + 1];
	char local_net[6 + 1];

	int ret = get_bds_msrn_prefix(client, rate_service_class[0], msrn, eff_date, area_code, local_net);
	if (ret != 0)
		return ret;

	return get_bds_same_city(client, org_trm_id[0], self_service_class[0], self_region_code[0], self_user_type[0],
			opp_dealer_code[0], opp_service_class[0], opp_region_code[0], opp_number_type, service_code,
			visit_area_code, visit_local_net, area_code, local_net, eff_date);
}

#endif

}

