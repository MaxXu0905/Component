/**
 * @file search.h
 * @brief The definition of rate lib.
 *
 * @author
 * @author Former author list: Genquan Xu
 * @version 4.1.0
 * @date 2006-07-22
 */

#if !defined(__SEARCH_H__)
#define __SEARCH_H__

#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <errno.h>
#include <values.h>
#include <assert.h>
#include <string.h>
#include <strings.h>
#include <stdarg.h>
#include <time.h>
#include <unistd.h>
#include <ctype.h>
#include <fcntl.h>

/*
    The returning value of all functions returning int is defined below:
    On success: return 0;
    On error: return error_code, which is combined of error_type('03') + error_no(table_id, 4).
*/

#if defined(__cplusplus)

extern "C"
{

#endif

/* These functions are used internally. */

void rtrim_char(char *number, char ch);
void ltrim_char(char* src, char ch);
void ip_trans2hex(const char* dec_ip, char* hex_ip);
time_t get_time(const char* date, const char* time);
int check_datetime(const char* datetime);
int is_number(const char* number);
int is_integer(const char* number);
int is_double(const char* number);
void trim_alpha(char* number);
void trim_illegal_char(char* src);
void str_add(const char *need_process, const char *add_ch, char *processed ,const char *delimiter,const int flag);
void str_cut(const char *need_process, const int number, char *processed,const char *delimiter, const int flag);
void str_chg_delimit(const char *process_ch, const char *old_delimiter, const char *instead_delimiter, char *processed_ch);
void str_sum(const char *process_ch, const char *delimiter, char *processed);
void str_column_sum(const char *process_ch1,const char *process_ch2, const char *delimiter,char *processed);
int get_duration_datetime(const char* duration, char* datetime);
void exchange_char(char *strp, char csource, char ctarget);

time_t ai_app_string_to_time_t(const char *data);
const char * ai_app_char_to_string(char value);
const char * ai_app_uchar_to_string(unsigned char value);
const char * ai_app_short_to_string(short value);
const char * ai_app_ushort_to_string(unsigned short value);
const char * ai_app_int_to_string(int value);
const char * ai_app_uint_to_string(unsigned int value);
const char * ai_app_long_to_string(long value);
const char * ai_app_ulong_to_string(unsigned long value);
const char * ai_app_float_to_string(float value);
const char * ai_app_double_to_string(double value);
const char * ai_app_time_t_to_string(time_t value);

#if defined(__BPS_SEARCH__)

int get_service_code(void* client, const char* ss_code, const char* bearer_service,
		const char* tele_service, time_t eff_date, char* service_code);
void standard_number(void* client, const char* file_type, time_t eff_date, char* number);
int get_tele_info(void* client, const char* visit_area_code, char* number, time_t eff_date,
		char* area_code, char* local_net, char* busi_district, char* dealer_code, char* user_type,
		char* service_class);
int get_region_code(void* client, const char* area_code, const char* local_net,
		const char* visit_area_code, const char* visit_local_net, time_t eff_date,
		char* region_code);
int get_voice_rating(void* client, const char* visit_local_net, const char* self_local_net, const char* self_service_class,
		const char* self_region_code, const char* self_user_type, const char* self_trunk_type, const char* org_trm_id,
		const char* opp_dealer_code, const char* opp_service_class, const char* opp_region_code,
		const char* opp_number_type, int number_type_flag, const char* same_city_flag,
		const char* service_code, const char* net_type, const char* call_type, int fee_item,
		char* item_fee_code, char* rate_dinner, int* base_flag, int* land_flag, int* other_flag, int* rebill_flag);
int rating(void* client, const char* rate_dinner, time_t eff_date, const char* call_duration, char* fav_fee, char* times);
int rating_fee(void* client, const char* rate_dinner, time_t eff_date, const char* call_duration, char* fee, char* fav_fee,
		char* bill_duration, char* times);
int get_smg_code(void* client, const char* platform_id, const char* approach_code, const char* service_code, const char* org_prefix,
		const char* trm_prefix, const char* src_in_smg, const char* src_out_smg, time_t eff_date, char* in_smg, char* out_smg);
int is_billing_trunk(void* client, const char* rate_service_class, const char* msc_id,
		const char* in_trunk_id, const char* out_trunk_id, time_t eff_date);
int get_net_type(void* client, const char* rate_service_class, const char* org_trm_id,
		const char* opp_number, time_t eff_date, char* net_type, char* opp_number_suffix);
int is_same_city(void* client, const char* rate_service_class,
		const char* msrn, const char* self_service_class,
		const char* self_region_code, const char* self_user_type,
		const char* opp_dealer_code, const char* opp_service_class,
		const char* opp_region_code, const char* opp_number_type,
		const char* service_code, const char* visit_area_code,
		const char* visit_local_net, const char* org_trm_id, time_t eff_date);

#endif

int get_channel(void *instance_mgr, const char* chnl_id);


#include "search_h.inl"

#if defined(__cplusplus)

}

#endif

/*
    When used by compiler for compiling, we need to replace, so we no need to write first parameter.
    But it can't be visible to search.cpp.
*/
#if !defined(__cplusplus)

#if defined(__BPS_SEARCH__)

#define get_service_code(ss_code, bearer_service, \
		tele_service, eff_date, service_code) \
		get_service_code(client, ss_code, bearer_service, \
		tele_service, eff_date, service_code)
#define standard_number(file_type, eff_date, number) \
		standard_number(client, file_type, eff_date, number)
#define get_tele_info(visit_area_code, number, eff_date, \
		area_code, local_net, busi_district, dealer_code, user_type, \
		service_class) \
		get_tele_info(client, visit_area_code, number, eff_date, \
		area_code, local_net, busi_district, dealer_code, user_type, \
		service_class)
#define get_region_code(area_code, local_net, \
		visit_area_code, visit_local_net, eff_date, \
		region_code) \
		get_region_code(client, area_code, local_net, \
		visit_area_code, visit_local_net, eff_date, \
		region_code)
#define get_voice_rating(visit_local_net, self_local_net, self_service_class, \
		self_region_code, self_user_type, self_trunk_type, org_trm_id, \
		opp_dealer_code, opp_service_class, opp_region_code, \
		opp_number_type, number_type_flag, same_city_flag, \
		service_code, net_type, call_type, fee_item, \
		item_fee_code, rate_dinner, base_flag, land_flag, other_flag, rebill_flag) \
		get_voice_rating(client, visit_local_net, self_local_net, self_service_class, \
		self_region_code, self_user_type, self_trunk_type, org_trm_id, \
		opp_dealer_code, opp_service_class, opp_region_code, \
		opp_number_type, number_type_flag, same_city_flag, \
		service_code, net_type, call_type, fee_item, \
		item_fee_code, rate_dinner, base_flag, land_flag, other_flag, rebill_flag)
#define rating(rate_dinner, eff_date, call_duration, fav_fee, times) \
		rating(client, rate_dinner, eff_date, call_duration, fav_fee, times)
#define rating_fee(rate_dinner, eff_date, call_duration, fee, fav_fee, \
		bill_duration, times) \
		rating_fee(client, rate_dinner, eff_date, call_duration, fee, fav_fee, \
		bill_duration, times)
#define get_smg_code(platform_id, approach_code, service_code, org_prefix, \
		trm_prefix, src_in_smg, src_out_smg, eff_date, in_smg, out_smg) \
		get_smg_code(client, platform_id, approach_code, service_code, org_prefix, \
		trm_prefix, src_in_smg, src_out_smg, eff_date, in_smg, out_smg)
#define is_billing_trunk(rate_service_class, msc_id, \
		in_trunk_id, out_trunk_id, eff_date) \
		is_billing_trunk(client, rate_service_class, msc_id, \
		in_trunk_id, out_trunk_id, eff_date)
#define get_net_type(rate_service_class, org_trm_id, \
		opp_number, eff_date, net_type, opp_number_suffix) \
		get_net_type(client, rate_service_class, org_trm_id, \
		opp_number, eff_date, net_type, opp_number_suffix)
#define is_same_city(rate_service_class, \
		msrn, self_service_class, \
		self_region_code,  self_user_type, \
		opp_dealer_code, opp_service_class, \
		opp_region_code, opp_number_type, \
		service_code, visit_area_code, \
		visit_local_net, org_trm_id, eff_date) \
		is_same_city(client, rate_service_class, \
		msrn, self_service_class, \
		self_region_code,  self_user_type, \
		opp_dealer_code, opp_service_class, \
		opp_region_code, opp_number_type, \
		service_code, visit_area_code, \
		visit_local_net, org_trm_id, eff_date)

#endif

#endif

#include "search_def.inl"

#endif

