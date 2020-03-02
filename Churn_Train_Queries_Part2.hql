drop table if exists dev.venona_avg_std_channel_train;  
create table dev.venona_avg_std_channel_train as select
a.account_guid as account_guid_schannel
,avg(std_channel_watch) as avg_std_channel
from
(select
	account_guid
	,stddev_pop(watch_time_ms) as std_channel_watch
from dev.venona_churn_active_train
where playback_type='linear' and tms_guide_id is not NULL and denver_date BETWEEN min_agg_date AND max_agg_date     --CHANGE DATE
group by account_guid, tms_guide_id, WEEKOFYEAR(denver_date)	) a
group by a.account_guid;

drop table if exists dev.never_logged_in_train;
create table dev.never_logged_in_train as
    SELECT metric.account_guid as account_guid_noLog, acct.billing_id, 1 as never_logged_in
    FROM (
      SELECT 
        billing_id
        ,mso
        ,billing_division
        ,CASE mso
              WHEN 'L-BHN' THEN CONCAT_WS('::', 'L-BHN', billing_id)
              WHEN 'L-CHTR' THEN CONCAT_WS('::', 'L-CHTR', billing_id)
              WHEN 'L-TWC' THEN CONCAT_WS('::', 'L-TWC', billing_division, billing_id)
          ELSE billing_id END AS account_guid
      FROM  prod.venona_acct_agg   
    ) metric
    RIGHT JOIN (
          SELECT DISTINCT mso, billing_id, system__sys  
          FROM dev.ml_account_changes_v1         
          --WHERE video_package_type = 'TV_STREAM'                
          WHERE tv_stream_package IS NOT NULL
            AND change_event_date BETWEEN '${hiveconf:min_date_agg}' and '${hiveconf:current_date}'  --CHANGE DATE
    ) acct                                    
    ON acct.billing_id = metric.billing_id and acct.mso = metric.mso and acct.system__sys = metric.billing_division
    WHERE metric.billing_id IS NULL AND metric.mso IS NULL and metric.billing_division IS NULL;


drop table if exists dev.venona_all_churnJan31_90_train;  
create table dev.venona_all_churnJan31_90_train as select 
	 a.*												
	,b.*
	,c.*
	--,d.*
  ,e.never_logged_in as never_logged_in_flag  --LEFT JOIN never_logged_in
from dev.venona_main_churn_train a FULL OUTER JOIN dev.venona_top_channel_train b on (a.account_guid=b.account_guid_tchannel)
FULL OUTER JOIN dev.venona_avg_std_channel_train c on (b.account_guid_tchannel=c.account_guid_schannel) LEFT JOIN dev.never_logged_in_train e on 
(c.account_guid_schannel=e.account_guid_noLog);



--PROMO ANALYSIS
--Same as for test set
drop table if exists dev.promo_account_history;
create table dev.promo_account_history as
select 
    CONCAT_WS('::',mso,system__sys,billing_id) as account_guid
  ,z.account__number_aes
  ,mso
  ,count(distinct zip_concat) as count_zip_code_changes --need to control for long tenure
  ,sum(customer__truck_trouble_call_count) as sum_truck_trouble_call
  ,sum(case when promo_desc<>'' then 1 else 0 end) as total_promo_5percent_flag
  ,z.most_recent_dwelling
  ,z.most_recent_promo_5percent_flag
from 
    (select REGEXP_EXTRACT(product__promotion_description,'(.*)(SB015|SI019|SV501)(.*)',2) AS promo_desc --sum for promo cnt
     ,account__number_aes
     ,prod.aes_encrypt(CASE WHEN LENGTH(prod.aes_decrypt(account__number_aes)) < 9 
        THEN LPAD(prod.aes_decrypt(account__number_aes), 9, '0') 
          ELSE prod.aes_decrypt(account__number_aes) END) AS billing_id
     ,'L-TWC' as mso
     ,system__sys
   ,customer__truck_trouble_call_count
   ,account__dwelling_description
   ,CONCAT(account__zip_code,account__zip_code_4) as zip_concat
   from prod.twc_account_history where product__promotion_description<>'Unkown'
   AND INSTR(LOWER(prod.aes_decrypt256(bill_customer_name_aes256)), 'tamtool') < 1
    AND ( video__strm_fl = TRUE OR video_strm_choice_fl = TRUE )
    AND partition_date >= '2017-06-24') d   --CHANGE DATE, restrict to only active customers------
         
INNER JOIN  --queries below retrieve most recent info about price

(select
 a.account__number_aes
,account__dwelling_description as most_recent_dwelling
,case when promo_desc_most_recent<>'' then 1 else 0 end as most_recent_promo_5percent_flag
from
  (select account__number_aes
    ,partition_date
    ,account__dwelling_description 
    ,REGEXP_EXTRACT(product__promotion_description,'(.*)(SB015|SI019|SV501)(.*)',2) AS promo_desc_most_recent
    from prod.twc_account_history) a      
 INNER JOIN --most recent date and promo
  (select max(partition_date) as max_date
    ,account__number_aes
    from prod.twc_account_history
    WHERE INSTR(LOWER(prod.aes_decrypt256(bill_customer_name_aes256)), 'tamtool') < 1
      AND ( video__strm_fl = TRUE OR video_strm_choice_fl = TRUE )
    --  AND partition_date = '${hiveconf:current_date}'          --CHANGE DATE, restrict to only active customers------
    group by account__number_aes --Limit to only active accounts
      ) b
 on a.account__number_aes=b.account__number_aes and a.partition_date=b.max_date 
      group by a.account__number_aes, a.account__dwelling_description, a.promo_desc_most_recent) z
      on d.account__number_aes=z.account__number_aes 
group by z.account__number_aes, mso,billing_id,system__sys ,z.most_recent_promo_5percent_flag, z.most_recent_dwelling


UNION ALL

--Start BHN
select 
    CONCAT_WS('::',mso,billing_id) as account_guid
  ,z.account__number_aes
  ,mso
  ,count(distinct zip_concat) as count_zip_code_changes --need to control for long tenure
  ,sum(customer__truck_trouble_call_count) as sum_truck_trouble_call
  ,sum(case when promo_desc<>'' then 1 else 0 end) as total_promo_5percent_flag
  ,z.most_recent_dwelling
  ,z.most_recent_promo_5percent_flag
from 
    (select REGEXP_EXTRACT(product__promotion_description,'(.*)(Pre TV|Phone|A-Triple Play TV Select Free DVR+Internet+Voice|HDDVR)(.*)',2) AS promo_desc --sum for promo cnt
     ,account__number_aes
     ,prod.aes_encrypt(substr(prod.aes_decrypt(account__number_aes),4)) AS billing_id
     ,'L-BHN' as mso
   ,customer__truck_trouble_call_count
   ,account__dwelling_description
   ,CONCAT(account__zip_code,account__zip_code_4) as zip_concat
   from prod.bhn_account_history where product__promotion_description<>'Unkown'
   AND INSTR(LOWER(prod.aes_decrypt256(bill_customer_name_aes256)), 'tamtool') < 1
    AND (video__strm_fl = TRUE OR spp_choice_fl = TRUE)
   AND partition_date >= '2017-06-24') d    --CHANGE DATE, restrict to only active customers------
         
INNER JOIN  --queries below retrieve most recent info about price

(select
 a.account__number_aes
,account__dwelling_description as most_recent_dwelling
,case when promo_desc_most_recent<>'' then 1 else 0 end as most_recent_promo_5percent_flag
from
  (select account__number_aes
    ,partition_date
    ,account__dwelling_description 
    ,REGEXP_EXTRACT(product__promotion_description,'(.*)(Pre TV|Phone|A-Triple Play TV Select Free DVR+Internet+Voice|HDDVR)(.*)',2) AS promo_desc_most_recent
    from prod.bhn_account_history) a      
 INNER JOIN --most recent date and promo
  (select max(partition_date) as max_date
    ,account__number_aes
    from prod.bhn_account_history
    WHERE INSTR(LOWER(prod.aes_decrypt256(bill_customer_name_aes256)), 'tamtool') < 1
    AND (video__strm_fl = TRUE OR spp_choice_fl = TRUE)
     AND partition_date >= '2017-06-24'     --CHANGE DATE, restrict to only active customers------
    group by account__number_aes --Limit to only active accounts
      ) b
 on a.account__number_aes=b.account__number_aes and a.partition_date=b.max_date 
      group by a.account__number_aes, a.account__dwelling_description, a.promo_desc_most_recent) z
      on d.account__number_aes=z.account__number_aes 
group by z.account__number_aes, mso,billing_id ,z.most_recent_promo_5percent_flag, z.most_recent_dwelling


UNION ALL

--Start CHTR
select 
    CONCAT_WS('::',mso,account__number_aes) as account_guid
  ,z.account__number_aes
  ,mso
  ,count(distinct zip_concat) as count_zip_code_changes --need to control for long tenure
  ,sum(customer__truck_trouble_call_count) as sum_truck_trouble_call
  ,sum(case when promo_desc<>'' then 1 else 0 end) as total_promo_5percent_flag
  ,z.most_recent_dwelling
  ,z.most_recent_promo_5percent_flag
from 
    (select REGEXP_EXTRACT(product__promotion_description,'(.*)(AR|AY|OR|AK)(.*)',2) AS promo_desc --sum for promo cnt
     ,account__number_aes
     --,prod.aes_encrypt(substr(prod.aes_decrypt(account__number_aes),4)) AS billing_id
     ,'L-CHTR' as mso
   ,customer__truck_trouble_call_count
   ,account__dwelling_description
   ,CONCAT(account__zip_code,account__zip_code_4) as zip_concat
   from prod.account_history where product__promotion_description<>'Unkown'
   AND ((customer__type = 'Commercial' and meta__file_type = 'Commercial Business')
OR (customer__type = 'Residential' and meta__file_type = 'Residential'))
AND lower(account__type) NOT IN ( 'employee', 'test' )
AND product__video_package_type IN ('SPP Spectrum TV Stream', 'Spectrum TV Stream', 'SPP Choice')
    AND partition_date_time >= '2017-06-24') d   --CHANGE DATE, restrict to only active customers------
         
INNER JOIN  --queries below retrieve most recent info about price

(select
 a.account__number_aes
,account__dwelling_description as most_recent_dwelling
,case when promo_desc_most_recent<>'' then 1 else 0 end as most_recent_promo_5percent_flag
from
  (select account__number_aes
    ,partition_date_time
    ,account__dwelling_description 
    ,REGEXP_EXTRACT(product__promotion_description,'(.*)(AR|AY|OR|AK)(.*)',2) AS promo_desc_most_recent
    from prod.account_history) a      
 INNER JOIN --most recent date and promo
  (select max(partition_date_time) as max_date
    ,account__number_aes
    from prod.account_history
    WHERE ((customer__type = 'Commercial' and meta__file_type = 'Commercial Business')
OR (customer__type = 'Residential' and meta__file_type = 'Residential'))
AND lower(account__type) NOT IN ( 'employee', 'test' )
AND product__video_package_type IN ('SPP Spectrum TV Stream', 'Spectrum TV Stream', 'SPP Choice')
      AND partition_date_time >= '2017-06-24'    --CHANGE DATE, restrict to only active customers------
    group by account__number_aes --Limit to only active accounts
      ) b
 on a.account__number_aes=b.account__number_aes and a.partition_date_time=b.max_date 
      group by a.account__number_aes, a.account__dwelling_description, a.promo_desc_most_recent) z
      on d.account__number_aes=z.account__number_aes 
group by z.account__number_aes, mso ,z.most_recent_promo_5percent_flag, z.most_recent_dwelling;


--COMBINE ALL VARS TOGETHER
drop table if exists dev.churn_all_vars_90day_train;
create table dev.churn_all_vars_90day_train as select
--dev.churn_labelJan31expanded
 f.account_guid
,f.churn
--,f.tv_stream_tenure                                           
--,f.customer_tenure                                            
--,f.connect_date                                             
--,f.convert_date 

--dev.churn_active_accounts_train
,g.mso
,g.system__sys
,g.billing_id
,g.account__number_aes
,'25' as week_number

--dev.mos_acct_total_train
,a.mos_comp_cnt_acct_bigger
,a.mos_comp_avg_acct_bigger 
,a.mos_comp_std_dev_acct 
,a.comp_avg_mos_bottom_10 
,a.comp_avg_mos_top_10        
,a.comp_weighted_avg_mos_acct 
,a.mos_comp_weighted_cnt_acct_bigger
,a.mos_comp_weighted_avg_acct_bigger 
,a.mos_comp_weighted_std_dev_acct

--dev.mos_acct_train
,a.active_day_cnt
,a.mos_bottom_10_percentile
,a.mos_top_10_percentile
,a.mos_std_dev_acct
,a.avg_mos_acct
,a.weighted_avg_mos_acct
,a.weighted_std_dev_acct

--dev.mos_acct_total_channel_number_train
,a.mos_comp_channel_cnt_acct_bigger
,a.mos_comp_channel_avg_acct_bigger 
,a.mos_comp_channel_std_dev_acct 
,a.mos_comp_channel_avg_mos_bottom_10 
,a.mos_comp_channel_avg_mos_top_10   
,a.mos_comp_channel_weighted_avg_mos_acct 
,a.mos_comp_channel_weighted_cnt_acct_bigger
,a.mos_comp_channel_weighted_avg_acct_bigger 
,a.mos_comp_channel_weighted_std_dev_acct 

--dev.mos_acct_week_part_train
,a.mos_comp_week_part_cnt_acct_bigger
,a.mos_comp_week_part_avg_acct_bigger 
,a.mos_comp_week_part_std_dev_acct 
,a.mos_comp_week_part_channel_avg_mos_bottom_10 
,a.mos_comp_week_part_avg_mos_top_10   
,a.mos_comp_week_part_weighted_avg_mos_acct 
,a.mos_comp_week_part_weighted_cnt_acct_bigger
,a.mos_comp_week_part_weighted_avg_acct_bigger 
,a.mos_comp_week_part_weighted_std_dev_acct 

--dev.mos_acct_day_part_train
,a.mos_comp_day_part_cnt_acct_bigger
,a.mos_comp_day_part_avg_acct_bigger 
,a.mos_comp_day_part_std_dev_acct 
,a.mos_comp_day_part_channel_avg_mos_bottom_10 
,a.mos_comp_day_part_avg_mos_top_10   
,a.comp_day_part_weighted_avg_mos_acct 
,a.mos_comp_day_part_weighted_cnt_acct_bigger
,a.mos_comp_day_part_weighted_avg_acct_bigger 
,a.mos_comp_day_part_weighted_std_dev_acct 

--dev.venona_main_churn_train
,b.sum_sports_flag
,b.avg_sports_flag
,b.avg_prem_flag
,b.sum_prem_flag 
,b.avg_hd_flag
,b.sum_hd_flag
,b.iphone_device_watch_time
,b.androidphone_device_watch_time
,b.uwp_device_watch_time  
,b.samsungtv_device_watch_time
,b.androidtablet_device_watch_time
,b.webbrowser_device_watch_time
,b.ipad_device_watch_time
,b.ipod_device_watch_time
,b.xbox_device_watch_time
,b.roku_device_watch_time
,b.watchtime10_L2DownShift
,b.avg_mos_x_time
,b.avg_rental_duration_hours   
,b.avg_content_available_date     
,b.avg_content_runtime                                            
,b.avg_content_expiration_date                                      
,b.avg_content_original_air_date                                                       
,b.avg_content_year         
,b.avg_is_active                                                
,b.avg_h_playback_start                                       
,b.avg_stream_init_starts                                       
,b.avg_stream_noninit_failures                                      
,b.avg_stream_init_failures                                       
,b.avg_stream_failures                                          
,b.avg_stream_stops                                             
,b.avg_tune_time_ms                                             
,b.avg_watch_time_ms                                            
,b.avg_bandwidth_consumed_mbps                                      
,b.avg_bitrate_content_elapsed_ms                                     
,b.avg_buffering_events                                         
,b.avg_buffering_stops                                          
,b.avg_buffering_duration_ms                                      
,b.avg_buffering_ratio                                          
,b.avg_buffering_score                                          
,b.avg_ad_breaks                                                
,b.avg_ad_events                                                
,b.avg_last_ad_sequence_number                                        
,b.avg_last_ad_end_sequence_number     
,b.avg2_bitrate_mbps                                          
,b.avg_bitrate_upshifts                                         
,b.avg_bitrate_downshifts                                                                 
,b.avg_downshifts_score                                         
,b.avg_start_timestamp                                          
,b.avg_end_timestamp                                            
,b.avg_lt_stop_sequence_number                                      
,b.avg_lt_stop_timestamp                                                                                                 
,b.avg_level2_downshifts                      
,b.avg_pibbe_ms                             
,b.avg_pibd2_ms                             
,b.avg_mos_score                             
,b.avg_watch_time_on_net_ms                                     
,b.avg_watch_time_off_net_ms                                                                        
,b.sum_stream_failures
,b.pctn_watch_time_off_net_ms
,b.max_denver_date
,b.dist_date_percent   
,b.dist_linear_channel_name
,b.dist_linear_network_name
,b.dist_linear_channel_category
,b.distinct_device_cnt
,b.sum_linear
,b.sum_vod
,b.max_linear_denver_date
,b.max_vod_denver_date
,b.sum_user_stream_cancel
,b.sum_user_stream_NOT_cancel
,b.total_stream_count_date                                                                                

--dev.venona_top_channel_train
,b.top_channel_watch_time                                           
,b.channel_watch_time                                                                          
                                      
--dev.search_features_train                                     
--,b.sum_unique_searches                                      
--,b.avg_unique_searches                                      
--,b.sum_search_entered_returned_no_results                                     
--,b.avg_search_entered_returned_no_results                                     
--,b.sum_search_entered_selected_results                                      
--,b.avg_search_entered_selected_results                                      
--,b.sum_search_response_time_ms                                      
--,b.avg_search_response_time_ms                                      
--,b.never_logged_in_flag 

--dev.churn_call_center_all_train
,c.total_calls
,c.call_tenure
,c.total_same_day_calls
,c.count_days_when_called --change to count_days_when_called
,c.time_from_install_to_first_call
,c.avg_call_date


--dev.churn_price
,d.most_recent_internet_package_change
,d.most_recent_video_package_change
,d.most_recent_other_package_change
,d.most_recent_tv_stream_package_change
,d.most_recent_stream_choice_package_change
,d.most_recent_internet_price_change
,d.most_recent_video_price_change
,d.most_recent_total_price_change
,d.package_internet_add_drop
,d.package_internet_changes
,d.package_internet_adds
,d.package_internet_drop
,d.package_video_add_drop
,d.package_video_changes
,d.package_other_add_drop
,d.package_other_changes
,d.package_tv_stream_add_drop
,d.package_tv_stream_changes
,d.package_stream_choice_package_add_drop
,d.package_stream_choice_package_changes
,d.price_internet_increase_decrease
,d.price_internet_changes
,d.price_video_increase_decrease
,d.price_video_total_changes
,d.price_total_increase_decrease
,d.price_total_changes
,d.total_count_int_100_day
,d.total_count_int_day
,d.percentile_score_price  
,d.package_other_adds
,d.package_other_drop
,d.package_tv_stream_adds
,d.package_tv_stream_drop
,d.package_video_adds
,d.package_video_drop
,d.package_stream_choice_adds
,d.package_stream_choice_drop
,d.internet_price_adds
,d.internet_price_drop
,d.video_price_adds
,d.video_price_drop
,d.total_price_adds
,d.total_price_drop


--dev.promo_account_history
,e.count_zip_code_changes 
,e.sum_truck_trouble_call
,e.total_promo_5percent_flag
,e.most_recent_dwelling
,e.most_recent_promo_5percent_flag

from dev.churn_labelJan31expanded f LEFT JOIN dev.churn_active_accounts_train g on f.account_guid=g.account_guid
LEFT JOIN
dev.mos_variables_90day_train a on g.account_guid=a.acct_guid 
LEFT JOIN
dev.venona_all_churnJan31_90_train b on a.acct_guid=b.account_guid 
LEFT JOIN 
dev.churn_call_center_all_train c on b.account_guid=c.dec_account_guid 
LEFT JOIN
dev.churn_price d on c.dec_account_guid=d.account_guid
LEFT JOIN
dev.promo_account_history e on d.account_guid=e.account_guid;
