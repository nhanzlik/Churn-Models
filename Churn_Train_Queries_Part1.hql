drop table if exists dev.churn_labelJan31expanded;
create table dev.churn_labelJan31expanded as
SELECT 
      *                                          
      ,case when churn_date='' then date_sub('${hiveconf:current_date}',90) else date_sub(churn_date,90) end as min_agg_date 
      ,case when churn_date='' then '${hiveconf:current_date}' else churn_date end as max_agg_date
FROM dev.churn_labelJan31;


--Not necessary if table created during test queries
drop table if exists dev.venona_stream_agg;
create table dev.venona_stream_agg as select  
content_genres 
,provider_asset_id 
,screen_resolution
,stream_id
,billing_id
,rental_duration_hours   
,content_available_date
,content_runtime                                            
,content_expiration_date                                      
,content_original_air_date                                                       
,content_year       
,is_active                                                
,has_playback_start                                       
,stream_init_starts                                     
,stream_noninit_failures                                    
,stream_init_failures                                                                                 
,stream_stops                                             
,tune_time_ms                                             
,watch_time_ms                                            
,bandwidth_consumed_mb                                      
,bitrate_content_elapsed_ms                                     
,buffering_events                                         
,buffering_stops                                          
,buffering_duration_ms                                      
,buffering_ratio                                          
,buffering_score                                          
,ad_breaks                                                
,ad_events                                                
,linear_channel_number
,last_ad_sequence_number                                      
,last_ad_end_sequence_number     
,avg_bitrate_mbps                                         
,bitrate_upshifts                                         
,bitrate_downshifts                                                                 
,downshifts_score                                         
,start_timestamp                                          
,end_timestamp                                            
,last_stop_sequence_number                                      
,last_stop_timestamp                                                                                               
,level2_downshifts                      
,pibbe_ms                             
,pibd2_ms                             
,mos_score                             
,watch_time_on_net_ms                                                                                                             
,stream_failures
,watch_time_off_net_ms
,linear_channel_name
,linear_network_name
,linear_channel_category
,device_type
,playback_type
,user_cancels_stream
,tms_guide_id
,mso
,billing_division
,CASE mso
              WHEN 'L-BHN' THEN CONCAT_WS('::', 'L-BHN', billing_id)
              WHEN 'L-CHTR' THEN CONCAT_WS('::', 'L-CHTR', billing_id)
              WHEN 'L-TWC' THEN CONCAT_WS('::', 'L-TWC', billing_division, billing_id)
          ELSE billing_id END AS account_guid
,denver_date
from prod.venona_stream_agg;


drop table if exists dev.churn_active_accounts_train;
create table dev.churn_active_accounts_train as
  select distinct * from
(SELECT * 
from 
(SELECT
CONCAT_WS('::','L-CHTR',account__number_aes) as account_guid
,'L-CHTR' AS mso
,account__number_aes
,account__number_aes AS billing_id
,system__sys
FROM prod.account_history
WHERE ((customer__type = 'Commercial' and meta__file_type = 'Commercial Business')
OR (customer__type = 'Residential' and meta__file_type = 'Residential'))
AND lower(account__type) NOT IN ( 'employee', 'test' )
AND product__video_package_type IN ('SPP Spectrum TV Stream', 'Spectrum TV Stream', 'SPP Choice')
) c
LEFT SEMI JOIN 
dev.churn_labelJan31expanded c2 on c2.account_guid=c.account_guid

UNION

SELECT * 
from
(SELECT 
CONCAT_WS('::',mso,billing_id) as account_guid
,mso
,account__number_aes
,billing_id
,system__sys
FROM
(SELECT
prod.aes_encrypt(substr(prod.aes_decrypt(account__number_aes),4)) AS billing_id,
account__number_aes,
NULL AS system__sys,
'L-BHN' AS mso,
partition_date AS partition_date_time
FROM prod.bhn_account_history
WHERE INSTR(LOWER(prod.aes_decrypt256(bill_customer_name_aes256)), 'tamtool') < 1
AND (video__strm_fl = TRUE OR spp_choice_fl = TRUE)
  ) b
)b1
LEFT SEMI JOIN
dev.churn_labelJan31expanded b2 on b2.account_guid=b1.account_guid

UNION

SELECT * 
from
(SELECT
CONCAT_WS('::',mso,system__sys,billing_id) as account_guid --FOR TWC contruct acct_guid as mso,system__sys,billing_id
,mso
,account__number_aes
,billing_id
,system__sys
from
(SELECT
prod.aes_encrypt(CASE WHEN LENGTH(prod.aes_decrypt(account__number_aes)) < 9 THEN LPAD(prod.aes_decrypt(account__number_aes), 9, '0') ELSE prod.aes_decrypt(account__number_aes) END) AS billing_id,
account__number_aes,
system__sys,
'L-TWC' AS mso,
partition_date AS partition_date_time
FROM prod.twc_account_history
WHERE INSTR(LOWER(prod.aes_decrypt256(bill_customer_name_aes256)), 'tamtool') < 1
AND ( video__strm_fl = TRUE OR video_strm_choice_fl = TRUE )
  ) a  
)a1
LEFT SEMI JOIN 
dev.churn_labelJan31expanded a2 on a2.account_guid=a1.account_guid) z;



drop table if exists dev.venona_churn_active_train;
create table dev.venona_churn_active_train as 
  SELECT a.min_agg_date
         ,a.max_agg_date
         ,a.churn
         ,a.tv_stream_tenure                                           
         ,a.customer_tenure                                            
         ,a.connect_date                                             
         ,a.convert_date 
         ,b.*
        FROM dev.churn_labelJan31expanded a LEFT JOIN dev.venona_stream_agg b ON a.account_guid=b.account_guid;

                                                    



--PRICE ANALYSIS
--Has one variable the goes through all of time and another that is most recent
--most recent is only considered for percentile score

drop table if exists dev.churn_price;
create table dev.churn_price as 
select account_guid
,most_recent_internet_package_change
,most_recent_video_package_change
,most_recent_other_package_change
,most_recent_tv_stream_package_change
,most_recent_stream_choice_package_change
,most_recent_internet_price_change
,most_recent_video_price_change
,most_recent_total_price_change
,package_internet_add_drop
,package_internet_changes
,package_internet_adds
,package_internet_drop
,package_video_add_drop
,package_video_changes
,package_other_add_drop
,package_other_changes
,package_tv_stream_add_drop
,package_tv_stream_changes
,package_stream_choice_package_add_drop
,package_stream_choice_package_changes
,price_internet_increase_decrease
,price_internet_changes
,price_video_increase_decrease
,price_video_total_changes
,price_total_increase_decrease
,price_total_changes
,total_count_int as total_count_int_100_day
,total_count_int/100 as total_count_int_day
,CASE
  when total_count_int between percentiles[1] and percentiles[2] then '10_20percent'
  when total_count_int between percentiles[2] and percentiles[3] then '20_30percent'
  when total_count_int between percentiles[3] and percentiles[4] then '30_40percent'
  when total_count_int between percentiles[4] and percentiles[5] then '40_50percent'
  when total_count_int between percentiles[5] and percentiles[6] then '50_60percent'
  when total_count_int between percentiles[6] and percentiles[7] then '60_70percent'
  when total_count_int between percentiles[7] and percentiles[8] then '70_80percent'
  when total_count_int between percentiles[8] and percentiles[9] then '80_90percent'
  when total_count_int > percentiles[9] then '90_100percent'
  ELSE '10percent' end as percentile_score_price  --maybe drop NULL defaults to 10percent

,package_other_adds
,package_other_drop
,package_tv_stream_adds
,package_tv_stream_drop
,package_video_adds
,package_video_drop
,package_stream_choice_adds
,package_stream_choice_drop
,internet_price_adds
,internet_price_drop
,video_price_adds
,video_price_drop
,total_price_adds
,total_price_drop
from
(select -- most recent event for each variable
  billing_id
  ,mso
  ,system__sys
  ,account__number_aes
  ,change_event_date 
,internet_package as most_recent_internet_package_change      
,video_package as most_recent_video_package_change        
,other_package as most_recent_other_package_change        
,tv_stream_package as most_recent_tv_stream_package_change    
,stream_choice_package as most_recent_stream_choice_package_change
,internet_price as most_recent_internet_price_change       
,video_price as most_recent_video_price_change          
,total_price as most_recent_total_price_change  
,CASE WHEN mso='L-BHN' THEN CONCAT_WS('::', 'L-BHN', billing_id)
      WHEN mso='L-CHTR' THEN CONCAT_WS('::', 'L-CHTR', billing_id)
      WHEN mso='L-TWC' THEN CONCAT_WS('::', 'L-TWC', system__sys, billing_id) --billing_division is system__sys
      ELSE billing_id END as account_guid
    from dev.ml_account_changes_v1   
) a
INNER JOIN
(select
billing_id
,mso
,system__sys
,account__number_aes
,sum(internet_package) as package_internet_add_drop  --should sum to 1 or 0 but doesn't, Yizhe look into
,count(internet_package) as package_internet_changes
,sum(case when internet_package = 1 then 1 else 0 end) as package_internet_adds
,sum(case when internet_package = -1 then 1 else 0 end) as package_internet_drop

,sum(video_package) as package_video_add_drop
,count(video_package) as package_video_changes
,sum(case when video_package = 1 then 1 else 0 end) as package_video_adds
,sum(case when video_package = -1 then 1 else 0 end) as package_video_drop

,sum(other_package) as package_other_add_drop
,count(other_package) as package_other_changes
,sum(case when other_package = 1 then 1 else 0 end) as package_other_adds
,sum(case when other_package = -1 then 1 else 0 end) as package_other_drop

,sum(tv_stream_package) as package_tv_stream_add_drop
,count(tv_stream_package) as package_tv_stream_changes
,sum(case when tv_stream_package = 1 then 1 else 0 end) as package_tv_stream_adds
,sum(case when tv_stream_package = -1 then 1 else 0 end) as package_tv_stream_drop

,sum(stream_choice_package) as package_stream_choice_package_add_drop
,count(stream_choice_package) as package_stream_choice_package_changes
,sum(case when stream_choice_package = 1 then 1 else 0 end) as package_stream_choice_adds
,sum(case when stream_choice_package = -1 then 1 else 0 end) as package_stream_choice_drop

,sum(internet_price) as price_internet_increase_decrease
,count(internet_price) as price_internet_changes
,sum(case when internet_price = 1 then 1 else 0 end) as  internet_price_adds
,sum(case when internet_price = -1 then 1 else 0 end) as internet_price_drop

,sum(video_price) as  price_video_increase_decrease
,count(video_price) as price_video_total_changes   --capture package upgrade, or promo ends...
,sum(case when video_price = 1 then 1 else 0 end) as  video_price_adds
,sum(case when video_price = -1 then 1 else 0 end) as video_price_drop

,sum(total_price) as price_total_increase_decrease
,count(total_price) as price_total_changes
,sum(case when total_price = 1 then 1 else 0 end) as  total_price_adds
,sum(case when total_price = -1 then 1 else 0 end) as total_price_drop

,max(change_event_date) as max_date
,count(total_price*100)/count(change_event_date) as total_count_int
from dev.ml_account_changes_v1
group by billing_id, mso, system__sys, account__number_aes) b
on a.billing_id=b.billing_id and a.mso=b.mso and a.system__sys=b.system__sys and 
a.account__number_aes=b.account__number_aes and a.change_event_date=b.max_date

CROSS JOIN

(select
  PERCENTILE(CAST(ROUND(total_count*100) as int), array(0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1)) as percentiles
  ,stddev_pop(total_count*100) as std_dev
  ,avg(total_count*100) as avg_price
  from 
  (select count(total_price)/count(change_event_date) as total_count  --compares to package from previous price change, one row per cust per day
    from dev.ml_account_changes_v1
    where change_event_date <= '${hiveconf:current_date}'  --CHANGE DATE this will be removed but since churn label is from 2018 Jan31 don't pull price after------------------
    group by billing_id,account__number_aes,system__sys,mso) c
  ) d 
  group by account_guid 
  ,most_recent_internet_package_change
,most_recent_video_package_change
,most_recent_other_package_change
,most_recent_tv_stream_package_change
,most_recent_stream_choice_package_change
,most_recent_internet_price_change
,most_recent_video_price_change
,most_recent_total_price_change
,package_internet_add_drop
,package_internet_changes
,package_internet_adds
,package_internet_drop
,package_video_add_drop
,package_video_changes
,total_count_int
,package_tv_stream_add_drop
,package_tv_stream_changes
,package_stream_choice_package_add_drop
,package_stream_choice_package_changes
,price_internet_increase_decrease
,price_internet_changes
,price_video_increase_decrease
,price_video_total_changes
,price_total_increase_decrease
,price_total_changes
,package_other_add_drop
,package_other_changes
,percentiles[1]
,percentiles[2]
,percentiles[3]
,percentiles[4]
,percentiles[5]
,percentiles[6]
,percentiles[7]
,percentiles[8]
,percentiles[9]
,total_price_adds
,total_price_drop
,package_other_adds
,package_other_drop
,package_tv_stream_adds
,package_tv_stream_drop
,package_video_adds
,package_video_drop
,package_stream_choice_adds
,package_stream_choice_drop
,internet_price_adds
,internet_price_drop
,video_price_adds
,video_price_drop;



drop table if exists dev.churn_call_center_all_train;
create table dev.churn_call_center_all_train as select
sum(total_calls) as total_calls
,datediff(max(denver_date),min(denver_date)) as call_tenure
,sum(same_day_calls) as total_same_day_calls
,count(same_day_calls) as count_days_when_called
,min(time_btw_install_and_first_call) as time_from_install_to_first_call
,avg(avg_int_date) as avg_call_date
,dec_account_guid
from
 (select
   AVG(CAST(REGEXP_REPLACE(denver_date,'-','') as INT)) as avg_int_date
  ,COUNT(denver_date) as total_calls
  ,MIN(daydiff) as time_btw_install_and_first_call
  ,CASE WHEN COUNT(denver_date)>=2 THEN 1 ELSE 0 END as same_day_calls
  ,denver_date
  ,dec_account_guid
  from
  (select
    account_guid
  ,churn_date
  from dev.churn_labelJan31expanded
  ) a

  LEFT JOIN

    (select
    denver_date    --can call in multiple times on same day which would produce dups
        ,daydiff           --daydiff is difference between install date and call date                           
        ,CASE companycode
          when 'BHN' THEN CONCAT('L-',CONCAT_WS('::',companycode,prod.aes_encrypt(subaccountno)))
          WHEN 'CHARTER' THEN CONCAT('L-',CONCAT_WS('::','CHTR',prod.aes_encrypt(subaccountno)))
          WHEN 'TWC' THEN CONCAT('L-',CONCAT_WS('::',companycode,system__sys,billing_id)) END as dec_account_guid
    from
    (select denver_date    
        ,daydiff                                      
        ,prod.aes_encrypt(subaccountno) as account__number_aes
        ,companycode
        ,subaccountno
    from dev.venona_tv_stream_call_volume) b 
    
    LEFT JOIN

    (select
    system__sys
    ,prod.aes_encrypt(CASE WHEN LENGTH(prod.aes_decrypt(account__number_aes)) < 9 THEN LPAD(prod.aes_decrypt(account__number_aes), 9, '0') ELSE prod.aes_decrypt(account__number_aes) END) AS billing_id 
    ,account__number_aes 
    from prod.twc_account_history) e
     on b.account__number_aes = e.account__number_aes) n
  on a.account_guid=n.dec_account_guid
  where denver_date<churn_date            --Exlude calls on or after churn date           
    group by denver_date, dec_account_guid) c 
 group by dec_account_guid;



drop table if exists dev.mos_acct_train;
create table dev.mos_acct_train as select
account_guid
,count(distinct denver_date) as active_day_cnt
,PERCENTILE(CAST(ROUND(mos_score*100) as INT) ,array(0.1)) as mos_bottom_10_percentile
,PERCENTILE(CAST(ROUND(mos_score*100) as INT) ,array(0.9)) as mos_top_10_percentile
,case when stddev_pop(mos_score*100) is NULL then 0 else stddev_pop(mos_score) end as mos_std_dev_acct
,case when avg(mos_score) is NULL then 0 else avg(mos_score) end as avg_mos_acct

--weighted by watch time
,case when avg(mos_score*watch_time_ms) is NULL then 0 else avg(mos_score*watch_time_ms) end as weighted_avg_mos_acct
,case when stddev_pop(mos_score*watch_time_ms) is NULL then 0 else stddev_pop(mos_score*watch_time_ms) end as weighted_std_dev_acct
from dev.venona_churn_active_train
where denver_date between min_agg_date AND max_agg_date  --CHANGE DATE, for current date hiveconf:current_date and current_timestamp
group by account_guid;


drop table if exists dev.mos_acct_total_train;
create table dev.mos_acct_total_train as
select account_guid
  	 ,sum(case when avg_mos_score_acct > avg_mos_score then 1 else 0 end)/count(distinct a.denver_date) as mos_comp_cnt_acct_bigger
  	 ,sum(sum_mos_acct - sum_mos)/count(distinct a.denver_date) as mos_comp_avg_acct_bigger 
  	 ,sqrt(sum(power(2,avg_mos_score_acct - avg_mos_score))/count(avg_mos_score_acct)) as mos_comp_std_dev_acct --std dev of account mean compared to overall mean
  	 ,sum(case when avg_mos_score_acct<top_bottom_10[1] then 1 else 0 end) as comp_avg_mos_bottom_10 --avg<bottom 10 percentile
  	 ,sum(case when avg_mos_score_acct>top_bottom_10[2] then 1 else 0 end) as comp_avg_mos_top_10   --avg>top 90 percentile
      	
  	--weigted by watch time
  	 ,sum(weighted_avg_mos_acct)/count(a.stream_id) as comp_weighted_avg_mos_acct 
  	 ,sum(case when weighted_avg_mos_acct>weighted_avg_mos then 1 else 0 end)/count(distinct a.denver_date) as mos_comp_weighted_cnt_acct_bigger
  	 ,sum(weighted_avg_mos_acct - weighted_avg_mos)/count(distinct a.denver_date) as mos_comp_weighted_avg_acct_bigger 
  	 ,sqrt(sum(power(2,weighted_avg_mos_acct - weighted_avg_mos))/count(weighted_avg_mos)) as mos_comp_weighted_std_dev_acct       

  		from 																												       
  		(select
  			avg(mos_score) as avg_mos_score_acct
  			,sum(mos_score) as sum_mos_acct
  			--,sum(watch_time_ms) as sum_watch_time_acct 
  		--weigted by watch time
  			,avg(mos_score*watch_time_ms) as weighted_avg_mos_acct
  			,denver_date
  			,account_guid
  			,stream_id
  			from dev.venona_churn_active_train
  			where denver_date between min_agg_date AND max_agg_date      --CHANGE DATE
  			group by account_guid, stream_id, denver_date
  			) a
  		LEFT JOIN
  			(select
  			 avg(mos_score) as avg_mos_score
  			,avg(mos_score*watch_time_ms) as weighted_avg_mos
  			,sum(mos_score) as sum_mos
  			,denver_date
  			,stream_id
  			,PERCENTILE(CAST(ROUND(mos_score) as INT), array(0.1, 0.9)) as top_bottom_10
  			from dev.venona_churn_active_train
  			where denver_date >= min_agg_date --CHANGE DATE don't need denver date below min agg date  
  			group by stream_id,denver_date
  				) b
  	on a.stream_id=b.stream_id and a.denver_date=b.denver_date
  	group by account_guid;


drop table if exists dev.mos_acct_total_channel_number_train;
create table dev.mos_acct_total_channel_number_train as
select account_guid
  	 ,sum(case when avg_mos_score_acct > avg_mos_score then 1 else 0 end)/count(distinct a.denver_date) as mos_comp_channel_cnt_acct_bigger
  	 ,sum(sum_mos_acct - sum_mos)/count(distinct a.denver_date) as mos_comp_channel_avg_acct_bigger 
  	 ,sqrt(sum(power(2,avg_mos_score_acct - avg_mos_score))/count(avg_mos_score_acct)) as mos_comp_channel_std_dev_acct --std dev of account mean compared to overall mean
  	 ,sum(case when avg_mos_score_acct<=top_bottom_10[1] then 1 else 0 end) as mos_comp_channel_avg_mos_bottom_10 --avg<bottom 10 percentile
  	 ,sum(case when avg_mos_score_acct>=top_bottom_10[2] then 1 else 0 end) as mos_comp_channel_avg_mos_top_10   --avg>top 90 percentile
      	
  	--weigted by watch time
  	 ,sum(weighted_avg_mos_acct)/count(a.linear_channel_number) as mos_comp_channel_weighted_avg_mos_acct --by linear_channel_number instead of stream_id
  	 ,sum(case when weighted_avg_mos_acct>weighted_avg_mos then 1 else 0 end)/count(distinct a.denver_date) as mos_comp_channel_weighted_cnt_acct_bigger
  	 ,sum(weighted_avg_mos_acct - weighted_avg_mos)/count(distinct a.denver_date) as mos_comp_channel_weighted_avg_acct_bigger 
  	 ,sqrt(sum(power(2,weighted_avg_mos_acct - weighted_avg_mos))/count(weighted_avg_mos)) as mos_comp_channel_weighted_std_dev_acct       

  		from 																												       
  		(select
  			avg(mos_score) as avg_mos_score_acct
  			,sum(mos_score) as sum_mos_acct
  			 
  		--weigted by watch time
  			,avg(mos_score*watch_time_ms) as weighted_avg_mos_acct
  			,denver_date
  			,account_guid
  			,linear_channel_number
  			from dev.venona_churn_active_train
  			where denver_date between min_agg_date AND max_agg_date   --CHANGE DATE
  			group by account_guid, linear_channel_number, denver_date
  			) a
  		LEFT JOIN
  			(select
  			 avg(mos_score) as avg_mos_score
  			,avg(mos_score*watch_time_ms) as weighted_avg_mos
  			,sum(mos_score) as sum_mos
  			,denver_date
  			,linear_channel_number
  			,PERCENTILE(CAST(ROUND(mos_score) as INT), array(0.1, 0.9)) as top_bottom_10
  			from dev.venona_churn_active_train
  			where denver_date >= min_agg_date        --CHANGE DATE
  			group by linear_channel_number,denver_date
  				) b
  	on a.linear_channel_number=b.linear_channel_number and a.denver_date=b.denver_date
  	group by account_guid;


drop table if exists dev.mos_acct_week_part_train;
create table dev.mos_acct_week_part_train as
select 
	account_guid
  	 ,sum(case when avg_mos_score_acct > avg_mos_score then 1 else 0 end)/sum(active_day_cnt) as mos_comp_week_part_cnt_acct_bigger
  	 ,sum(sum_mos_acct - sum_mos)/sum(active_day_cnt) as mos_comp_week_part_avg_acct_bigger 
  	 --new std dev calc below     --std dev of account mean compared to overall mean
  	 ,case when count(avg_mos_score_acct)=0 then NULL else sqrt(sum(power(2,avg_mos_score_acct - avg_mos_score))/count(avg_mos_score_acct)) end as mos_comp_week_part_std_dev_acct
  	 ,sum(case when avg_mos_score_acct<=top_bottom_10[1] then 1 else 0 end) as mos_comp_week_part_channel_avg_mos_bottom_10 --avg<bottom 10 percentile
  	 ,sum(case when avg_mos_score_acct>=top_bottom_10[2] then 1 else 0 end) as mos_comp_week_part_avg_mos_top_10   --avg>top 90 percentile
      	
  	--weigted by watch time
  	 ,sum(weighted_avg_mos_acct)/count(b.week_part) as mos_comp_week_part_weighted_avg_mos_acct --by linear_channel_number instead of stream_id
  	 ,sum(case when weighted_avg_mos_acct>weighted_avg_mos then 1 else 0 end)/sum(active_day_cnt) as mos_comp_week_part_weighted_cnt_acct_bigger
  	 ,sum(weighted_avg_mos_acct - weighted_avg_mos)/sum(active_day_cnt) as mos_comp_week_part_weighted_avg_acct_bigger 
  	 --new std dev calc below     --std dev of account mean compared to overall mean
  	 ,case when count(weighted_avg_mos)=0 then NULL else sqrt(sum(power(2,weighted_avg_mos_acct - weighted_avg_mos))/count(weighted_avg_mos)) end as mos_comp_week_part_weighted_std_dev_acct

 from
(select
			avg(mos_score) as avg_mos_score_acct
  			,sum(mos_score) as sum_mos_acct
  			,count(distinct denver_date) as active_day_cnt
  		--weigted by watch time
  			,avg(mos_score*watch_time_ms) as weighted_avg_mos_acct
  			,account_guid
  			,week_part
  			,day_part
	from
		(select * 
	   	,CASE
	   		when from_unixtime(CAST(IF(length(start_timestamp) < 13, rpad(start_timestamp, 13, "0"), start_timestamp)/1000 as BIGINT), 'HH') BETWEEN  2 AND 5 THEN 'Overnight'
	   		when from_unixtime(CAST(IF(length(start_timestamp) < 13, rpad(start_timestamp, 13, "0"), start_timestamp)/1000 as BIGINT), 'HH') BETWEEN  2 AND 5 THEN 'Breakfast'
	   		when from_unixtime(CAST(IF(length(start_timestamp) < 13, rpad(start_timestamp, 13, "0"), start_timestamp)/1000 as BIGINT), 'HH') BETWEEN  2 AND 5 THEN 'Daytime'
	   		when from_unixtime(CAST(IF(length(start_timestamp) < 13, rpad(start_timestamp, 13, "0"), start_timestamp)/1000 as BIGINT), 'HH') BETWEEN  2 AND 5 THEN 'PrimeTime'
	   		ELSE 'late_night'
	   		end as day_part 
	   	,CASE
	   		when from_unixtime(CAST(IF(length(start_timestamp) < 13, rpad(start_timestamp, 13, "0"), start_timestamp)/1000 as BIGINT), 'u') BETWEEN  1 AND 5 THEN 'week_day'
	   		ELSE 'weekend'
	   		end as week_part
		from dev.venona_churn_active_train
		where denver_date between min_agg_date AND max_agg_date) a    --CHANGE DATE
group by account_guid,week_part,day_part) b
LEFT JOIN
(select
  	avg(mos_score) as avg_mos_score
  	,avg(mos_score*watch_time_ms) as weighted_avg_mos
  	,sum(mos_score) as sum_mos
  	,week_part
  	,day_part
  	,PERCENTILE(CAST(ROUND(mos_score) as INT), array(0.1, 0.9)) as top_bottom_10
	from
		(select * 
			,CASE
	   		when from_unixtime(CAST(IF(length(start_timestamp) < 13, rpad(start_timestamp, 13, "0"), start_timestamp)/1000 as BIGINT), 'HH') BETWEEN  2 AND 5 THEN 'Overnight'
	   		when from_unixtime(CAST(IF(length(start_timestamp) < 13, rpad(start_timestamp, 13, "0"), start_timestamp)/1000 as BIGINT), 'HH') BETWEEN  2 AND 5 THEN 'Breakfast'
	   		when from_unixtime(CAST(IF(length(start_timestamp) < 13, rpad(start_timestamp, 13, "0"), start_timestamp)/1000 as BIGINT), 'HH') BETWEEN  2 AND 5 THEN 'Daytime'
	   		when from_unixtime(CAST(IF(length(start_timestamp) < 13, rpad(start_timestamp, 13, "0"), start_timestamp)/1000 as BIGINT), 'HH') BETWEEN  2 AND 5 THEN 'PrimeTime'
	   		ELSE 'late_night'
	   		end as day_part
	   		,CASE
	   		when from_unixtime(CAST(IF(length(start_timestamp) < 13, rpad(start_timestamp, 13, "0"), start_timestamp)/1000 as BIGINT), 'u') BETWEEN  1 AND 5 THEN 'week_day'
	   		ELSE 'weekend'
	   		end as week_part
		from dev.venona_churn_active_train
		where denver_date >= min_agg_date) c   --CHANGE DATE
group by week_part,day_part) d
on b.week_part=d.week_part and b.day_part=d.day_part
group by account_guid;



drop table if exists dev.mos_acct_day_part_train;
create table dev.mos_acct_day_part_train as
select 
	account_guid
  	 ,sum(case when avg_mos_score_acct > avg_mos_score then 1 else 0 end)/sum(active_day_cnt) as mos_comp_day_part_cnt_acct_bigger
  	 ,sum(sum_mos_acct - sum_mos)/sum(active_day_cnt) as mos_comp_day_part_avg_acct_bigger 
  	 --new std dev below   --std dev of account mean compared to overall mean
  	 ,case when count(avg_mos_score_acct)=0 then NULL else sqrt(sum(power(2,avg_mos_score_acct - avg_mos_score))/count(avg_mos_score_acct)) end as mos_comp_day_part_std_dev_acct
  	 ,sum(case when avg_mos_score_acct<=top_bottom_10[1] then 1 else 0 end) as mos_comp_day_part_channel_avg_mos_bottom_10 --avg<bottom 10 percentile
  	 ,sum(case when avg_mos_score_acct>=top_bottom_10[2] then 1 else 0 end) as mos_comp_day_part_avg_mos_top_10   --avg>top 10 percentile
      	
  	--weighted by watch time
  	 ,sum(weighted_avg_mos_acct)/count(distinct b.denver_date) as comp_day_part_weighted_avg_mos_acct --by linear_channel_number instead of stream_id
  	 ,sum(case when weighted_avg_mos_acct>weighted_avg_mos then 1 else 0 end)/sum(active_day_cnt) as mos_comp_day_part_weighted_cnt_acct_bigger
  	 ,sum(weighted_avg_mos_acct - weighted_avg_mos)/sum(active_day_cnt) as mos_comp_day_part_weighted_avg_acct_bigger 
  	 --new std dev below    --std dev of account mean compared to overall mean
  	 ,case when count(weighted_avg_mos)=0 then NULL else sqrt(sum(power(2,weighted_avg_mos_acct - weighted_avg_mos))/count(weighted_avg_mos)) end as mos_comp_day_part_weighted_std_dev_acct

 from
(select
			avg(mos_score) as avg_mos_score_acct
  			,sum(mos_score) as sum_mos_acct
  			,count(distinct denver_date) as active_day_cnt
  		--weigted by watch time
  			,avg(mos_score*watch_time_ms) as weighted_avg_mos_acct
  			,account_guid
  			,denver_date
  			,day_part
	from
		(select * 
	   	,CASE
	   		when from_unixtime(CAST(IF(length(start_timestamp) < 13, rpad(start_timestamp, 13, "0"), start_timestamp)/1000 as BIGINT), 'HH') BETWEEN  2 AND 5 THEN 'Overnight'
	   		when from_unixtime(CAST(IF(length(start_timestamp) < 13, rpad(start_timestamp, 13, "0"), start_timestamp)/1000 as BIGINT), 'HH') BETWEEN  2 AND 5 THEN 'Breakfast'
	   		when from_unixtime(CAST(IF(length(start_timestamp) < 13, rpad(start_timestamp, 13, "0"), start_timestamp)/1000 as BIGINT), 'HH') BETWEEN  2 AND 5 THEN 'Daytime'
	   		when from_unixtime(CAST(IF(length(start_timestamp) < 13, rpad(start_timestamp, 13, "0"), start_timestamp)/1000 as BIGINT), 'HH') BETWEEN  2 AND 5 THEN 'PrimeTime'
	   		ELSE 'late_night'
	   		end as day_part 
	   	from dev.venona_churn_active_train
		where denver_date BETWEEN min_agg_date AND max_agg_date) a   --CHANG DATE
group by account_guid,denver_date,day_part) b
LEFT JOIN
(select
  	 avg(mos_score) as avg_mos_score
  	,avg(mos_score*watch_time_ms) as weighted_avg_mos
  	,sum(mos_score) as sum_mos
  	,day_part
  	,denver_date
  	,PERCENTILE(CAST(ROUND(mos_score) as INT), array(0.1, 0.9)) as top_bottom_10
	from
		(select * 
			,CASE
	   		when from_unixtime(CAST(IF(length(start_timestamp) < 13, rpad(start_timestamp, 13, "0"), start_timestamp)/1000 as BIGINT), 'HH') BETWEEN  2 AND 5 THEN 'Overnight'
	   		when from_unixtime(CAST(IF(length(start_timestamp) < 13, rpad(start_timestamp, 13, "0"), start_timestamp)/1000 as BIGINT), 'HH') BETWEEN  2 AND 5 THEN 'Breakfast'
	   		when from_unixtime(CAST(IF(length(start_timestamp) < 13, rpad(start_timestamp, 13, "0"), start_timestamp)/1000 as BIGINT), 'HH') BETWEEN  2 AND 5 THEN 'Daytime'
	   		when from_unixtime(CAST(IF(length(start_timestamp) < 13, rpad(start_timestamp, 13, "0"), start_timestamp)/1000 as BIGINT), 'HH') BETWEEN  2 AND 5 THEN 'PrimeTime'
	   		ELSE 'late_night'
	   		end as day_part
	   	from dev.venona_churn_active_train
		where denver_date >= min_agg_date) c   --CHANGE DATE
group by denver_date,day_part) d
on b.denver_date=d.denver_date and b.day_part=d.day_part
group by account_guid;


drop table if exists dev.mos_variables_90day_train;
create table dev.mos_variables_90day_train as
select 
--dev.mos_acct_total_train
a.account_guid as acct_guid
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
,b.active_day_cnt
,b.mos_bottom_10_percentile
,b.mos_top_10_percentile
,b.mos_std_dev_acct
,b.avg_mos_acct
,b.weighted_avg_mos_acct
,b.weighted_std_dev_acct

--dev.mos_acct_total_channel_number_train
,c.mos_comp_channel_cnt_acct_bigger
,c.mos_comp_channel_avg_acct_bigger 
,c.mos_comp_channel_std_dev_acct 
,c.mos_comp_channel_avg_mos_bottom_10 
,c.mos_comp_channel_avg_mos_top_10   
,c.mos_comp_channel_weighted_avg_mos_acct 
,c.mos_comp_channel_weighted_cnt_acct_bigger
,c.mos_comp_channel_weighted_avg_acct_bigger 
,c.mos_comp_channel_weighted_std_dev_acct 

--dev.mos_acct_week_part_train
,d.mos_comp_week_part_cnt_acct_bigger
,d.mos_comp_week_part_avg_acct_bigger 
,d.mos_comp_week_part_std_dev_acct 
,d.mos_comp_week_part_channel_avg_mos_bottom_10 
,d.mos_comp_week_part_avg_mos_top_10   
,d.mos_comp_week_part_weighted_avg_mos_acct 
,d.mos_comp_week_part_weighted_cnt_acct_bigger
,d.mos_comp_week_part_weighted_avg_acct_bigger 
,d.mos_comp_week_part_weighted_std_dev_acct 

--dev.mos_acct_day_part_train
,e.mos_comp_day_part_cnt_acct_bigger
,e.mos_comp_day_part_avg_acct_bigger 
,e.mos_comp_day_part_std_dev_acct 
,e.mos_comp_day_part_channel_avg_mos_bottom_10 
,e.mos_comp_day_part_avg_mos_top_10   
,e.comp_day_part_weighted_avg_mos_acct 
,e.mos_comp_day_part_weighted_cnt_acct_bigger
,e.mos_comp_day_part_weighted_avg_acct_bigger 
,e.mos_comp_day_part_weighted_std_dev_acct 
from dev.mos_acct_total_train a FULL OUTER JOIN  dev.mos_acct_train b on a.account_guid=b.account_guid FULL OUTER JOIN dev.mos_acct_total_channel_number_train c on b.account_guid=c.account_guid 
FULL OUTER JOIN dev.mos_acct_week_part_train d on c.account_guid=d.account_guid FULL OUTER JOIN dev.mos_acct_day_part_train e on d.account_guid=e.account_guid;


--VENONA VARS
drop table if exists dev.venona_main_churn_train;
create table dev.venona_main_churn_train as
select                
sum(case when content_genres RLIKE '.*football.*|.*basketball.*|.*baseball.*|.*soccer.*' OR provider_asset_id RLIKE 'nbcolympics.*|nbcsports.*|espn.*|nfl.*|foxdeportes.*|foxsports1.*|beinsport.*' then 1 else 0 end) as sum_sports_flag
,avg(case when content_genres RLIKE '.*football.*|.*basketball.*|.*baseball.*|.*soccer.*' OR provider_asset_id RLIKE 'nbcolympics.*|nbcsports.*|espn.*|nfl.*|foxdeportes.*|foxsports1.*|beinsport.*' then 1 else 0 end) as avg_sports_flag
,avg(case when linear_network_name RLIKE '.*hbo.*|.*showtime.*|.*starz.*|.*cinemax.*' OR provider_asset_id RLIKE 'sho.*|hbo.*|starzencore.*|cinemax.*' then 1 else 0 end) as avg_prem_flag
,sum(case when linear_network_name RLIKE '.*hbo.*|.*showtime.*|.*starz.*|.*cinemax.*' OR provider_asset_id RLIKE 'sho.*|hbo.*|starzencore.*|cinemax.*' then 1 else 0 end) as sum_prem_flag 
,avg(case when cast(split(screen_resolution,'x')[0] as int)>720 OR cast(split(screen_resolution,'x')[1] as int)>720 then 1 else 0 end) as avg_hd_flag
,sum(case when cast(split(screen_resolution,'x')[0] as int)>720 OR cast(split(screen_resolution,'x')[1] as int)>720 then 1 else 0 end) as sum_hd_flag
,sum(case when device_type='iphone' then watch_time_ms else 0 end) as iphone_device_watch_time
,sum(case when device_type='androidphone' then watch_time_ms else 0 end) as androidphone_device_watch_time
,sum(case when device_type='uwp' then watch_time_ms else 0 end) as uwp_device_watch_time  --universal windows platform, new windows 10
,sum(case when device_type='samsungtv' then watch_time_ms else 0 end) as samsungtv_device_watch_time
,sum(case when device_type='androidtablet' then watch_time_ms else 0 end) as androidtablet_device_watch_time
,sum(case when device_type='webbrowser' then watch_time_ms else 0 end) as webbrowser_device_watch_time
,sum(case when device_type='ipad' then watch_time_ms else 0 end) as ipad_device_watch_time
,sum(case when device_type='ipod touch' then watch_time_ms else 0 end) as ipod_device_watch_time
,sum(case when device_type='xbox' then watch_time_ms else 0 end) as xbox_device_watch_time
,sum(case when device_type='roku' then watch_time_ms else 0 end) as roku_device_watch_time
,sum(watch_time_ms)/sum(level2_downshifts) as watchtime10_L2DownShift
,sum(mos_score*watch_time_on_net_ms)/count(billing_id) as avg_mos_x_time
,AVG(rental_duration_hours) as avg_rental_duration_hours   
,AVG(content_available_date) as avg_content_available_date 	  
,AVG(content_runtime) as  avg_content_runtime     	                 	                    
,AVG(content_expiration_date) as  avg_content_expiration_date	              	                    
,AVG(content_original_air_date) as  avg_content_original_air_date               	                                  	   
,AVG(content_year) as avg_content_year        	
,AVG(is_active) as  avg_is_active           	              	                    
,AVG(has_playback_start) as  avg_h_playback_start  	              	                    
,AVG(stream_init_starts) as avg_stream_init_starts  	              	                    
,AVG(stream_noninit_failures) as avg_stream_noninit_failures	              	                    
,AVG(stream_init_failures) as avg_stream_init_failures 	              	                    
,AVG(stream_failures) as avg_stream_failures     	              	                    
,AVG(stream_stops) as avg_stream_stops        	              	                    
,AVG(tune_time_ms) as avg_tune_time_ms       	              	                    
,AVG(watch_time_ms) as avg_watch_time_ms       	              	                    
,AVG(bandwidth_consumed_mb) as  avg_bandwidth_consumed_mbps	              	                    
,AVG(bitrate_content_elapsed_ms) as  avg_bitrate_content_elapsed_ms	              	                    
,AVG(buffering_events) as avg_buffering_events    	              	                    
,AVG(buffering_stops) as avg_buffering_stops     	              	                    
,AVG(buffering_duration_ms)	as avg_buffering_duration_ms	              	                    
,AVG(buffering_ratio) as avg_buffering_ratio     	              	                    
,AVG(buffering_score) as avg_buffering_score     	              	                    
,AVG(ad_breaks) as avg_ad_breaks           	              	                    
,AVG(ad_events) as avg_ad_events           	              	                    
,AVG(last_ad_sequence_number) as avg_last_ad_sequence_number	                 	                    
,AVG(last_ad_end_sequence_number) as avg_last_ad_end_sequence_number	   
,AVG(avg_bitrate_mbps) as avg2_bitrate_mbps    	              	                    
,AVG(bitrate_upshifts) as avg_bitrate_upshifts    	              	                    
,AVG(bitrate_downshifts) as avg_bitrate_downshifts  	              	                                	          	
,AVG(downshifts_score) as avg_downshifts_score    	              	                    
,AVG(start_timestamp) as avg_start_timestamp     	              	                    
,AVG(end_timestamp) as avg_end_timestamp       	              	                    
,AVG(last_stop_sequence_number)	as avg_lt_stop_sequence_number	              	                    
,AVG(last_stop_timestamp) as avg_lt_stop_timestamp 	              	                         	            	                                     
,AVG(level2_downshifts) as avg_level2_downshifts   	                 	
,AVG(pibbe_ms) as avg_pibbe_ms            	              	
,AVG(pibd2_ms) as avg_pibd2_ms            	              	
,AVG(mos_score) as avg_mos_score           	              	 
,AVG(watch_time_on_net_ms) as avg_watch_time_on_net_ms	              	                    
,AVG(watch_time_off_net_ms) as avg_watch_time_off_net_ms	              	                                 	                    
,sum(stream_failures) as sum_stream_failures
,sum(watch_time_off_net_ms)/sum(watch_time_ms) as pctn_watch_time_off_net_ms
,max(denver_date) as max_denver_date
,count(distinct denver_date)/60 as dist_date_percent   --CHANGE DATE
,count(distinct linear_channel_name) as dist_linear_channel_name
,count(distinct linear_network_name) as dist_linear_network_name
,count(distinct linear_channel_category) as dist_linear_channel_category
,count(distinct device_type) as distinct_device_cnt
,sum(case when playback_type='linear' then 1 else 0 end) as sum_linear
,sum(case when playback_type='vod' then 1 else 0 end) as sum_vod
,max(case when playback_type='linear' then denver_date else NULL end) as max_linear_denver_date
,max(case when playback_type='vod' then denver_date else NULL end) as max_vod_denver_date
,sum(case when user_cancels_stream=TRUE then 1 else 0 end) as sum_user_stream_cancel
,sum(case when user_cancels_stream=FALSE then 1 else 0 end) as sum_user_stream_NOT_cancel
,count(denver_date) as total_stream_count_date
,account_guid
from
dev.venona_churn_active_train
  where denver_date BETWEEN min_agg_date AND max_agg_date    --CHANGE DATE 
  group by account_guid;




drop table if exists dev.venona_top_channel_train;
create table dev.venona_top_channel_train as select 
b.account_guid as account_guid_tchannel
,b.top_channel as top_channel_watch_time
,b.channel_watch_time 

from 
   (select 
     	a.*
     	,row_number() over (partition by a.account_guid order by a.channel_watch_time desc) as rn
     	,a.tms_guide_id as top_channel   --tms_guide_id
       from
	      (select
   		    	account_guid
            	,tms_guide_id
           		,sum(watch_time_ms) as channel_watch_time
            from dev.venona_churn_active_train
           where playback_type='linear' and tms_guide_id is not NULL and denver_date BETWEEN min_agg_date AND max_agg_date  --CHANGE DATE
           group by account_guid, tms_guide_id) a
    ) b  
where b.rn=1;

