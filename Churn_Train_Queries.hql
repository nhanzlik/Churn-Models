drop table if exists dev.venona_stream_agg_churn;
create table dev.venona_stream_agg_churn as select * ,
CASE mso
              WHEN 'L-BHN' THEN CONCAT_WS('::', 'L-BHN', billing_id)
              WHEN 'L-CHTR' THEN CONCAT_WS('::', 'L-CHTR', billing_id)
              WHEN 'L-TWC' THEN CONCAT_WS('::', 'L-TWC', billing_division, billing_id)
          ELSE billing_id END AS account_guid
from dev.venona_stream_agg_partition;


drop table if exists dev.churn_active_accounts;
create table dev.churn_active_accounts as
SELECT
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
AND partition_date_time = '${hiveconf:current_date}'    --CHANGE DATE

UNION

SELECT 
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
AND partition_date = '${hiveconf:current_date}') b   --CHANGE DATE

UNION

SELECT
CONCAT_WS('::',mso,system__sys,billing_id) as account_guid --ADD system__sys  FOR TWC contruct acct_guid as mso,system__sys,billing_id
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
AND partition_date = '${hiveconf:current_date}') a;

--STOP THIS PART HAS TWO seperat queries
--Create final venona table, all tables created from this table
--Use LEFT OUTER JOIN to exclude accounts that are in train set so they are not included when calculating values in test set
   --this has an impact on percentile scoring
--TRAIN SET QUERY
drop table if exists dev.venona_churn_active;
create table dev.venona_churn_active AS
SELECT c.* 
FROM dev.venona_stream_agg_churn c
LEFT JOIN
(select b.* 
from  dev.churn_active_accounts b
    LEFT OUTER JOIN 
dev.churn_labelJan31expanded a  --Make sure accounts aren't in training set
ON a.account_guid=b.account_guid
  WHERE b.account_guid is NULL) a1
ON c.account_guid=a1.account_guid;


--USE QUERY BELOW FOR FUTURE RUNS
--When a test set is generated in the future and there is not a training set use the query below
--NO TRAINING SET QUERY
--drop table if exists dev.venona_churn_active;
--create table dev.venona_churn_active as 
--  SELECT a.* FROM dev.venona_stream_agg a 
--    LEFT SEMI JOIN dev.churn_active_accounts b ON a.account_guid=b.account_guid;


--PRICE ANALYSIS
--Two parts of this query-first part includes all price information, second part includes most recent price information
--most recent price info is only considered for percentile score
--SEE CHANGE DATE NOTE 

drop table if exists dev.churn_price;
create table dev.churn_price as   --variables may not capture when account is tv stream customer
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
    from prod.ml_account_changes_v1   --changed from dev.ml_account_changes_v1
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
from prod.ml_account_changes_v1  --dev.ml_account_changes_v2 os am update to dev.ml_account_changes_v2
group by billing_id, mso, system__sys, account__number_aes) b
on a.billing_id=b.billing_id and a.mso=b.mso and a.system__sys=b.system__sys and 
a.account__number_aes=b.account__number_aes and a.change_event_date=b.max_date

CROSS JOIN

(select
  PERCENTILE(CAST(ROUND(total_count*100) as int), array(0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1)) as percentiles
  ,stddev_pop(total_count*100) as std_dev
  ,avg(total_count*100) as avg_price
  from 
  (select count(total_price)/count(change_event_date) as total_count  --compares to pachage from previous price change, one row per cust per day
    from prod.ml_account_changes_v1
    where change_event_date<='${hiveconf:current_date}'  --CHANGE DATE remove for future runs------------------
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


--CALL ANALYSIS
--Include all data
drop table if exists dev.churn_call_center_all;
create table dev.churn_call_center_all as select
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
    denver_date    
        ,daydiff                                      
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
  where denver_date<='${hiveconf:current_date}'  --CHANGE DATE, remove for future runs                      
    group by denver_date, dec_account_guid) c 
 group by dec_account_guid;


--MOS ANALYSIS
--All date changes 60 days back from current date
drop table if exists dev.mos_acct;
create table dev.mos_acct as select
	 account_guid
	,count(distinct denver_date) as active_day_cnt
	,PERCENTILE(CAST(ROUND(mos_score*100) as INT) ,array(0.1)) as mos_bottom_10_percentile
	,PERCENTILE(CAST(ROUND(mos_score*100) as INT) ,array(0.9)) as mos_top_10_percentile
	,case when stddev_pop(mos_score*100) is NULL then 0 else stddev_pop(mos_score) end as mos_std_dev_acct
	,case when avg(mos_score) is NULL then 0 else avg(mos_score) end as avg_mos_acct

	--weighted by watch time
	,case when avg(mos_score*watch_time_ms) is NULL then 0 else avg(mos_score*watch_time_ms) end as weighted_avg_mos_acct
	,case when stddev_pop(mos_score*watch_time_ms) is NULL then 0 else stddev_pop(mos_score*watch_time_ms) end as weighted_std_dev_acct
from dev.venona_churn_active
where denver_date between '${hiveconf:min_date_agg}' and '${hiveconf:current_date}'  --CHANGE DATE
group by account_guid;


drop table if exists dev.mos_acct_total;
create table dev.mos_acct_total as
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
  			from dev.venona_churn_active
  			where denver_date between '${hiveconf:min_date_agg}' and '${hiveconf:current_date}'      --CHANGE DATE
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
  			from dev.venona_churn_active
  			where denver_date between '${hiveconf:min_date_agg}' and '${hiveconf:current_date}'   --CHANGE DATE  
  			group by stream_id,denver_date
  				) b
  	on a.stream_id=b.stream_id and a.denver_date=b.denver_date
  	group by account_guid;

drop table if exists dev.mos_acct_total_channel_number;
create table dev.mos_acct_total_channel_number as
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
  			from dev.venona_churn_active
  			where denver_date between '${hiveconf:min_date_agg}' and '${hiveconf:current_date}'   --CHANGE DATE
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
  			from dev.venona_churn_active
  			where denver_date between '${hiveconf:min_date_agg}' and '${hiveconf:current_date}'        --CHANGE DATE
  			group by linear_channel_number,denver_date
  				) b
  	on a.linear_channel_number=b.linear_channel_number and a.denver_date=b.denver_date
  	group by account_guid;


drop table if exists dev.mos_acct_week_part;
create table dev.mos_acct_week_part as
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
		from dev.venona_churn_active
		where denver_date between '${hiveconf:min_date_agg}' and '${hiveconf:current_date}') a    --CHANGE DATE
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
		from dev.venona_churn_active
		where denver_date between '${hiveconf:min_date_agg}' and '${hiveconf:current_date}') c   --CHANGE DATE
group by week_part,day_part) d
on b.week_part=d.week_part and b.day_part=d.day_part
group by account_guid;



drop table if exists dev.mos_acct_day_part;
create table dev.mos_acct_day_part as
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
	   	from dev.venona_churn_active
		where denver_date between '${hiveconf:min_date_agg}' and '${hiveconf:current_date}') a   --CHANGE DATE
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
	   	from dev.venona_churn_active
		where denver_date between '${hiveconf:min_date_agg}' and '${hiveconf:current_date}') c   --CHANGE DATE
group by denver_date,day_part) d
on b.denver_date=d.denver_date and b.day_part=d.day_part
group by account_guid;


drop table if exists dev.mos_variables_90day;
create table dev.mos_variables_90day as
select 
--dev.mos_acct_total
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

--dev.mos_acct
,b.active_day_cnt
,b.mos_bottom_10_percentile
,b.mos_top_10_percentile
,b.mos_std_dev_acct
,b.avg_mos_acct
,b.weighted_avg_mos_acct
,b.weighted_std_dev_acct

--dev.mos_acct_total_channel_number
,c.mos_comp_channel_cnt_acct_bigger
,c.mos_comp_channel_avg_acct_bigger 
,c.mos_comp_channel_std_dev_acct 
,c.mos_comp_channel_avg_mos_bottom_10 
,c.mos_comp_channel_avg_mos_top_10   
,c.mos_comp_channel_weighted_avg_mos_acct 
,c.mos_comp_channel_weighted_cnt_acct_bigger
,c.mos_comp_channel_weighted_avg_acct_bigger 
,c.mos_comp_channel_weighted_std_dev_acct 

--dev.mos_acct_week_part
,d.mos_comp_week_part_cnt_acct_bigger
,d.mos_comp_week_part_avg_acct_bigger 
,d.mos_comp_week_part_std_dev_acct 
,d.mos_comp_week_part_channel_avg_mos_bottom_10 
,d.mos_comp_week_part_avg_mos_top_10   
,d.mos_comp_week_part_weighted_avg_mos_acct 
,d.mos_comp_week_part_weighted_cnt_acct_bigger
,d.mos_comp_week_part_weighted_avg_acct_bigger 
,d.mos_comp_week_part_weighted_std_dev_acct 

--dev.mos_acct_day_part
,e.mos_comp_day_part_cnt_acct_bigger
,e.mos_comp_day_part_avg_acct_bigger 
,e.mos_comp_day_part_std_dev_acct 
,e.mos_comp_day_part_channel_avg_mos_bottom_10 
,e.mos_comp_day_part_avg_mos_top_10   
,e.comp_day_part_weighted_avg_mos_acct 
,e.mos_comp_day_part_weighted_cnt_acct_bigger
,e.mos_comp_day_part_weighted_avg_acct_bigger 
,e.mos_comp_day_part_weighted_std_dev_acct 
from dev.mos_acct_total a FULL OUTER JOIN  dev.mos_acct b on a.account_guid=b.account_guid FULL OUTER JOIN dev.mos_acct_total_channel_number c on b.account_guid=c.account_guid 
FULL OUTER JOIN dev.mos_acct_week_part d on c.account_guid=d.account_guid FULL OUTER JOIN dev.mos_acct_day_part e on d.account_guid=e.account_guid;


--VENONA VARs
drop table if exists dev.venona_main_churn;
create table dev.venona_main_churn as
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
,count(distinct denver_date)/(datediff('${hiveconf:current_date}','${hiveconf:min_date_agg}')+1) as dist_date_percent   --CHANGE DATE
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
dev.venona_churn_active
  where denver_date between '${hiveconf:min_date_agg}' and '${hiveconf:current_date}'   --CHANGE DATE  
  group by account_guid;



drop table if exists dev.venona_top_channel;
create table dev.venona_top_channel as select 
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
            from dev.venona_churn_active
           where playback_type='linear' and tms_guide_id is not NULL and denver_date between '${hiveconf:min_date_agg}' and '${hiveconf:current_date}'  --CHANGE DATE
           group by account_guid, tms_guide_id) a
    ) b  
where b.rn=1;



drop table if exists dev.venona_avg_std_channel;  
create table dev.venona_avg_std_channel as select
a.account_guid as account_guid_schannel
,avg(std_channel_watch) as avg_std_channel
from
(select
	account_guid
	,stddev_pop(watch_time_ms) as std_channel_watch
from dev.venona_churn_active
where playback_type='linear' and tms_guide_id is not NULL and denver_date between '${hiveconf:min_date_agg}' and '${hiveconf:current_date}'     --CHANGE DATE
group by account_guid, tms_guide_id, WEEKOFYEAR(denver_date)	) a
group by a.account_guid;


--DROP TABLE IF EXISTS dev.search_features;
--CREATE TABLE dev.search_features AS
--SELECT 
--       account_guid as account_guid_features,
--       COUNT(DISTINCT search_id) AS sum_unique_searches,
--       COUNT(DISTINCT search_id)/COUNT(DISTINCT visit_id) as avg_unique_searches,
--       SUM(IF(search_results = 0, 1, 0)) AS sum_search_entered_returned_no_results,
--       SUM(IF(search_results = 0, 1, 0))/COUNT(DISTINCT visit_id) AS avg_search_entered_returned_no_results,
--       SUM(IF(search_results > 0, 1, 0)) AS sum_search_entered_selected_results,
--       SUM(IF(search_results > 0, 1, 0))/COUNT(DISTINCT visit_id) AS avg_search_entered_selected_results,
--       SUM(search_response_time_ms) AS sum_search_response_time_ms,
--       SUM(search_response_time_ms)/COUNT(DISTINCT visit_id) AS avg_search_response_time_ms --not average search response
--       --SUM(search_response_time_ms)/SUM(search_entered) AS avg_search_response_time_ms
--FROM 
--(SELECT 
--      visit_id
--      ,search_id
--      ,operation_type
--      ,SPLIT(search_array, '\\|\\|') [2] AS search_results  
--      ,SPLIT(search_array, '\\|\\|') [3] AS search_response_time_ms
--      ,denver_date 
--      ,account_guid    
--FROM                                       
--  (SELECT CASE
--      WHEN mso='L-BHN' THEN CONCAT_WS('::', 'L-BHN', billing_id)
--      WHEN mso='L-CHTR' THEN CONCAT_WS('::', 'L-CHTR', billing_id)
--      WHEN mso='L-TWC' THEN CONCAT_WS('::', 'L-TWC', billing_division, billing_id) --billing_division is system__sys for twc
--      ELSE billing_id END as account_guid,
--      visit_id,
--      search_id,
--      operation_type,
--      search_array,
--      denver_date
--  FROM prod.venona_page_agg
--  LATERAL VIEW EXPLODE(search_text_list) explode_table AS search_array      
--  WHERE denver_date between '${hiveconf:min_date_agg}' and '${hiveconf:current_date}'                   --CHANGE DATE
--  AND operation_type IN ('searchEntered', 'searchResultSelected')     
--  AND search_id IS NOT NULL
--  AND search_array IS NOT NULL
--   ) b
--) c
--WHERE denver_date between '${hiveconf:min_date_agg}' and '${hiveconf:current_date}'      --CHANGE DATE
--GROUP BY account_guid;


drop table if exists dev.never_logged_in;
create table dev.never_logged_in as
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
      FROM  prod.venona_acct_agg    --test.venona_acct_agg_tmp
    ) metric
    RIGHT JOIN (
          SELECT DISTINCT mso, billing_id, system__sys  
          FROM prod.ml_account_changes_v1          
          --WHERE video_package_type = 'TV_STREAM'                
          WHERE tv_stream_package IS NOT NULL
            AND change_event_date BETWEEN '${hiveconf:min_date_agg}' and '${hiveconf:current_date}'  --CHANGE DATE
    ) acct                                    
    ON acct.billing_id = metric.billing_id and acct.mso = metric.mso and acct.system__sys = metric.billing_division
    WHERE metric.billing_id IS NULL AND metric.mso IS NULL and metric.billing_division IS NULL;


drop table if exists dev.venona_all_churnJan31_90;  
create table dev.venona_all_churnJan31_90 as select 
	 a.*												
	,b.*
	,c.*
	--,d.*
  ,e.never_logged_in as never_logged_in_flag
from dev.venona_main_churn a FULL OUTER dev.venona_top_channel b on (a.account_guid=b.account_guid_tchannel)
FULL OUTER dev.venona_avg_std_channel c on (b.account_guid_tchannel=c.account_guid_schannel) 
--FULL OUTER dev.search_features d on(c.account_guid_schannel=d.account_guid_features) 
FULL OUTER dev.never_logged_in e on (c.account_guid_schannel=e.account_guid_noLog);



--PROMO ANALYSIS
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
    AND partition_date = '${hiveconf:current_date}') d     --CHANGE DATE, restrict to only active customers------

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
      AND partition_date = '${hiveconf:current_date}'          --CHANGE DATE, restrict to only active customers------
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
    AND partition_date = '${hiveconf:current_date}') d    --CHANGE DATE, restrict to only active customers------
         
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
      AND partition_date = '${hiveconf:current_date}'     --CHANGE DATE, restrict to only active customers------
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
    AND partition_date_time = '${hiveconf:current_date}') d   --CHANGE DATE, restrict to only active customers------
         
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
      AND partition_date_time = '${hiveconf:current_date}'    --CHANGE DATE, restrict to only active customers------
    group by account__number_aes --Limit to only active accounts
      ) b
 on a.account__number_aes=b.account__number_aes and a.partition_date_time=b.max_date 
      group by a.account__number_aes, a.account__dwelling_description, a.promo_desc_most_recent) z
      on d.account__number_aes=z.account__number_aes 
group by z.account__number_aes, mso ,z.most_recent_promo_5percent_flag, z.most_recent_dwelling;

--COMBINE ALL VARS TOGETHER
drop table if exists dev.churn_all_vars_90day;
create table dev.churn_all_vars_90day as select
--dev.churn_active_accounts
f.account_guid
,f.mso
,f.system__sys
,f.billing_id
,f.account__number_aes
,'25' as week_number

-- TODO Need below from ml_account_changes_v2
--,tv_stream_tenure                                           
--,customer_tenure                                            
--,connect_date                                             
--,convert_date 

--dev.mos_acct_total
,a.mos_comp_cnt_acct_bigger
,a.mos_comp_avg_acct_bigger 
,a.mos_comp_std_dev_acct 
,a.comp_avg_mos_bottom_10 
,a.comp_avg_mos_top_10        
,a.comp_weighted_avg_mos_acct 
,a.mos_comp_weighted_cnt_acct_bigger
,a.mos_comp_weighted_avg_acct_bigger 
,a.mos_comp_weighted_std_dev_acct

--dev.mos_acct
,a.active_day_cnt
,a.mos_bottom_10_percentile
,a.mos_top_10_percentile
,a.mos_std_dev_acct
,a.avg_mos_acct
,a.weighted_avg_mos_acct
,a.weighted_std_dev_acct

--dev.mos_acct_total_channel_number
,a.mos_comp_channel_cnt_acct_bigger
,a.mos_comp_channel_avg_acct_bigger 
,a.mos_comp_channel_std_dev_acct 
,a.mos_comp_channel_avg_mos_bottom_10 
,a.mos_comp_channel_avg_mos_top_10   
,a.mos_comp_channel_weighted_avg_mos_acct 
,a.mos_comp_channel_weighted_cnt_acct_bigger
,a.mos_comp_channel_weighted_avg_acct_bigger 
,a.mos_comp_channel_weighted_std_dev_acct 

--dev.mos_acct_week_part
,a.mos_comp_week_part_cnt_acct_bigger
,a.mos_comp_week_part_avg_acct_bigger 
,a.mos_comp_week_part_std_dev_acct 
,a.mos_comp_week_part_channel_avg_mos_bottom_10 
,a.mos_comp_week_part_avg_mos_top_10   
,a.mos_comp_week_part_weighted_avg_mos_acct 
,a.mos_comp_week_part_weighted_cnt_acct_bigger
,a.mos_comp_week_part_weighted_avg_acct_bigger 
,a.mos_comp_week_part_weighted_std_dev_acct 

--dev.mos_acct_day_part
,a.mos_comp_day_part_cnt_acct_bigger
,a.mos_comp_day_part_avg_acct_bigger 
,a.mos_comp_day_part_std_dev_acct 
,a.mos_comp_day_part_channel_avg_mos_bottom_10 
,a.mos_comp_day_part_avg_mos_top_10   
,a.comp_day_part_weighted_avg_mos_acct 
,a.mos_comp_day_part_weighted_cnt_acct_bigger
,a.mos_comp_day_part_weighted_avg_acct_bigger 
,a.mos_comp_day_part_weighted_std_dev_acct 

--dev.venona_main_churn
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

--dev.venona_top_channel
,b.top_channel_watch_time                                           
,b.channel_watch_time                                                                          
                                      
--dev.search_features                                     
--,b.sum_unique_searches                                      
--,b.avg_unique_searches                                      
--,b.sum_search_entered_returned_no_results                                     
--,b.avg_search_entered_returned_no_results                                     
--,b.sum_search_entered_selected_results                                      
--,b.avg_search_entered_selected_results                                      
--,b.sum_search_response_time_ms                                      
--,b.avg_search_response_time_ms                                      
--,b.never_logged_in_flag 

--dev.churn_call_center_all
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
,e.account__number_aes
,e.mso
,e.count_zip_code_changes 
,e.sum_truck_trouble_call
,e.total_promo_5percent_flag
,e.most_recent_dwelling
,e.most_recent_promo_5percent_flag

from dev.churn_active_accounts f LEFT JOIN dev.mos_variables_90day a on f.account_guid=a.acct_guid 
LEFT JOIN
dev.venona_all_churnJan31_90 b on a.acct_guid=b.account_guid 
LEFT JOIN 
dev.churn_call_center_all c on b.account_guid=c.dec_account_guid 
LEFT JOIN
dev.churn_price d on c.dec_account_guid=d.account_guid
LEFT JOIN
dev.promo_account_history e on d.account_guid=e.account_guid;
