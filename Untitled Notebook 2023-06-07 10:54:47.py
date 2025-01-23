# Databricks notebook source
mhi_query = """
WITH magic_base AS
(SELECT mid AS merchant_id, DATE(live_date) AS live_date, sales_merchant_status, segment, CAST(IF(pre_magic_cr='',NULL,pre_magic_cr) AS DOUBLE)*100.0 AS pre_magic_cr_percentage,
CAST(IF(pre_magic_rto='',NULL,pre_magic_rto) AS DOUBLE)*100.0 AS pre_magic_rto_percentage
FROM batch_sheets.magic_merchant_list
WHERE lower(sales_merchant_status) = 'live'),

magic_first_producer_created_date AS
(SELECT d.merchant_id, date(live_date) AS live_date, min(date(producer_created_date)) AS modified_magic_first_producer_created_date
 FROM aggregate_pa.cx_1cc_events_dump_v1 d
 INNER JOIN magic_base m ON d.merchant_id = m.merchant_id
 WHERE lower(event_name) IN ('open') AND producer_created_date >= live_date
 GROUP BY 1,2),

magic_cr AS
(SELECT d.merchant_id,-- sales_merchant_status, segment, live_date,

COUNT(DISTINCT CASE WHEN (DATE_DIFF('day', modified_magic_first_producer_created_date, date(producer_created_date)) between 0 and 20) AND lower(event_name)='submit' THEN checkout_id ELSE NULL END)*100.00/NULLIF(COUNT(DISTINCT CASE WHEN (DATE_DIFF('day', modified_magic_first_producer_created_date, date(producer_created_date)) between 0 and 20) AND lower(event_name)='open' THEN checkout_id ELSE NULL END),0) AS magic_first_three_weeks_cr,

COUNT(DISTINCT CASE WHEN IF(pre_magic_cr_percentage IS NOT NULL, 1=1, DATE(producer_created_date) > date_add('day', 20, date(modified_magic_first_producer_created_date))) AND (DATE_DIFF('day', date(producer_created_date), NOW()) between 1 and 31) AND lower(event_name)='submit' THEN checkout_id ELSE NULL END)*100.00/NULLIF(COUNT(DISTINCT CASE WHEN IF(pre_magic_cr_percentage IS NOT NULL, 1=1, DATE(producer_created_date) > date_add('day', 20, date(modified_magic_first_producer_created_date))) AND (DATE_DIFF('day', date(producer_created_date), NOW()) between 1 and 31) AND lower(event_name)='open' THEN checkout_id ELSE NULL END),0) AS magic_cr_percentage

FROM aggregate_pa.cx_1cc_events_dump_v1 d
INNER JOIN magic_first_producer_created_date ON d.merchant_id = magic_first_producer_created_date.merchant_id
INNER JOIN magic_base m
ON m.merchant_id = d.merchant_id
WHERE lower(event_name) IN ('open','submit')
AND d.merchant_id<>'Hb4PVe74lPmk0k'
GROUP BY 1),

modified_first_rto_date AS
(SELECT f.merchant_id, DATE(live_date) AS live_date, pre_magic_rto_percentage, MIN(DATE(p.created_date)) as modified_magic_rto_first_payment_date
FROM aggregate_pa.magic_rto_reimbursement_fact f
INNER JOIN payments p ON f.order_id = (p.order_id)
INNER JOIN magic_base ON f.merchant_id = magic_base.merchant_id

WHERE f.status NOT IN ('created', 'cancelled')
AND cod_intelligence_enabled = true
AND cod_eligible = true
AND f.merchant_id<>'Hb4PVe74lPmk0k'
AND DATE(p.created_date) >= live_date
GROUP BY 1,2,3),


magic_rto AS
(SELECT f.merchant_id,

COUNT(DISTINCT(CASE WHEN IF(pre_magic_rto_percentage IS NOT NULL, 1=1, DATE(f.order_created_date) > date_add('day', 20, date(modified_magic_rto_first_payment_date))) AND ((DATE_DIFF('day', DATE(f.order_created_date), NOW())) BETWEEN 15 AND 44) AND f.status = 'rto' THEN f.order_id ELSE null END))*100.00/NULLIF(COUNT(DISTINCT IF(IF(pre_magic_rto_percentage IS NOT NULL, 1=1, DATE(f.order_created_date) > date_add('day', 20, date(modified_magic_rto_first_payment_date))) AND (DATE_DIFF('day', DATE(f.order_created_date), NOW())) BETWEEN 15 AND 44,f.order_id,NULL)),0) AS magic_rto_percentage,

COUNT(DISTINCT(CASE WHEN (DATE_DIFF('day', DATE(modified_magic_rto_first_payment_date), DATE(f.order_created_date)) BETWEEN 0 AND 21) AND f.status = 'rto' THEN f.order_id ELSE null END))*100.00/NULLIF(COUNT(DISTINCT IF((DATE_DIFF('day', DATE(modified_magic_rto_first_payment_date), DATE(f.order_created_date)) BETWEEN 0 AND 21) ,f.order_id,NULL)),0) AS magic_first_three_weeks_rto_percentage

FROM aggregate_pa.magic_rto_reimbursement_fact f
--INNER JOIN payments p ON f.order_id = (p.order_id)
INNER JOIN modified_first_rto_date ON f.merchant_id = modified_first_rto_date.merchant_id
WHERE f.status NOT IN ('created', 'cancelled')
AND cod_intelligence_enabled = true
AND cod_eligible = true
AND f.merchant_id<>'Hb4PVe74lPmk0k'
--AND (DATE_DIFF('day', DATE(f.order_created_date), NOW())) BETWEEN 1 AND 31
GROUP BY 1),

magic_page_load_time AS
(
SELECT producer_created_date, merchant_id, checkout_id, order_id,(cast(json_extract_scalar(properties,'$.data.meta["timeSince.open"]') as bigint))/1000.0 Render_Time
from hive.aggregate_pa.cx_1cc_events_dump_v1
where (DATE_DIFF('day', producer_created_date, NOW())) between 1 and 31
AND ((UPPER(event_name ) = UPPER('render:complete')))
),

Submit AS
(
SELECT checkout_id, order_id, (cast(json_extract_scalar(properties,'$.data.meta["timeSince.render"]') as bigint))/1000.0 Submit_Time
from aggregate_pa.cx_1cc_events_dump_v1
where (DATE_DIFF('day', producer_created_date, NOW())) between 1 and 31
AND event_name='submit'
),

Payment AS
(
SELECT id, method, order_id, if(method = 'cod',0,authorized_at - created_at) as payment_time

FROM realtime_hudi_api.payments

WHERE(DATE_DIFF('day', DATE(created_date), NOW())) between 1 and 31
AND ((authorized_at IS NOT NULL
      AND lower(method) <> 'cod')
     OR
      (lower(method) = 'cod')
    )
),

load_completion_time AS
(SELECT r.merchant_id, round((approx_percentile((render_time+submit_time+payment_time), 0.5)),2) p50_completion_time_seconds
FROM magic_page_load_time R
INNER JOIN Submit S ON R.checkout_id = S.checkout_id
INNER JOIN Payment P ON SUBSTR(S.order_id, 7, 14) = P.order_id
GROUP BY 1),

prefill_prelogin AS
(SELECT
merchant_id,

COUNT(DISTINCT CASE WHEN ((CASE WHEN lower(json_extract_scalar(context,'$.user_agent_parsed.os.family'))='ios' OR lower(browser_name) LIKE '%safari%' THEN 'not-applicable' ELSE 'applicable' END) = 'applicable') AND event_name = 'render:1cc_summary_screen_loaded_completed' AND (CASE WHEN (CAST(json_extract(properties, '$.data.prefill_contact_number') AS varchar) IS NULL
OR CAST(json_extract(properties, '$.data.prefill_contact_number') AS varchar) = '') THEN 0
ELSE 1 END) = 1 THEN checkout_id ELSE NULL END)*100.0/NULLIF(count(DISTINCT IF(event_name = 'render:1cc_summary_screen_loaded_completed' AND ((CASE WHEN lower(json_extract_scalar(context,'$.user_agent_parsed.os.family'))='ios' OR lower(browser_name) LIKE '%safari%' THEN 'not-applicable' ELSE 'applicable' END) = 'applicable'),checkout_id,NULL)),0) AS magic_prefill_percentage,

COUNT(DISTINCT (CASE WHEN event_name = 'render:1cc_summary_screen_loaded_completed' AND json_extract_scalar(properties, '$.data.meta.initial_loggedIn') = 'true' THEN checkout_id
ELSE null END))*100.00/NULLIF(COUNT(DISTINCT IF(event_name = 'render:1cc_summary_screen_loaded_completed',checkout_id,NULL)),0) AS magic_pre_login_percentage
FROM
aggregate_pa.cx_1cc_events_dump_v1
WHERE (DATE_DIFF('day', producer_created_date, NOW())) BETWEEN 1 AND 31
GROUP BY 1)

SELECT magic_base.merchant_id, pre_magic_cr_percentage, pre_magic_rto_percentage,
IF(pre_magic_cr_percentage IS NULL, magic_first_three_weeks_cr, pre_magic_cr_percentage) as modified_pre_magic_cr_percentage, magic_cr_percentage,
IF(pre_magic_rto_percentage IS NULL, magic_first_three_weeks_rto_percentage, pre_magic_rto_percentage) as modified_pre_magic_rto_percentage, magic_rto_percentage,
p50_completion_time_seconds,
magic_prefill_percentage,
magic_pre_login_percentage,
approx_percentile((magic_page_load_time.render_time), 0.5) AS p50_page_load_time_seconds

FROM magic_base
LEFT JOIN magic_cr ON magic_base.merchant_id = magic_cr.merchant_id
LEFT JOIN magic_rto ON magic_base.merchant_id = magic_rto.merchant_id
LEFT JOIN magic_page_load_time ON magic_base.merchant_id = magic_page_load_time.merchant_id
LEFT JOIN load_completion_time ON magic_base.merchant_id = load_completion_time.merchant_id
LEFT JOIN prefill_prelogin ON magic_base.merchant_id = prefill_prelogin.merchant_id
GROUP BY 1,2,3,4,5,6,7,8,9,10


"""
