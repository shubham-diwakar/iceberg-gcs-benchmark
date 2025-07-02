WITH
  customer_total_return AS (
    SELECT
      sr_customer_sk,
      sr_store_sk,
      SUM(sr_return_amt) AS ctr_total_return
    FROM
      ${database}.${schema}.store_returns
      JOIN ${database}.${schema}.date_dim ON sr_returned_date_sk = d_date_sk
    WHERE
      d_year = 2000
    GROUP BY
      sr_customer_sk,
      sr_store_sk
  ),
  store_avg_return AS (
    SELECT
      sr_store_sk,
      AVG(ctr_total_return) AS avg_return
    FROM
      customer_total_return
    GROUP BY
      sr_store_sk
  )
SELECT
  c_customer_id
FROM
  customer_total_return ctr
  JOIN store_avg_return sar ON ctr.sr_store_sk = sar.sr_store_sk
  JOIN ${database}.${schema}.store s ON ctr.sr_store_sk = s.s_store_sk
  JOIN ${database}.${schema}.customer c ON ctr.sr_customer_sk = c.c_customer_sk
WHERE
  ctr.ctr_total_return > (sar.avg_return * 1.2)
  AND s.s_state = 'TN'
ORDER BY
  c_customer_id
LIMIT 100;