CREATE OR REPLACE TABLE __CATALOG__.__SCHEMA__.silver_prospect AS
SELECT
  split(raw_line, ',')[0] AS agency_id,
  split(raw_line, ',')[1] AS last_name,
  split(raw_line, ',')[2] AS first_name,
  split(raw_line, ',')[3] AS middle_initial,
  split(raw_line, ',')[4] AS gender,
  split(raw_line, ',')[5] AS address_line1,
  split(raw_line, ',')[6] AS address_line2,
  split(raw_line, ',')[7] AS postal_code,
  split(raw_line, ',')[8] AS city,
  split(raw_line, ',')[9] AS state,
  split(raw_line, ',')[10] AS country,
  split(raw_line, ',')[11] AS phone,
  try_cast(split(raw_line, ',')[12] AS INT) AS income,
  try_cast(split(raw_line, ',')[13] AS INT) AS number_cars,
  try_cast(split(raw_line, ',')[14] AS INT) AS number_children,
  split(raw_line, ',')[15] AS marital_status,
  try_cast(split(raw_line, ',')[16] AS INT) AS age,
  try_cast(split(raw_line, ',')[17] AS INT) AS credit_rating,
  split(raw_line, ',')[18] AS own_or_rent_flag,
  split(raw_line, ',')[19] AS employer,
  try_cast(split(raw_line, ',')[20] AS BOOLEAN) AS is_customer,
  try_cast(split(raw_line, ',')[21] AS BIGINT) AS net_worth,
  array_join(
    array_compact(
      array(
        CASE WHEN try_cast(split(raw_line, ',')[21] AS BIGINT) > 1000000 OR try_cast(split(raw_line, ',')[12] AS INT) > 200000 THEN 'HighValue' ELSE NULL END,
        CASE WHEN try_cast(split(raw_line, ',')[16] AS INT) < 25 THEN 'YoungAdult' ELSE NULL END,
        CASE WHEN try_cast(split(raw_line, ',')[17] AS INT) > 700 THEN 'HighCredit' ELSE NULL END
      )
    ),
    ','
  ) AS marketing_nameplate,
  _batch_id AS batch_id,
  current_timestamp() AS load_timestamp
FROM __CATALOG__.__SCHEMA__.bronze_prospect
WHERE _batch_id = __BATCH_ID__
  AND raw_line IS NOT NULL
  AND raw_line != ''
