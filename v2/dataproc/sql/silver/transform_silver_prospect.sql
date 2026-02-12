CREATE OR REPLACE TABLE __CATALOG__.__SCHEMA__.silver_prospect AS
SELECT
  split_part(raw_line, ',', 1) AS agency_id,
  split_part(raw_line, ',', 2) AS last_name,
  split_part(raw_line, ',', 3) AS first_name,
  split_part(raw_line, ',', 4) AS middle_initial,
  split_part(raw_line, ',', 5) AS gender,
  split_part(raw_line, ',', 6) AS address_line1,
  split_part(raw_line, ',', 7) AS address_line2,
  split_part(raw_line, ',', 8) AS postal_code,
  split_part(raw_line, ',', 9) AS city,
  split_part(raw_line, ',', 10) AS state,
  split_part(raw_line, ',', 11) AS country,
  split_part(raw_line, ',', 12) AS phone,
  try_cast(split_part(raw_line, ',', 13) AS INT) AS income,
  try_cast(split_part(raw_line, ',', 14) AS INT) AS number_cars,
  try_cast(split_part(raw_line, ',', 15) AS INT) AS number_children,
  split_part(raw_line, ',', 16) AS marital_status,
  try_cast(split_part(raw_line, ',', 17) AS INT) AS age,
  try_cast(split_part(raw_line, ',', 18) AS INT) AS credit_rating,
  split_part(raw_line, ',', 19) AS own_or_rent_flag,
  split_part(raw_line, ',', 20) AS employer,
  try_cast(split_part(raw_line, ',', 21) AS BOOLEAN) AS is_customer,
  try_cast(split_part(raw_line, ',', 22) AS BIGINT) AS net_worth,
  array_join(
    array_compact(
      array(
        CASE WHEN try_cast(split_part(raw_line, ',', 22) AS BIGINT) > 1000000 OR try_cast(split_part(raw_line, ',', 13) AS INT) > 200000 THEN 'HighValue' ELSE NULL END,
        CASE WHEN try_cast(split_part(raw_line, ',', 17) AS INT) < 25 THEN 'YoungAdult' ELSE NULL END,
        CASE WHEN try_cast(split_part(raw_line, ',', 18) AS INT) > 700 THEN 'HighCredit' ELSE NULL END
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
