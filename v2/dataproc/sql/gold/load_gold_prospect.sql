DROP TABLE IF EXISTS __CATALOG__.__SCHEMA__.gold_prospect;
CREATE TABLE __CATALOG__.__SCHEMA__.gold_prospect USING DELTA AS
SELECT 
    agency_id,
    last_name,
    first_name,
    middle_initial,
    gender,
    address_line1,
    address_line2,
    postal_code,
    city,
    state,
    country,
    phone,
    income,
    number_cars,
    number_children,
    marital_status,
    age,
    credit_rating,
    own_or_rent_flag,
    employer,
    is_customer,
    net_worth,
    marketing_nameplate,
    current_timestamp() AS etl_timestamp
FROM __CATALOG__.__SCHEMA__.silver_prospect
WHERE batch_id = __BATCH_ID__
