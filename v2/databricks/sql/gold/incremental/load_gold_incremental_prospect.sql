-- TPC-DI v2: Gold incremental - gold_prospect (Batch 2+)
-- SCD Type 1 (Upsert). Deduplicate source so one row per agency_id. Explicit INSERT for schema safety.
-- Placeholders: __CATALOG__, __SCHEMA__, __BATCH_ID__

-- Deduplicate source so only the LATEST record per agency_id updates the target
WITH latest_silver_prospect AS (
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
        batch_id
    FROM __CATALOG__.__SCHEMA__.silver_prospect
    WHERE batch_id = __BATCH_ID__
    QUALIFY ROW_NUMBER() OVER (PARTITION BY agency_id ORDER BY load_timestamp DESC) = 1
)
MERGE INTO __CATALOG__.__SCHEMA__.gold_prospect AS target
USING latest_silver_prospect AS source
ON target.agency_id = source.agency_id
WHEN MATCHED THEN UPDATE SET
    target.last_name = source.last_name,
    target.first_name = source.first_name,
    target.middle_initial = source.middle_initial,
    target.gender = source.gender,
    target.address_line1 = source.address_line1,
    target.address_line2 = source.address_line2,
    target.postal_code = source.postal_code,
    target.city = source.city,
    target.state = source.state,
    target.country = source.country,
    target.phone = source.phone,
    target.income = source.income,
    target.number_cars = source.number_cars,
    target.number_children = source.number_children,
    target.marital_status = source.marital_status,
    target.age = source.age,
    target.credit_rating = source.credit_rating,
    target.own_or_rent_flag = source.own_or_rent_flag,
    target.employer = source.employer,
    target.is_customer = source.is_customer,
    target.net_worth = source.net_worth,
    target.marketing_nameplate = source.marketing_nameplate,
    target.batch_id = source.batch_id,
    target.etl_timestamp = current_timestamp()
WHEN NOT MATCHED THEN INSERT (
    agency_id, last_name, first_name, middle_initial, gender,
    address_line1, address_line2, postal_code, city, state, country,
    phone, income, number_cars, number_children, marital_status,
    age, credit_rating, own_or_rent_flag, employer, is_customer,
    net_worth, marketing_nameplate, batch_id, etl_timestamp
) VALUES (
    source.agency_id, source.last_name, source.first_name, source.middle_initial,
    source.gender, source.address_line1, source.address_line2, source.postal_code,
    source.city, source.state, source.country, source.phone, source.income,
    source.number_cars, source.number_children, source.marital_status,
    source.age, source.credit_rating, source.own_or_rent_flag, source.employer,
    source.is_customer, source.net_worth, source.marketing_nameplate,
    source.batch_id, current_timestamp()
);
