"""
v3 Gold: PySpark loads using v2 table/column names.
Same tables/columns as v2. Joins silver + dims where needed.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, lit, to_date, date_format,
    trim, upper, monotonically_increasing_id, coalesce,
    sum as spark_sum, count,
)
from pyspark.sql.types import TimestampType

# v2 gold load order (batch)
GOLD_TABLE_ORDER = [
    "gold_dim_date", "gold_dim_time", "gold_dim_status_type", "gold_dim_trade_type", "gold_dim_industry",
    "gold_dim_customer", "gold_dim_account", "gold_dim_broker", "gold_dim_company", "gold_dim_security",
    "gold_fact_trade", "gold_fact_cash_balances", "gold_fact_holdings", "gold_fact_market_history", "gold_fact_watches",
    "gold_financials", "gold_prospect",
]


def load_gold_batch(
    spark: SparkSession,
    database: str,
    batch_id: int,
    table_order: list = None,
) -> None:
    """Load batch gold tables (v2 names). table_order defaults to GOLD_TABLE_ORDER."""
    order = table_order or GOLD_TABLE_ORDER
    for table_short in order:
        if table_short == "gold_dim_date":
            _gold_dim_date(spark, database, batch_id)
        elif table_short == "gold_dim_time":
            _gold_dim_time(spark, database, batch_id)
        elif table_short == "gold_dim_status_type":
            _gold_dim_status_type(spark, database, batch_id)
        elif table_short == "gold_dim_trade_type":
            _gold_dim_trade_type(spark, database, batch_id)
        elif table_short == "gold_dim_industry":
            _gold_dim_industry(spark, database, batch_id)
        elif table_short == "gold_dim_customer":
            _gold_dim_customer(spark, database, batch_id)
        elif table_short == "gold_dim_account":
            _gold_dim_account(spark, database, batch_id)
        elif table_short == "gold_dim_broker":
            _gold_dim_broker(spark, database, batch_id)
        elif table_short == "gold_dim_company":
            _gold_dim_company(spark, database, batch_id)
        elif table_short == "gold_dim_security":
            _gold_dim_security(spark, database, batch_id)
        elif table_short == "gold_fact_trade":
            _gold_fact_trade(spark, database, batch_id)
        elif table_short == "gold_fact_cash_balances":
            _gold_fact_cash_balances(spark, database, batch_id)
        elif table_short == "gold_fact_holdings":
            _gold_fact_holdings(spark, database, batch_id)
        elif table_short == "gold_fact_market_history":
            _gold_fact_market_history(spark, database, batch_id)
        elif table_short == "gold_fact_watches":
            _gold_fact_watches(spark, database, batch_id)
        elif table_short == "gold_financials":
            _gold_financials(spark, database, batch_id)
        elif table_short == "gold_prospect":
            _gold_prospect(spark, database, batch_id)


def _drop_and_save(df, database: str, table_short: str):
    spark = df.sparkSession
    spark.sql(f"DROP TABLE IF EXISTS {database}.{table_short}")
    df.write.format("delta").mode("overwrite").saveAsTable(f"{database}.{table_short}")


def _gold_dim_date(spark, database: str, batch_id: int):
    sd = spark.table(f"{database}.silver_date").filter(col("batch_id") == batch_id)
    df = sd.select(
        col("sk_date_id"), col("sk_date_id").alias("date_id"), col("date_value"), col("date_desc"),
        col("calendar_year_id"), col("calendar_year_desc"), col("calendar_qtr_id"), col("calendar_qtr_desc"),
        col("calendar_month_id"), col("calendar_month_desc"), col("calendar_week_id"), col("calendar_week_desc"),
        col("day_of_week_num"), col("day_of_week_desc"), col("fiscal_year_id"), col("fiscal_year_desc"),
        col("fiscal_qtr_id"), col("fiscal_qtr_desc"), col("holiday_flag"),
        current_timestamp().alias("etl_timestamp"),
    )
    _drop_and_save(df, database, "gold_dim_date")


def _gold_dim_time(spark, database: str, batch_id: int):
    st = spark.table(f"{database}.silver_time").filter(col("batch_id") == batch_id)
    df = st.select(
        col("sk_time_id"), col("sk_time_id").alias("time_id"), col("time_value"),
        col("hour_id"), col("hour_desc"), col("minute_id"), col("minute_desc"),
        col("second_id"), col("second_desc"), col("market_hours_flag"), col("office_hours_flag"),
        current_timestamp().alias("etl_timestamp"),
    )
    _drop_and_save(df, database, "gold_dim_time")


def _gold_dim_status_type(spark, database: str, batch_id: int):
    st = spark.table(f"{database}.silver_status_type").filter(col("batch_id") == batch_id)
    df = st.select(
        col("st_id").alias("sk_status_type_id"), col("st_id").alias("status_type_id"),
        col("st_id").alias("status_type_code"), col("st_name").alias("status_type_name"),
        current_timestamp().alias("etl_timestamp"),
    )
    _drop_and_save(df, database, "gold_dim_status_type")


def _gold_dim_trade_type(spark, database: str, batch_id: int):
    st = spark.table(f"{database}.silver_trade_type").filter(col("batch_id") == batch_id)
    df = st.select(
        col("tt_id").alias("sk_trade_type_id"), col("tt_id").alias("trade_type_id"),
        col("tt_id").alias("trade_type_code"), col("tt_name").alias("trade_type_name"),
        col("tt_is_sell").alias("is_sell"), col("tt_is_mrkt").alias("is_market"),
        current_timestamp().alias("etl_timestamp"),
    )
    _drop_and_save(df, database, "gold_dim_trade_type")


def _gold_dim_industry(spark, database: str, batch_id: int):
    si = spark.table(f"{database}.silver_industry").filter(col("batch_id") == batch_id)
    df = si.select(
        col("in_id").alias("sk_industry_id"), col("in_id").alias("industry_id"),
        col("in_name").alias("industry_name"), col("in_sc_id").alias("sector_id"),
        lit(None).cast("string").alias("sector_name"),
        current_timestamp().alias("etl_timestamp"),
    )
    _drop_and_save(df, database, "gold_dim_industry")


def _gold_dim_customer(spark, database: str, batch_id: int):
    sc = spark.table(f"{database}.silver_customers").filter(col("batch_id") == batch_id).filter(col("is_current")).filter(col("customer_id") != -1)
    df = sc.select(
        col("sk_customer_id"), col("customer_id"), col("tax_id"), col("status"),
        col("last_name"), col("first_name"), col("middle_name"), col("gender"), col("tier"), col("dob"),
        col("address_line1"), col("address_line2"), col("postal_code"), col("city"), col("state_prov"), col("country"),
        col("email1"), col("email2"), col("local_tax_id"), col("national_tax_id"),
        lit(True).alias("is_current"),
        coalesce(col("effective_date"), col("load_timestamp")).alias("start_date"),
        lit("9999-12-31").cast("date").alias("end_date"),
        col("batch_id"),
        current_timestamp().alias("etl_timestamp"),
    )
    _drop_and_save(df, database, "gold_dim_customer")


def _gold_dim_account(spark, database: str, batch_id: int):
    from pyspark.sql.functions import trim
    sa = spark.table(f"{database}.silver_accounts").filter(col("batch_id") == batch_id).filter(col("is_current")).filter(col("account_id") != -1)
    dc = spark.table(f"{database}.gold_dim_customer").filter(col("is_current"))
    sa_eff = coalesce(sa["effective_date"], sa["load_timestamp"])
    join_cond = (
        trim(sa["customer_id"].cast("string")) == trim(dc["customer_id"].cast("string"))
        & (dc["is_current"])
        & (sa_eff >= dc["start_date"])
        & (dc["end_date"].isNull() | (sa_eff < dc["end_date"]))
    )
    df = sa.join(dc, join_cond, "inner").select(
        monotonically_increasing_id().alias("sk_account_id"),
        sa["account_id"], sa["broker_id"], dc["sk_customer_id"], sa["customer_id"],
        sa["account_name"], sa["tax_status"], sa["status_id"],
        lit(True).alias("is_current"),
        coalesce(sa["effective_date"], sa["load_timestamp"]).alias("start_date"),
        lit("9999-12-31").cast("date").alias("end_date"),
        sa["batch_id"],
        current_timestamp().alias("etl_timestamp"),
    )
    _drop_and_save(df, database, "gold_dim_account")


def _gold_dim_broker(spark, database: str, batch_id: int):
    from pyspark.sql.functions import split, element_at, size, concat
    bh = spark.table(f"{database}.bronze_hr").filter(col("_batch_id") == batch_id).filter(col("raw_line").isNotNull())
    arr = split(col("raw_line"), ",")
    brokers = bh.filter(size(arr) >= 8).filter(element_at(arr, 8).like("%BROKER%")).select(
        element_at(arr, 1).alias("employee_id"),
        element_at(arr, 2).alias("manager_id"),
        element_at(arr, 3).alias("first_name"),
        element_at(arr, 4).alias("last_name"),
        element_at(arr, 5).alias("branch"),
        element_at(arr, 6).alias("office"),
        element_at(arr, 7).alias("phone"),
        element_at(arr, 8).alias("job_code"),
    ).distinct()
    df = brokers.select(
        monotonically_increasing_id().alias("sk_broker_id"),
        col("employee_id").cast("long").alias("broker_id"),
        concat(col("first_name"), lit(" "), col("last_name")).alias("broker_name"),
        col("branch"), col("office"), col("phone"),
        lit(True).alias("is_current"),
        current_timestamp().alias("etl_timestamp"),
    )
    _drop_and_save(df, database, "gold_dim_broker")


def _gold_dim_company(spark, database: str, batch_id: int):
    sc = spark.table(f"{database}.silver_companies").filter(col("batch_id") == batch_id)
    si = spark.table(f"{database}.silver_industry").filter(col("batch_id") == batch_id)
    df = sc.join(si, sc["industry_id"] == si["in_id"], "left").select(
        sc["sk_company_id"], sc["company_id"], sc["company_name"],
        sc["industry_id"], coalesce(si["in_sc_id"], lit("Unknown")).alias("sector"),
        sc["status"], sc["address_line1"], sc["address_line2"], sc["postal_code"],
        sc["city"], sc["state_province"].alias("state_prov"), sc["country"],
        sc["description"], sc["founding_date"], sc["ceo_name"],
        lit(True).alias("is_current"),
        sc["load_timestamp"].alias("start_date"),
        lit("9999-12-31").cast("date").alias("end_date"),
        sc["batch_id"],
        current_timestamp().alias("etl_timestamp"),
    )
    _drop_and_save(df, database, "gold_dim_company")


def _gold_dim_security(spark, database: str, batch_id: int):
    ss = spark.table(f"{database}.silver_securities").filter(col("batch_id") == batch_id)
    dc = spark.table(f"{database}.gold_dim_company").filter(col("is_current"))
    join_cond = (
        (ss["co_name_or_cik"] == dc["company_id"])
        & (dc["is_current"])
        & (ss["load_timestamp"] >= dc["start_date"])
        & (dc["end_date"].isNull() | (ss["load_timestamp"] < dc["end_date"]))
    )
    df = ss.join(dc, join_cond, "left").select(
        monotonically_increasing_id().alias("sk_security_id"),
        ss["symbol"].alias("security_id"), ss["symbol"],
        ss["issue_type"], ss["status"], ss["name"], ss["ex_id"].alias("exchange_id"),
        ss["sh_out"].alias("shares_outstanding"), ss["first_trade_date"], ss["first_trade_exchg"].alias("first_trade_exchange"),
        ss["dividend"], coalesce(dc["sk_company_id"], lit(-1)).alias("sk_company_id"), ss["co_name_or_cik"].alias("company_id"),
        lit(True).alias("is_current"), ss["load_timestamp"].alias("start_date"),
        lit("9999-12-31").cast("date").alias("end_date"), ss["batch_id"],
        current_timestamp().alias("etl_timestamp"),
    )
    _drop_and_save(df, database, "gold_dim_security")


def _gold_fact_trade(spark, database: str, batch_id: int):
    st = spark.table(f"{database}.silver_trades").filter(col("batch_id") == batch_id).filter(col("is_current"))
    dd = spark.table(f"{database}.gold_dim_date")
    dt = spark.table(f"{database}.gold_dim_time")
    da = spark.table(f"{database}.gold_dim_account")
    dc = spark.table(f"{database}.gold_dim_customer")
    ds = spark.table(f"{database}.gold_dim_security")
    dtt = spark.table(f"{database}.gold_dim_trade_type")
    df = st.join(dd, to_date(st["trade_dts"]) == to_date(dd["date_value"]), "inner") \
        .join(dt, date_format(st["trade_dts"], "HH:mm:ss") == dt["time_value"], "inner") \
        .join(da, trim(st["account_id"].cast("string")) == trim(da["account_id"].cast("string")), "inner") \
        .join(dc, trim(da["customer_id"].cast("string")) == trim(dc["customer_id"].cast("string")), "inner") \
        .join(ds, trim(st["symbol"].cast("string")) == trim(ds["symbol"].cast("string")), "inner") \
        .join(dtt, trim(st["trade_type_id"].cast("string")) == trim(dtt["trade_type_id"].cast("string")), "inner") \
        .select(
            st["trade_id"].alias("sk_trade_id"), dd["sk_date_id"], dt["sk_time_id"], dc["sk_customer_id"],
            da["sk_account_id"], ds["sk_security_id"], dtt["sk_trade_type_id"],
            st["trade_id"], st["trade_dts"], st["trade_price"], st["quantity"].alias("trade_quantity"),
            (st["trade_price"] * st["quantity"]).alias("trade_amount"), st["commission"], st["charge"], st["tax"],
            st["status_id"], st["is_cash"], st["exec_name"], st["batch_id"],
            lit(False).alias("late_arriving_flag"),
            current_timestamp().alias("etl_timestamp"),
        )
    _drop_and_save(df, database, "gold_fact_trade")


def _gold_fact_cash_balances(spark, database: str, batch_id: int):
    sct = spark.table(f"{database}.silver_cash_transaction").filter(col("batch_id") == batch_id).filter(col("is_current"))
    dd = spark.table(f"{database}.gold_dim_date")
    da = spark.table(f"{database}.gold_dim_account")
    dc = spark.table(f"{database}.gold_dim_customer")
    joined = sct.join(dd, to_date(sct["ct_dts"]) == to_date(dd["date_value"]), "inner") \
        .join(da, trim(sct["ct_ca_id"].cast("string")) == trim(da["account_id"].cast("string")), "inner") \
        .join(dc, trim(da["customer_id"].cast("string")) == trim(dc["customer_id"].cast("string")), "inner")
    df = joined.groupBy(dd["sk_date_id"], da["sk_account_id"], dc["sk_customer_id"], sct["ct_ca_id"]) \
        .agg(spark_sum("ct_amt").alias("cash_balance"), count("*").alias("transaction_count")) \
        .select(
            col("sk_date_id"), col("sk_account_id"), col("sk_customer_id"),
            col("ct_ca_id").alias("account_id"), col("cash_balance"), col("transaction_count"),
            current_timestamp().alias("etl_timestamp"),
        )
    _drop_and_save(df, database, "gold_fact_cash_balances")


def _gold_fact_holdings(spark, database: str, batch_id: int):
    shh = spark.table(f"{database}.silver_holding_history").filter(col("batch_id") == batch_id).filter(col("is_current"))
    st = spark.table(f"{database}.silver_trades").filter(col("batch_id") == batch_id).filter(col("is_current"))
    dd = spark.table(f"{database}.gold_dim_date")
    da = spark.table(f"{database}.gold_dim_account")
    ds = spark.table(f"{database}.gold_dim_security")
    joined = shh.join(st, shh["hh_t_id"] == st["trade_id"], "inner") \
        .join(dd, to_date(st["trade_dts"]) == to_date(dd["date_value"]), "inner") \
        .join(da, st["account_id"] == da["account_id"], "inner") \
        .join(ds, st["symbol"] == ds["symbol"], "inner")
    df = joined.select(
        dd["sk_date_id"], da["sk_account_id"], ds["sk_security_id"],
        st["account_id"], st["symbol"], shh["hh_after_qty"].alias("quantity"),
        st["trade_price"].alias("purchase_price"), to_date(st["trade_dts"]).alias("purchase_date"),
        current_timestamp().alias("etl_timestamp"),
    )
    _drop_and_save(df, database, "gold_fact_holdings")


def _gold_fact_market_history(spark, database: str, batch_id: int):
    sdm = spark.table(f"{database}.silver_daily_market").filter(col("batch_id") == batch_id)
    dd = spark.table(f"{database}.gold_dim_date")
    ds = spark.table(f"{database}.gold_dim_security")
    dc = spark.table(f"{database}.gold_dim_company")
    df = sdm.join(dd, sdm["dm_date"] == dd["date_value"], "inner") \
        .join(ds, trim(sdm["dm_s_symb"].cast("string")) == trim(ds["symbol"].cast("string")), "inner") \
        .join(dc, trim(ds["company_id"].cast("string")) == trim(dc["company_id"].cast("string")), "left") \
        .select(
            dd["sk_date_id"], ds["sk_security_id"], dc["sk_company_id"],
            sdm["dm_date"].alias("market_date"), sdm["dm_s_symb"].alias("symbol"),
            sdm["dm_close"].alias("close_price"), sdm["dm_high"].alias("high_price"),
            sdm["dm_low"].alias("low_price"), sdm["dm_vol"].alias("volume"),
            sdm["batch_id"], current_timestamp().alias("etl_timestamp"),
        )
    _drop_and_save(df, database, "gold_fact_market_history")


def _gold_fact_watches(spark, database: str, batch_id: int):
    swh = spark.table(f"{database}.silver_watch_history").filter(col("batch_id") == batch_id).filter(col("is_current"))
    dc = spark.table(f"{database}.gold_dim_customer")
    ds = spark.table(f"{database}.gold_dim_security")
    df = swh.join(dc, swh["w_c_id"].cast("long") == dc["customer_id"].cast("long"), "inner") \
        .join(ds, upper(trim(swh["w_s_symb"])) == upper(trim(ds["symbol"])), "inner") \
        .select(
            dc["sk_customer_id"], ds["sk_security_id"],
            swh["w_c_id"].alias("customer_id"), swh["w_s_symb"].alias("symbol"),
            swh["w_dts"].alias("watch_date"), swh["w_action"].alias("watch_action"),
            current_timestamp().alias("etl_timestamp"),
        )
    _drop_and_save(df, database, "gold_fact_watches")


def _gold_financials(spark, database: str, batch_id: int):
    sf = spark.table(f"{database}.silver_financials").filter(col("batch_id") == batch_id)
    dc = spark.table(f"{database}.gold_dim_company").filter(col("is_current"))
    df = sf.join(dc, sf["co_name_or_cik"] == dc["company_id"], "left").select(
        coalesce(dc["sk_company_id"], lit(-1)).alias("sk_company_id"),
        sf["co_name_or_cik"], sf["year"], sf["quarter"], sf["qtr_start_date"], sf["posting_date"],
        sf["revenue"], sf["earnings"], sf["eps"], sf["diluted_eps"], sf["margin"],
        sf["inventory"], sf["assets"], sf["liabilities"], sf["sh_out"], sf["diluted_sh_out"],
        sf["batch_id"], current_timestamp().alias("etl_timestamp"),
    )
    _drop_and_save(df, database, "gold_financials")


def _gold_prospect(spark, database: str, batch_id: int):
    sp = spark.table(f"{database}.silver_prospect").filter(col("batch_id") == batch_id)
    df = sp.select(
        col("agency_id"), col("last_name"), col("first_name"), col("middle_initial"), col("gender"),
        col("address_line1"), col("address_line2"), col("postal_code"), col("city"), col("state"), col("country"),
        col("phone"), col("income"), col("number_cars"), col("number_children"), col("marital_status"),
        col("age"), col("credit_rating"), col("own_or_rent_flag"), col("employer"), col("is_customer"),
        col("net_worth"), col("marketing_nameplate"),
        current_timestamp().alias("etl_timestamp"),
    )
    _drop_and_save(df, database, "gold_prospect")
