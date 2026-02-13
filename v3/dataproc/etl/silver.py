"""
v3 Silver: PySpark transforms using v2 table/column names.
Same tables/columns as v2. Pipe-delimited: element_at(split(raw_line, '|'), n) (1-based).
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, lit, split, element_at,
    when, size, substring, length, trim, to_date, monotonically_increasing_id,
    array, array_join, coalesce,
)
from pyspark.sql.types import TimestampType

# Helper: 1-based pipe field (v2 split_part(raw_line, '|', n) -> element_at(split(...), n))
def _p(s, n):
    return element_at(split(col("raw_line"), "\\|"), n)


def transform_silver_batch(
    spark: SparkSession,
    database: str,
    batch_id: int,
    table_order: list,
) -> None:
    """
    Transform batch silver tables (v2 names). Reads from database.bronze_*.
    table_order: list of silver table short names (e.g. silver_date, silver_time, ...).
    """
    for table_short in table_order:
        if table_short == "silver_date":
            _silver_date(spark, database, batch_id)
        elif table_short == "silver_time":
            _silver_time(spark, database, batch_id)
        elif table_short == "silver_status_type":
            _silver_status_type(spark, database, batch_id)
        elif table_short == "silver_trade_type":
            _silver_trade_type(spark, database, batch_id)
        elif table_short == "silver_industry":
            _silver_industry(spark, database, batch_id)
        elif table_short == "silver_tax_rate":
            _silver_tax_rate(spark, database, batch_id)
        elif table_short == "silver_trades":
            _silver_trades(spark, database, batch_id)
        elif table_short == "silver_daily_market":
            _silver_daily_market(spark, database, batch_id)
        elif table_short == "silver_cash_transaction":
            _silver_cash_transaction(spark, database, batch_id)
        elif table_short == "silver_holding_history":
            _silver_holding_history(spark, database, batch_id)
        elif table_short == "silver_watch_history":
            _silver_watch_history(spark, database, batch_id)
        elif table_short == "silver_prospect":
            _silver_prospect(spark, database, batch_id)
        elif table_short == "silver_companies":
            _silver_companies(spark, database, batch_id)
        elif table_short == "silver_securities":
            _silver_securities(spark, database, batch_id)
        elif table_short == "silver_financials":
            _silver_financials(spark, database, batch_id)


def _bronze(spark, database: str, bronze_table: str, batch_id: int):
    return spark.table(f"{database}.{bronze_table}").filter(col("_batch_id") == batch_id).filter(col("raw_line").isNotNull()).filter(col("raw_line") != "")


def _silver_date(spark, database: str, batch_id: int):
    b = _bronze(spark, database, "bronze_date", batch_id)
    df = b.select(
        col("raw_line").cast("string"),
        col("_batch_id"),
    ).filter(size(split(col("raw_line"), "\\|")) >= 18).select(
        _p("raw_line", 1).cast("int").alias("sk_date_id"),
        _p("raw_line", 2).cast("date").alias("date_value"),
        _p("raw_line", 3).alias("date_desc"),
        _p("raw_line", 4).cast("int").alias("calendar_year_id"),
        _p("raw_line", 5).alias("calendar_year_desc"),
        _p("raw_line", 6).cast("int").alias("calendar_qtr_id"),
        _p("raw_line", 7).alias("calendar_qtr_desc"),
        _p("raw_line", 8).cast("int").alias("calendar_month_id"),
        _p("raw_line", 9).alias("calendar_month_desc"),
        _p("raw_line", 10).cast("int").alias("calendar_week_id"),
        _p("raw_line", 11).alias("calendar_week_desc"),
        _p("raw_line", 12).cast("int").alias("day_of_week_num"),
        _p("raw_line", 13).alias("day_of_week_desc"),
        _p("raw_line", 14).cast("int").alias("fiscal_year_id"),
        _p("raw_line", 15).alias("fiscal_year_desc"),
        _p("raw_line", 16).cast("int").alias("fiscal_qtr_id"),
        _p("raw_line", 17).alias("fiscal_qtr_desc"),
        _p("raw_line", 18).cast("boolean").alias("holiday_flag"),
        lit(batch_id).alias("batch_id"),
        current_timestamp().alias("load_timestamp"),
    )
    df.write.format("delta").mode("overwrite").saveAsTable(f"{database}.silver_date")


def _silver_time(spark, database: str, batch_id: int):
    b = _bronze(spark, database, "bronze_time", batch_id)
    df = b.select(
        _p("raw_line", 1).cast("int").alias("sk_time_id"),
        _p("raw_line", 2).alias("time_value"),
        _p("raw_line", 3).cast("int").alias("hour_id"),
        _p("raw_line", 4).alias("hour_desc"),
        _p("raw_line", 5).cast("int").alias("minute_id"),
        _p("raw_line", 6).alias("minute_desc"),
        _p("raw_line", 7).cast("int").alias("second_id"),
        _p("raw_line", 8).alias("second_desc"),
        _p("raw_line", 9).cast("boolean").alias("market_hours_flag"),
        _p("raw_line", 10).cast("boolean").alias("office_hours_flag"),
        lit(batch_id).alias("batch_id"),
        current_timestamp().alias("load_timestamp"),
    )
    df.write.format("delta").mode("overwrite").saveAsTable(f"{database}.silver_time")


def _silver_status_type(spark, database: str, batch_id: int):
    b = _bronze(spark, database, "bronze_status_type", batch_id)
    df = b.select(
        _p("raw_line", 1).alias("st_id"),
        _p("raw_line", 2).alias("st_name"),
        lit(batch_id).alias("batch_id"),
        current_timestamp().alias("load_timestamp"),
    )
    df.write.format("delta").mode("overwrite").saveAsTable(f"{database}.silver_status_type")


def _silver_trade_type(spark, database: str, batch_id: int):
    b = _bronze(spark, database, "bronze_trade_type", batch_id)
    df = b.select(
        _p("raw_line", 1).alias("tt_id"),
        _p("raw_line", 2).alias("tt_name"),
        _p("raw_line", 3).cast("boolean").alias("tt_is_sell"),
        _p("raw_line", 4).cast("boolean").alias("tt_is_mrkt"),
        lit(batch_id).alias("batch_id"),
        current_timestamp().alias("load_timestamp"),
    )
    df.write.format("delta").mode("overwrite").saveAsTable(f"{database}.silver_trade_type")


def _silver_industry(spark, database: str, batch_id: int):
    b = _bronze(spark, database, "bronze_industry", batch_id)
    df = b.select(
        _p("raw_line", 1).alias("in_id"),
        _p("raw_line", 2).alias("in_name"),
        _p("raw_line", 3).alias("in_sc_id"),
        lit(batch_id).alias("batch_id"),
        current_timestamp().alias("load_timestamp"),
    )
    df.write.format("delta").mode("overwrite").saveAsTable(f"{database}.silver_industry")


def _silver_tax_rate(spark, database: str, batch_id: int):
    b = _bronze(spark, database, "bronze_tax_rate", batch_id)
    df = b.select(
        _p("raw_line", 1).alias("tx_id"),
        _p("raw_line", 2).alias("tx_name"),
        _p("raw_line", 3).cast("double").alias("tx_rate"),
        lit(batch_id).alias("batch_id"),
        current_timestamp().alias("load_timestamp"),
    )
    df.write.format("delta").mode("overwrite").saveAsTable(f"{database}.silver_tax_rate")


def _silver_trades(spark, database: str, batch_id: int):
    b = _bronze(spark, database, "bronze_trade", batch_id)
    b = b.filter(size(split(col("raw_line"), "\\|")) >= 14)
    df = b.select(
        _p("raw_line", 1).cast("long").alias("trade_id"),
        _p("raw_line", 2).cast("timestamp").alias("trade_dts"),
        _p("raw_line", 3).alias("status_id"),
        _p("raw_line", 4).alias("trade_type_id"),
        when(_p("raw_line", 5) == "1", True).otherwise(False).alias("is_cash"),
        _p("raw_line", 6).alias("symbol"),
        _p("raw_line", 7).cast("int").alias("quantity"),
        _p("raw_line", 8).cast("double").alias("bid_price"),
        _p("raw_line", 9).cast("long").alias("account_id"),
        _p("raw_line", 10).alias("exec_name"),
        _p("raw_line", 11).cast("double").alias("trade_price"),
        _p("raw_line", 12).cast("double").alias("charge"),
        _p("raw_line", 13).cast("double").alias("commission"),
        _p("raw_line", 14).cast("double").alias("tax"),
        lit(True).alias("is_current"),
        _p("raw_line", 2).cast("timestamp").alias("effective_date"),
        lit(None).cast(TimestampType()).alias("end_date"),
        lit(batch_id).alias("batch_id"),
        current_timestamp().alias("load_timestamp"),
        lit("SBATCH").alias("record_type"),
    )
    df.write.format("delta").mode("overwrite").saveAsTable(f"{database}.silver_trades")


def _silver_daily_market(spark, database: str, batch_id: int):
    from pyspark.sql.functions import concat
    b = _bronze(spark, database, "bronze_daily_market", batch_id)
    b = b.filter(size(split(col("raw_line"), "\\|")) == 6)
    df = b.select(
        concat(_p("raw_line", 1).cast("string"), lit("|"), _p("raw_line", 2)).alias("dm_key"),
        _p("raw_line", 1).cast("date").alias("dm_date"),
        _p("raw_line", 2).alias("dm_s_symb"),
        _p("raw_line", 3).cast("double").alias("dm_close"),
        _p("raw_line", 4).cast("double").alias("dm_high"),
        _p("raw_line", 5).cast("double").alias("dm_low"),
        _p("raw_line", 6).cast("long").alias("dm_vol"),
        lit(batch_id).alias("batch_id"),
        current_timestamp().alias("load_timestamp"),
    )
    df.write.format("delta").mode("overwrite").saveAsTable(f"{database}.silver_daily_market")


def _silver_cash_transaction(spark, database: str, batch_id: int):
    b = _bronze(spark, database, "bronze_cash_transaction", batch_id)
    b = b.filter(size(split(col("raw_line"), "\\|")) == 4)
    from pyspark.sql.functions import concat
    df = b.select(
        concat(_p("raw_line", 1).cast("string"), lit("|"), _p("raw_line", 2)).alias("ct_key"),
        _p("raw_line", 1).cast("long").alias("ct_ca_id"),
        _p("raw_line", 2).cast("timestamp").alias("ct_dts"),
        _p("raw_line", 3).cast("double").alias("ct_amt"),
        _p("raw_line", 4).alias("ct_name"),
        lit(True).alias("is_current"),
        _p("raw_line", 2).cast("timestamp").alias("effective_date"),
        lit(None).cast(TimestampType()).alias("end_date"),
        lit(batch_id).alias("batch_id"),
        current_timestamp().alias("load_timestamp"),
        lit("SBATCH").alias("record_type"),
    )
    df.write.format("delta").mode("overwrite").saveAsTable(f"{database}.silver_cash_transaction")


def _silver_holding_history(spark, database: str, batch_id: int):
    b = _bronze(spark, database, "bronze_holding_history", batch_id)
    b = b.filter(size(split(col("raw_line"), "\\|")) == 4)
    df = b.select(
        _p("raw_line", 1).cast("long").alias("hh_h_t_id"),
        _p("raw_line", 2).cast("long").alias("hh_t_id"),
        _p("raw_line", 3).cast("int").alias("hh_before_qty"),
        _p("raw_line", 4).cast("int").alias("hh_after_qty"),
        lit(True).alias("is_current"),
        lit("1970-01-01").cast("timestamp").alias("effective_date"),
        lit(None).cast(TimestampType()).alias("end_date"),
        lit(batch_id).alias("batch_id"),
        current_timestamp().alias("load_timestamp"),
        lit("SBATCH").alias("record_type"),
    )
    df.write.format("delta").mode("overwrite").saveAsTable(f"{database}.silver_holding_history")


def _silver_watch_history(spark, database: str, batch_id: int):
    b = _bronze(spark, database, "bronze_watch_history", batch_id)
    b = b.filter(size(split(col("raw_line"), "\\|")) == 4)
    from pyspark.sql.functions import concat
    df = b.select(
        concat(_p("raw_line", 1), lit("-"), _p("raw_line", 2)).alias("wh_key"),
        _p("raw_line", 1).cast("long").alias("w_c_id"),
        _p("raw_line", 2).alias("w_s_symb"),
        _p("raw_line", 3).cast("timestamp").alias("w_dts"),
        _p("raw_line", 4).alias("w_action"),
        lit(True).alias("is_current"),
        _p("raw_line", 3).cast("timestamp").alias("effective_date"),
        lit(None).cast(TimestampType()).alias("end_date"),
        lit(batch_id).alias("batch_id"),
        current_timestamp().alias("load_timestamp"),
        lit("SBATCH").alias("record_type"),
    )
    df.write.format("delta").mode("overwrite").saveAsTable(f"{database}.silver_watch_history")


def _silver_prospect(spark, database: str, batch_id: int):
    b = _bronze(spark, database, "bronze_prospect", batch_id)
    def _c(n):
        return element_at(split(col("raw_line"), ","), n)
    from pyspark.sql.functions import concat_ws
    nameplate = concat_ws(",",
        when((_c(22).cast("long") > 1000000) | (_c(13).cast("int") > 200000), lit("HighValue")),
        when(_c(17).cast("int") < 25, lit("YoungAdult")),
        when(_c(18).cast("int") > 700, lit("HighCredit")),
    )
    df = b.select(
        _c(1).alias("agency_id"), _c(2).alias("last_name"), _c(3).alias("first_name"), _c(4).alias("middle_initial"),
        _c(5).alias("gender"), _c(6).alias("address_line1"), _c(7).alias("address_line2"), _c(8).alias("postal_code"),
        _c(9).alias("city"), _c(10).alias("state"), _c(11).alias("country"), _c(12).alias("phone"),
        _c(13).cast("int").alias("income"), _c(14).cast("int").alias("number_cars"), _c(15).cast("int").alias("number_children"),
        _c(16).alias("marital_status"), _c(17).cast("int").alias("age"), _c(18).cast("int").alias("credit_rating"),
        _c(19).alias("own_or_rent_flag"), _c(20).alias("employer"), _c(21).cast("boolean").alias("is_customer"),
        _c(22).cast("long").alias("net_worth"),
        nameplate.alias("marketing_nameplate"),
        col("_batch_id").alias("batch_id"),
        current_timestamp().alias("load_timestamp"),
    )
    df.write.format("delta").mode("overwrite").saveAsTable(f"{database}.silver_prospect")


def _silver_companies(spark, database: str, batch_id: int):
    b = spark.table(f"{database}.bronze_finwire").filter(col("_batch_id") == batch_id).filter(substring(col("raw_line"), 16, 3) == "CMP").filter(length(col("raw_line")) >= 394)
    raw = col("raw_line")
    df = b.select(
        monotonically_increasing_id().alias("sk_company_id"),
        trim(substring(raw, 79, 10)).alias("company_id"),
        trim(substring(raw, 19, 60)).alias("company_name"),
        trim(substring(raw, 93, 2)).alias("industry_id"),
        trim(substring(raw, 95, 4)).alias("sp_rating"),
        trim(substring(raw, 89, 4)).alias("status"),
        to_date(substring(raw, 99, 8), "yyyyMMdd").alias("founding_date"),
        trim(substring(raw, 348, 46)).alias("ceo_name"),
        trim(substring(raw, 107, 80)).alias("address_line1"),
        trim(substring(raw, 187, 80)).alias("address_line2"),
        trim(substring(raw, 267, 12)).alias("postal_code"),
        trim(substring(raw, 279, 25)).alias("city"),
        trim(substring(raw, 304, 20)).alias("state_province"),
        trim(substring(raw, 324, 24)).alias("country"),
        trim(substring(raw, 394, 150)).alias("description"),
        col("_batch_id").alias("batch_id"),
        current_timestamp().alias("load_timestamp"),
    )
    df.write.format("delta").mode("overwrite").saveAsTable(f"{database}.silver_companies")


def _silver_securities(spark, database: str, batch_id: int):
    b = spark.table(f"{database}.bronze_finwire").filter(col("_batch_id") == batch_id).filter(substring(col("raw_line"), 16, 3) == "SEC").filter(length(col("raw_line")) >= 220)
    raw = col("raw_line")
    df = b.select(
        trim(substring(raw, 19, 15)).alias("symbol"),
        trim(substring(raw, 34, 6)).alias("issue_type"),
        trim(substring(raw, 40, 4)).alias("status"),
        trim(substring(raw, 44, 70)).alias("name"),
        trim(substring(raw, 114, 6)).alias("ex_id"),
        trim(substring(raw, 120, 13)).cast("long").alias("sh_out"),
        to_date(substring(raw, 133, 8), "yyyyMMdd").alias("first_trade_date"),
        trim(substring(raw, 141, 8)).alias("first_trade_exchg"),
        trim(substring(raw, 149, 12)).cast("double").alias("dividend"),
        trim(substring(raw, 161, 60)).alias("co_name_or_cik"),
        col("_batch_id").alias("batch_id"),
        current_timestamp().alias("load_timestamp"),
    )
    df.write.format("delta").mode("overwrite").saveAsTable(f"{database}.silver_securities")


def _silver_financials(spark, database: str, batch_id: int):
    b = spark.table(f"{database}.bronze_finwire").filter(col("_batch_id") == batch_id).filter(substring(col("raw_line"), 16, 3) == "FIN").filter(length(col("raw_line")) >= 246)
    raw = col("raw_line")
    df = b.select(
        trim(substring(raw, 187, 60)).alias("co_name_or_cik"),
        trim(substring(raw, 19, 4)).cast("int").alias("year"),
        trim(substring(raw, 23, 1)).cast("int").alias("quarter"),
        to_date(substring(raw, 24, 8), "yyyyMMdd").alias("qtr_start_date"),
        to_date(substring(raw, 32, 8), "yyyyMMdd").alias("posting_date"),
        trim(substring(raw, 40, 17)).cast("double").alias("revenue"),
        trim(substring(raw, 57, 17)).cast("double").alias("earnings"),
        trim(substring(raw, 74, 12)).cast("double").alias("eps"),
        trim(substring(raw, 86, 12)).cast("double").alias("diluted_eps"),
        trim(substring(raw, 98, 12)).cast("double").alias("margin"),
        trim(substring(raw, 110, 17)).cast("double").alias("inventory"),
        trim(substring(raw, 127, 17)).cast("double").alias("assets"),
        trim(substring(raw, 144, 17)).cast("double").alias("liabilities"),
        trim(substring(raw, 161, 13)).cast("long").alias("sh_out"),
        trim(substring(raw, 174, 13)).cast("long").alias("diluted_sh_out"),
        col("_batch_id").alias("batch_id"),
        current_timestamp().alias("load_timestamp"),
    )
    df.write.format("delta").mode("overwrite").saveAsTable(f"{database}.silver_financials")
