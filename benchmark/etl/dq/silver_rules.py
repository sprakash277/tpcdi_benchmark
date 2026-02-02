"""
Silver-layer Data Quality rules for TPC-DI.

Runs mandatory TPC-DI validation rules and generic checks (completeness, uniqueness,
validity, consistency). Failures are logged to gold.dim_messages.
"""

import logging
import time
from datetime import datetime
from typing import Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count

from benchmark.etl.dq.dim_messages import ensure_dim_messages_exists, log_message
from benchmark.etl.table_timing import is_detailed as table_timing_is_detailed

logger = logging.getLogger(__name__)


class SilverDQRunner:
    """
    Runs DQ rules against Silver tables and logs to gold.dim_messages.
    """

    def __init__(self, platform):
        self.platform = platform
        self.spark = platform.get_spark()

    def run_silver_dq(self, batch_id: int, prefix: str, dim_messages_table: Optional[str] = None) -> None:
        """
        Run all Silver DQ rules for the given batch and prefix.
        Logs failures to gold_dim_messages (or dim_messages_table if provided).

        Args:
            batch_id: Batch number
            prefix: Table prefix (e.g. catalog.schema)
            dim_messages_table: Full name of dim_messages table (default: {prefix}.gold_dim_messages)
        """
        start_time = time.time()
        start_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if table_timing_is_detailed():
            logger.info(f"[TIMING] Starting Silver DQ validation for batch {batch_id} at {start_datetime}")
        
        messages_table = dim_messages_table or f"{prefix}.gold_dim_messages"
        ensure_dim_messages_exists(self.spark, messages_table, self.platform)

        def log(component: str, message: str, severity: str = "Alert", source: str = ""):
            log_message(
                self.spark, self.platform, messages_table,
                batch_id=batch_id, component_name=component,
                message_text=message, severity=severity, source_table=source,
            )

        # --- DimCustomer (silver_customers) ---
        try:
            self._run_customer_rules(prefix, batch_id, log, messages_table)
        except Exception as e:
            logger.warning(f"Silver DQ silver_customers failed: {e}")
            log("Silver_Customer_Validation", f"DQ run failed: {e}", "Alert", f"{prefix}.silver_customers")

        # --- DimAccount (silver_accounts) ---
        try:
            self._run_account_rules(prefix, batch_id, log, messages_table)
        except Exception as e:
            logger.warning(f"Silver DQ silver_accounts failed: {e}")
            log("Silver_Account_Validation", f"DQ run failed: {e}", "Alert", f"{prefix}.silver_accounts")

        # --- FactTrades / silver_trades ---
        try:
            self._run_trade_rules(prefix, batch_id, log, messages_table)
        except Exception as e:
            logger.warning(f"Silver DQ silver_trades failed: {e}")
            log("Silver_Trade_Validation", f"DQ run failed: {e}", "Alert", f"{prefix}.silver_trades")

        # --- DimDate (silver_date) ---
        try:
            self._run_date_rules(prefix, log, messages_table)
        except Exception as e:
            logger.warning(f"Silver DQ silver_date failed: {e}")
            log("Silver_Date_Validation", f"DQ run failed: {e}", "Alert", f"{prefix}.silver_date")

        end_time = time.time()
        end_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        duration = end_time - start_time
        
        if table_timing_is_detailed():
            logger.info(f"[TIMING] Completed Silver DQ validation for batch {batch_id} at {end_datetime}")
            logger.info(f"[TIMING] Silver DQ - Start: {start_datetime}, End: {end_datetime}, Duration: {duration:.2f}s")
        
        logger.info(f"Silver DQ completed for batch_id={batch_id}, prefix={prefix} in {duration:.2f}s")

    def _run_customer_rules(self, prefix: str, batch_id: int, log, messages_table: str) -> None:
        source = f"{prefix}.silver_customers"
        try:
            df = self.spark.table(source)
        except Exception:
            logger.debug(f"Table {source} not found, skipping customer DQ")
            return
        if "batch_id" in df.columns:
            df = df.filter(col("batch_id") == batch_id)

        null_key = df.filter(col("customer_id").isNull() | col("tax_id").isNull())
        n = null_key.count()
        if n > 0:
            log("Silver_Customer_Validation", f"customer_id/tax_id NULL: {n} row(s)", "Reject", source)

        bad_tier = df.filter(col("tier").isNotNull() & ~(col("tier").isin([1, 2, 3])))
        n = bad_tier.count()
        if n > 0:
            log("Silver_Customer_Validation", f"tier not in (1,2,3): {n} row(s)", "Alert", source)

        from pyspark.sql.functions import current_date
        today = current_date()
        future_dob = df.filter(col("dob").isNotNull() & (col("dob") > today))
        n = future_dob.count()
        if n > 0:
            log("Silver_Customer_Validation", f"dob in future: {n} row(s)", "Alert", source)

        dup = df.groupBy("customer_id").agg(count("*").alias("cnt")).filter(col("cnt") > 1)
        n = dup.count()
        if n > 0:
            log("Silver_Customer_Validation", f"duplicate customer_id within batch: {n} key(s)", "Alert", source)

        if "end_date" in df.columns and "effective_date" in df.columns:
            cond = (col("end_date").isNotNull() & col("effective_date").isNotNull()
                    & (col("end_date") < col("effective_date")))
            bad_dates = df.filter(cond)
            n = bad_dates.count()
            if n > 0:
                log("Silver_Customer_Validation", f"end_date < effective_date: {n} row(s)", "Alert", source)

    def _run_account_rules(self, prefix: str, batch_id: int, log, messages_table: str) -> None:
        source = f"{prefix}.silver_accounts"
        try:
            acc = self.spark.table(source)
        except Exception:
            logger.debug(f"Table {source} not found, skipping account DQ")
            return
        if "batch_id" in acc.columns:
            acc = acc.filter(col("batch_id") == batch_id)
        try:
            cust = self.spark.table(f"{prefix}.silver_customers")
            cust_ids = cust.select("customer_id").distinct()
            missing = acc.join(cust_ids, acc["customer_id"] == cust_ids["customer_id"], "left_anti")
            n = missing.count()
            if n > 0:
                log("Silver_Account_Validation", f"customer_id not in silver_customers: {n} row(s)", "Alert", source)
        except Exception as e:
            log("Silver_Account_Validation", f"RI check failed (silver_customers): {e}", "Alert", source)
        null_cust = acc.filter(col("customer_id").isNull())
        if null_cust.count() > 0:
            log("Silver_Account_Validation", "customer_id NULL in silver_accounts", "Reject", source)
        if "end_date" in acc.columns and "effective_date" in acc.columns:
            cond = col("end_date").isNotNull() & col("effective_date").isNotNull()
            cond = cond & (col("end_date") < col("effective_date"))
            bad = acc.filter(cond)
            if bad.count() > 0:
                log("Silver_Account_Validation", "end_date < effective_date in silver_accounts", "Alert", source)

    def _run_trade_rules(self, prefix: str, batch_id: int, log, messages_table: str) -> None:
        source = f"{prefix}.silver_trades"
        try:
            df = self.spark.table(source)
        except Exception:
            logger.debug(f"Table {source} not found, skipping trade DQ")
            return
        if "batch_id" in df.columns:
            df = df.filter(col("batch_id") == batch_id)

        bad_logic = df.filter((col("bid_price").isNull()) | (col("bid_price") <= 0))
        n = bad_logic.count()
        if n > 0:
            log("Silver_Trade_Validation", f"bid_price NULL or <= 0: {n} row(s)", "Alert", source)
        bad_qty = df.filter((col("quantity").isNull()) | (col("quantity") <= 0))
        n = bad_qty.count()
        if n > 0:
            log("Silver_Trade_Validation", f"quantity NULL or <= 0: {n} row(s)", "Alert", source)

        dup = df.groupBy("trade_id").agg(count("*").alias("cnt")).filter(col("cnt") > 1)
        n = dup.count()
        if n > 0:
            log("Silver_Trade_Validation", f"duplicate trade_id within batch: {n} key(s)", "Alert", source)

    def _run_date_rules(self, prefix: str, log, messages_table: str) -> None:
        source = f"{prefix}.silver_date"
        try:
            df = self.spark.table(source)
        except Exception:
            logger.debug(f"Table {source} not found, skipping date DQ")
            return

        from pyspark.sql.functions import length
        str_id = col("sk_date_id").cast("string")
        invalid = df.filter(
            col("sk_date_id").isNull()
            | (length(str_id) != 8)
            | ~str_id.rlike("^[0-9]{8}$")
        )
        n = invalid.count()
        if n > 0:
            log("Silver_Date_Validation", f"sk_date_id not valid YYYYMMDD format: {n} row(s)", "Alert", source)
