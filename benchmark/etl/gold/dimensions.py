"""
Gold layer dimension table loaders.

Dimensions are current versions from Silver tables, ready for star schema joins.
"""

import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, current_timestamp

# Placeholder IDs for late-arriving dimension (TPC-DI: trade arrives before account/customer)
PLACEHOLDER_CUSTOMER_ID = -1
PLACEHOLDER_ACCOUNT_ID = -1

from benchmark.etl.gold.base import GoldLoaderBase

logger = logging.getLogger(__name__)


class GoldDimCustomer(GoldLoaderBase):
    """Gold dimension table: DimCustomer (current customer versions)."""
    
    def load(self, silver_table: str, target_table: str) -> DataFrame:
        """
        Create DimCustomer from silver_customers (current versions only).
        
        Args:
            silver_table: silver_customers table name
            target_table: gold.DimCustomer table name
            
        Returns:
            DataFrame with DimCustomer schema
        """
        logger.info(f"Loading gold.DimCustomer from {silver_table}")
        
        # Select current versions only
        current_df = self._select_current_version(silver_table)
        
        # Gold dimension: select all columns (already cleaned in Silver)
        gold_df = current_df.select(
            col("sk_customer_id"),
            col("customer_id"),
            col("tax_id"),
            col("status"),
            col("last_name"),
            col("first_name"),
            col("middle_name"),
            col("gender"),
            col("tier"),
            col("dob"),
            col("address_line1"),
            col("address_line2"),
            col("postal_code"),
            col("city"),
            col("state_prov"),
            col("country"),
            col("email1"),
            col("email2"),
            col("local_tax_id"),
            col("national_tax_id"),
            current_timestamp().alias("etl_timestamp"),
        )
        # Placeholder row for late-arriving facts (trade before customer exists)
        placeholder_row = self.spark.range(1).select(
            lit(PLACEHOLDER_CUSTOMER_ID).alias("sk_customer_id"),
            lit(PLACEHOLDER_CUSTOMER_ID).alias("customer_id"),
            lit("Unknown").alias("tax_id"),
            lit("Unknown").alias("status"),
            lit("Unknown").alias("last_name"),
            lit("Unknown").alias("first_name"),
            lit("Unknown").alias("middle_name"),
            lit("Unknown").alias("gender"),
            lit(1).alias("tier"),
            lit(None).cast("date").alias("dob"),
            lit("").alias("address_line1"),
            lit("").alias("address_line2"),
            lit("").alias("postal_code"),
            lit("").alias("city"),
            lit("").alias("state_prov"),
            lit("").alias("country"),
            lit("").alias("email1"),
            lit("").alias("email2"),
            lit("").alias("local_tax_id"),
            lit("").alias("national_tax_id"),
            current_timestamp().alias("etl_timestamp"),
        )
        gold_df = gold_df.unionByName(placeholder_row, allowMissingColumns=True)
        return self._write_gold_table(gold_df, target_table, mode="overwrite")


class GoldDimAccount(GoldLoaderBase):
    """Gold dimension table: DimAccount (current account versions)."""
    
    def load(self, silver_table: str, target_table: str) -> DataFrame:
        """
        Create DimAccount from silver_accounts (current versions only).
        
        Args:
            silver_table: silver_accounts table name
            target_table: gold.DimAccount table name
            
        Returns:
            DataFrame with DimAccount schema
        """
        logger.info(f"Loading gold.DimAccount from {silver_table}")
        
        # Select current versions only
        current_df = self._select_current_version(silver_table)
        
        # Gold dimension: select all columns
        gold_df = current_df.select(
            col("account_id").alias("sk_account_id"),
            col("account_id"),
            col("broker_id"),
            col("customer_id"),
            col("account_name"),
            col("tax_status"),
            col("status_id"),
            current_timestamp().alias("etl_timestamp"),
        )
        # Placeholder row for late-arriving facts (trade before account exists)
        placeholder_row = self.spark.range(1).select(
            lit(PLACEHOLDER_ACCOUNT_ID).alias("sk_account_id"),
            lit(PLACEHOLDER_ACCOUNT_ID).alias("account_id"),
            lit(PLACEHOLDER_ACCOUNT_ID).alias("broker_id"),
            lit(PLACEHOLDER_CUSTOMER_ID).alias("customer_id"),
            lit("Unknown").alias("account_name"),
            lit(0).alias("tax_status"),
            lit("ACTV").alias("status_id"),
            current_timestamp().alias("etl_timestamp"),
        )
        gold_df = gold_df.unionByName(placeholder_row, allowMissingColumns=True)
        return self._write_gold_table(gold_df, target_table, mode="overwrite")


class GoldDimCompany(GoldLoaderBase):
    """Gold dimension table: DimCompany (from silver_companies)."""
    
    def load(self, silver_table: str, target_table: str) -> DataFrame:
        """
        Create DimCompany from silver_companies.
        
        Args:
            silver_table: silver_companies table name
            target_table: gold.DimCompany table name
        """
        logger.info(f"Loading gold.DimCompany from {silver_table}")
        
        silver_df = self.spark.table(silver_table)
        
        # Companies don't have SCD2, so all records are current
        gold_df = silver_df.select(
            col("sk_company_id"),
            col("cik").alias("company_id"),
            col("company_name"),
            col("industry_id"),
            col("sp_rating").alias("sector"),  # Using sp_rating as sector placeholder
            col("status"),
            col("address_line1"),
            col("address_line2"),
            col("postal_code"),
            col("city"),
            col("state_province").alias("state_prov"),
            col("country"),
            col("description"),
            col("founding_date"),
            col("ceo_name"),
            current_timestamp().alias("etl_timestamp"),
        )
        
        return self._write_gold_table(gold_df, target_table, mode="overwrite")


class GoldDimSecurity(GoldLoaderBase):
    """Gold dimension table: DimSecurity (from silver_securities)."""
    
    def load(self, silver_table: str, target_table: str) -> DataFrame:
        """
        Create DimSecurity from silver_securities.
        
        Args:
            silver_table: silver_securities table name
            target_table: gold.DimSecurity table name
        """
        logger.info(f"Loading gold.DimSecurity from {silver_table}")
        
        silver_df = self.spark.table(silver_table)
        
        # Securities don't have SCD2, so all records are current
        gold_df = silver_df.select(
            # Create surrogate key from symbol (or use row_number if needed)
            col("symbol").alias("sk_security_id"),  # Using symbol as SK for now
            col("symbol").alias("security_id"),
            col("symbol"),
            col("issue_type"),
            col("status"),
            col("name"),
            col("ex_id").alias("exchange_id"),
            col("sh_out").alias("shares_outstanding"),
            col("first_trade_date"),
            col("first_trade_exchg").alias("first_trade_exchange"),
            col("dividend"),
            col("co_name_or_cik").alias("company_id"),  # Reference to company
            current_timestamp().alias("etl_timestamp"),
        )
        
        return self._write_gold_table(gold_df, target_table, mode="overwrite")


class GoldDimDate(GoldLoaderBase):
    """Gold dimension table: DimDate (from silver_date)."""
    
    def load(self, silver_table: str, target_table: str) -> DataFrame:
        """
        Create DimDate from silver_date.
        
        Args:
            silver_table: silver_date table name
            target_table: gold.DimDate table name
        """
        logger.info(f"Loading gold.DimDate from {silver_table}")
        
        silver_df = self.spark.table(silver_table)
        
        # Date dimension: all columns (silver_date already has sk_date_id)
        gold_df = silver_df.select(
            col("sk_date_id"),
            col("sk_date_id").alias("date_id"),  # date_id = sk_date_id
            col("date_value"),
            col("date_desc"),
            col("calendar_year_id"),
            col("calendar_year_desc"),
            col("calendar_qtr_id"),
            col("calendar_qtr_desc"),
            col("calendar_month_id"),
            col("calendar_month_desc"),
            col("calendar_week_id"),
            col("calendar_week_desc"),
            col("day_of_week_num"),
            col("day_of_week_desc"),
            col("fiscal_year_id"),
            col("fiscal_year_desc"),
            col("fiscal_qtr_id"),
            col("fiscal_qtr_desc"),
            col("holiday_flag"),
            current_timestamp().alias("etl_timestamp"),
        )
        
        return self._write_gold_table(gold_df, target_table, mode="overwrite")


class GoldDimTradeType(GoldLoaderBase):
    """Gold dimension table: DimTradeType (from silver_trade_type)."""
    
    def load(self, silver_table: str, target_table: str) -> DataFrame:
        """
        Create DimTradeType from silver_trade_type.
        
        Args:
            silver_table: silver_trade_type table name
            target_table: gold.DimTradeType table name
        """
        logger.info(f"Loading gold.DimTradeType from {silver_table}")
        
        silver_df = self.spark.table(silver_table)
        
        gold_df = silver_df.select(
            col("tt_id").alias("sk_trade_type_id"),
            col("tt_id").alias("trade_type_id"),
            col("tt_id").alias("trade_type_code"),
            col("tt_name").alias("trade_type_name"),
            col("tt_is_sell").alias("is_sell"),
            col("tt_is_mrkt").alias("is_market"),
            current_timestamp().alias("etl_timestamp"),
        )
        
        return self._write_gold_table(gold_df, target_table, mode="overwrite")


class GoldDimStatusType(GoldLoaderBase):
    """Gold dimension table: DimStatusType (from silver_status_type)."""
    
    def load(self, silver_table: str, target_table: str) -> DataFrame:
        """
        Create DimStatusType from silver_status_type.
        
        Args:
            silver_table: silver_status_type table name
            target_table: gold.DimStatusType table name
        """
        logger.info(f"Loading gold.DimStatusType from {silver_table}")
        
        silver_df = self.spark.table(silver_table)
        
        gold_df = silver_df.select(
            col("st_id").alias("sk_status_type_id"),
            col("st_id").alias("status_type_id"),
            col("st_id").alias("status_type_code"),
            col("st_name").alias("status_type_name"),
            current_timestamp().alias("etl_timestamp"),
        )
        
        return self._write_gold_table(gold_df, target_table, mode="overwrite")


class GoldDimIndustry(GoldLoaderBase):
    """Gold dimension table: DimIndustry (from silver_industry)."""
    
    def load(self, silver_table: str, target_table: str) -> DataFrame:
        """
        Create DimIndustry from silver_industry.
        
        Args:
            silver_table: silver_industry table name
            target_table: gold.DimIndustry table name
        """
        logger.info(f"Loading gold.DimIndustry from {silver_table}")
        
        silver_df = self.spark.table(silver_table)
        
        # Spec 3.2.13 Industry: IN_ID, IN_NAME, IN_SC_ID only (no sector name in source)
        gold_df = silver_df.select(
            col("in_id").alias("sk_industry_id"),
            col("in_id").alias("industry_id"),
            col("in_name").alias("industry_name"),
            col("in_sc_id").alias("sector_id"),
            lit(None).cast("string").alias("sector_name"),  # Not in spec; leave empty or extend from lookup
            current_timestamp().alias("etl_timestamp"),
        )
        
        return self._write_gold_table(gold_df, target_table, mode="overwrite")
