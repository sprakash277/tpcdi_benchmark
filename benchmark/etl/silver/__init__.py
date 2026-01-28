"""
Silver Layer ETL - Cleaned and Refined Data for TPC-DI Benchmark.

The Silver layer is where the "heavy lifting" of the TPC-DI specification happens:
- Parse complex formats (XML, fixed-width)
- Type conversions (strings to decimals/timestamps)
- SCD Type 2 handling for slowly changing dimensions
- Incremental merge logic

Transforms Bronze raw data into clean, typed, versioned tables.
"""

import logging
from typing import TYPE_CHECKING

# Import individual loaders
from benchmark.etl.silver.customers import SilverCustomers
from benchmark.etl.silver.accounts import SilverAccounts
from benchmark.etl.silver.companies import SilverCompanies
from benchmark.etl.silver.securities import SilverSecurities
from benchmark.etl.silver.financials import SilverFinancials
from benchmark.etl.silver.trades import SilverTrades
from benchmark.etl.silver.daily_market import SilverDailyMarket
from benchmark.etl.silver.reference import (
    SilverDate, SilverStatusType, SilverTradeType, SilverIndustry
)

if TYPE_CHECKING:
    from benchmark.platforms.databricks import DatabricksPlatform
    from benchmark.platforms.dataproc import DataprocPlatform

logger = logging.getLogger(__name__)

# Export all loader classes
__all__ = [
    "SilverETL",
    "SilverCustomers",
    "SilverAccounts",
    "SilverCompanies",
    "SilverSecurities",
    "SilverFinancials",
    "SilverTrades",
    "SilverDailyMarket",
    "SilverDate",
    "SilverStatusType",
    "SilverTradeType",
    "SilverIndustry",
]


class SilverETL:
    """
    Silver Layer ETL orchestrator for TPC-DI.
    
    Coordinates all Silver layer loaders to transform Bronze data
    into cleaned, typed, versioned tables.
    """
    
    def __init__(self, platform):
        """
        Initialize Silver ETL orchestrator.
        
        Args:
            platform: Platform adapter (DatabricksPlatform or DataprocPlatform)
        """
        self.platform = platform
        
        # Initialize all loaders
        self.customers = SilverCustomers(platform)
        self.accounts = SilverAccounts(platform)
        self.companies = SilverCompanies(platform)
        self.securities = SilverSecurities(platform)
        self.financials = SilverFinancials(platform)
        self.trades = SilverTrades(platform)
        self.daily_market = SilverDailyMarket(platform)
        self.date = SilverDate(platform)
        self.status_type = SilverStatusType(platform)
        self.trade_type = SilverTradeType(platform)
        self.industry = SilverIndustry(platform)
        
        logger.info("Initialized SilverETL orchestrator")
    
    def run_silver_batch_load(self, batch_id: int, target_database: str, target_schema: str):
        """
        Run full Silver layer load for a batch.
        
        Reads from Bronze tables and creates cleaned Silver tables.
        
        Args:
            batch_id: Batch number (1 for historical, 2+ for incremental)
            target_database: Target database/catalog name
            target_schema: Target schema name
        """
        prefix = f"{target_database}.{target_schema}"
        
        logger.info(f"Starting Silver layer load for Batch{batch_id}")
        
        # Reference data (Batch1 only)
        if batch_id == 1:
            self.date.load(f"{prefix}.bronze_date", f"{prefix}.silver_date")
            self.status_type.load(f"{prefix}.bronze_status_type", f"{prefix}.silver_status_type")
            self.trade_type.load(f"{prefix}.bronze_trade_type", f"{prefix}.silver_trade_type")
            self.industry.load(f"{prefix}.bronze_industry", f"{prefix}.silver_industry")
            
            # FINWIRE parsing (Batch1 only)
            try:
                self.companies.load(f"{prefix}.bronze_finwire", f"{prefix}.silver_companies")
                self.securities.load(f"{prefix}.bronze_finwire", f"{prefix}.silver_securities")
                self.financials.load(f"{prefix}.bronze_finwire", f"{prefix}.silver_financials")
            except Exception as e:
                logger.warning(f"FINWIRE parsing skipped: {e}")
        
        # Customer and Account data (all batches)
        self.customers.load(f"{prefix}.bronze_customer_mgmt", f"{prefix}.silver_customers", batch_id)
        self.accounts.load(f"{prefix}.bronze_customer_mgmt", f"{prefix}.silver_accounts", batch_id)
        
        # Trade and Market data (all batches)
        try:
            self.trades.load(f"{prefix}.bronze_trade", f"{prefix}.silver_trades", batch_id)
        except Exception as e:
            logger.warning(f"Trade data skipped: {e}")
        
        try:
            self.daily_market.load(f"{prefix}.bronze_daily_market", f"{prefix}.silver_daily_market", batch_id)
        except Exception as e:
            logger.warning(f"Daily market data skipped: {e}")
        
        logger.info(f"Silver layer load completed for Batch{batch_id}")
