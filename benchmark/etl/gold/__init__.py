"""
Gold Layer ETL - Business-Ready Analytics Tables for TPC-DI Benchmark.

The Gold layer provides query-optimized, business-ready tables:
- Current versions only (no SCD Type 2 complexity)
- Denormalized star schema (facts + dimensions)
- Pre-joined tables ready for BI tools and analytics

Transforms Silver cleaned data into Gold star schema tables.
"""

import logging
from typing import TYPE_CHECKING

# Import dimension loaders
from benchmark.etl.gold.dimensions import (
    GoldDimCustomer,
    GoldDimAccount,
    GoldDimCompany,
    GoldDimSecurity,
    GoldDimDate,
    GoldDimTradeType,
    GoldDimStatusType,
    GoldDimIndustry,
)

# Import fact loaders
from benchmark.etl.gold.facts import (
    GoldFactTrade,
    GoldFactMarketHistory,
    GoldFactCashBalances,
    GoldFactHoldings,
)
from benchmark.etl.table_timing import start_table as table_timing_start

if TYPE_CHECKING:
    from benchmark.platforms.databricks import DatabricksPlatform
    from benchmark.platforms.dataproc import DataprocPlatform

logger = logging.getLogger(__name__)

# Export all loader classes
__all__ = [
    "GoldETL",
    "GoldDimCustomer",
    "GoldDimAccount",
    "GoldDimCompany",
    "GoldDimSecurity",
    "GoldDimDate",
    "GoldDimTradeType",
    "GoldDimStatusType",
    "GoldDimIndustry",
    "GoldFactTrade",
    "GoldFactMarketHistory",
    "GoldFactCashBalances",
    "GoldFactHoldings",
]


class GoldETL:
    """
    Gold Layer ETL orchestrator for TPC-DI.
    
    Coordinates all Gold layer loaders to transform Silver data
    into business-ready star schema tables.
    """
    
    def __init__(self, platform):
        """
        Initialize Gold ETL orchestrator.
        
        Args:
            platform: Platform adapter (DatabricksPlatform or DataprocPlatform)
        """
        self.platform = platform
        
        # Initialize dimension loaders
        self.dim_customer = GoldDimCustomer(platform)
        self.dim_account = GoldDimAccount(platform)
        self.dim_company = GoldDimCompany(platform)
        self.dim_security = GoldDimSecurity(platform)
        self.dim_date = GoldDimDate(platform)
        self.dim_trade_type = GoldDimTradeType(platform)
        self.dim_status_type = GoldDimStatusType(platform)
        self.dim_industry = GoldDimIndustry(platform)
        
        # Initialize fact loaders
        self.fact_trade = GoldFactTrade(platform)
        self.fact_market_history = GoldFactMarketHistory(platform)
        self.fact_cash_balances = GoldFactCashBalances(platform)
        self.fact_holdings = GoldFactHoldings(platform)
        
        logger.info("Initialized GoldETL orchestrator")

    def run_gold_load(self, target_database: str, target_schema: str):
        """
        Run full Gold layer load.
        
        Reads from Silver tables and creates Gold star schema tables.
        Dimensions must be loaded before facts (for joins).
        
        Args:
            target_database: Target database/catalog name
            target_schema: Target schema name
        """
        prefix = ".".join(p for p in (target_database, target_schema) if p)
        
        logger.info("Starting Gold layer load")
        
        # Step 1: Load dimension tables (must be done first)
        logger.info("Loading Gold dimension tables...")
        
        table_timing_start(f"{prefix}.gold_dim_date")
        self.dim_date.load(
            f"{prefix}.silver_date",
            f"{prefix}.gold_dim_date"
        )
        
        table_timing_start(f"{prefix}.gold_dim_customer")
        self.dim_customer.load(
            f"{prefix}.silver_customers",
            f"{prefix}.gold_dim_customer"
        )
        
        table_timing_start(f"{prefix}.gold_dim_account")
        self.dim_account.load(
            f"{prefix}.silver_accounts",
            f"{prefix}.gold_dim_account"
        )
        
        table_timing_start(f"{prefix}.gold_dim_company")
        self.dim_company.load(
            f"{prefix}.silver_companies",
            f"{prefix}.gold_dim_company"
        )

        table_timing_start(f"{prefix}.gold_dim_security")
        self.dim_security.load(
            f"{prefix}.silver_securities",
            f"{prefix}.gold_dim_security"
        )
        
        table_timing_start(f"{prefix}.gold_dim_trade_type")
        self.dim_trade_type.load(
            f"{prefix}.silver_trade_type",
            f"{prefix}.gold_dim_trade_type"
        )
        
        table_timing_start(f"{prefix}.gold_dim_status_type")
        self.dim_status_type.load(
            f"{prefix}.silver_status_type",
            f"{prefix}.gold_dim_status_type"
        )
        
        table_timing_start(f"{prefix}.gold_dim_industry")
        self.dim_industry.load(
            f"{prefix}.silver_industry",
            f"{prefix}.gold_dim_industry"
        )
        
        logger.info("Gold dimension tables loaded")
        
        # Step 2: Load fact tables (join with dimensions)
        logger.info("Loading Gold fact tables...")
        
        table_timing_start(f"{prefix}.gold_fact_trade")
        self.fact_trade.load(
            f"{prefix}.silver_trades",
            f"{prefix}.gold_fact_trade",
            dim_customer_table=f"{prefix}.gold_dim_customer",
            dim_account_table=f"{prefix}.gold_dim_account",
            dim_security_table=f"{prefix}.gold_dim_security",
            dim_date_table=f"{prefix}.gold_dim_date",
            dim_trade_type_table=f"{prefix}.gold_dim_trade_type",
        )

        table_timing_start(f"{prefix}.gold_fact_market_history")
        self.fact_market_history.load(
            f"{prefix}.silver_daily_market",
            f"{prefix}.gold_fact_market_history",
            dim_date_table=f"{prefix}.gold_dim_date",
            dim_security_table=f"{prefix}.gold_dim_security",
        )
        
        # Optional fact tables (may not exist yet)
        try:
            table_timing_start(f"{prefix}.gold_fact_cash_balances")
            self.fact_cash_balances.load(
                f"{prefix}.silver_cash_transaction",
                f"{prefix}.gold_fact_cash_balances",
                dim_date_table=f"{prefix}.gold_dim_date",
                dim_account_table=f"{prefix}.gold_dim_account",
            )
        except Exception as e:
            logger.warning(f"FactCashBalances skipped: {e}")
        
        try:
            table_timing_start(f"{prefix}.gold_fact_holdings")
            self.fact_holdings.load(
                f"{prefix}.silver_holding_history",
                f"{prefix}.gold_fact_holdings",
                dim_date_table=f"{prefix}.gold_dim_date",
                dim_account_table=f"{prefix}.gold_dim_account",
                dim_security_table=f"{prefix}.gold_dim_security",
            )
        except Exception as e:
            logger.warning(f"FactHoldings skipped: {e}")
        
        logger.info("Gold layer load completed")
