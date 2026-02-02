# Schema Comparison: tpcdi_benchmark vs shannon-barrow/databricks-tpc-di

This document compares table schemas between this project (tpcdi_benchmark) and the [Databricks TPC-DI reference implementation](https://github.com/shannon-barrow/databricks-tpc-di), which follows the official **TPC-DI v1.1.0** specification.

---

## Architecture Differences

| Aspect | tpcdi_benchmark | databricks-tpc-di |
|--------|-----------------|-------------------|
| **Pattern** | Medallion (Bronze → Silver → Gold) | Raw → Stage → DW (direct to spec) |
| **Bronze** | Raw text lines (`raw_line`) + metadata | FinWire parsed; incremental stage tables |
| **Silver** | Parsed, cleaned, SCD2 | Stage tables (CustomerIncremental, etc.) + Dim*/Fact* |
| **Gold** | Denormalized star schema | TPC-DI spec tables (DimCustomer, FactHoldings, etc.) |
| **Naming** | `gold_dim_*`, `gold_fact_*` | `DimCustomer`, `FactHoldings` (spec names) |

---

## Tables in databricks-tpc-di (from dw_init.sql)

### Stage / Incremental Tables
- `FinWire`, `CustomerIncremental`, `AccountIncremental`, `ProspectIncremental`
- `WatchIncremental`, `DailyMarketIncremental`, `CashTransactionIncremental`, `HoldingIncremental`, `TradeIncremental`
- `CompanyFinancialsStg`

### Reference / Dimension Tables
- `TaxRate`, `BatchDate`, `DimDate`, `DimTime`, `StatusType`, `industry`, `TradeType`
- `DimBroker`, `DimCustomer`, `DimAccount`, `DimCompany`, `DimSecurity`, `Prospect`, `Financial`
- `DimTrade`

### Fact Tables
- `FactHoldings`, `FactCashBalances`, `FactMarketHistory`, `FactWatches`

---

## Schema Comparison by Table

### DimCustomer

| Column | databricks-tpc-di | tpcdi_benchmark (gold_dim_customer) |
|--------|-------------------|-------------------------------------|
| Surrogate key | `sk_customerid` BIGINT | `sk_customer_id` BIGINT ✓ |
| Customer ID | `customerid` BIGINT | `customer_id` BIGINT ✓ |
| Tax ID | `taxid` | `tax_id` ✓ |
| Status | `status` | `status` ✓ |
| Name | `lastname`, `firstname`, `middleinitial` | `last_name`, `first_name`, `middle_name` ✓ |
| Gender, Tier, DOB | ✓ | ✓ |
| Address | `addressline1`, `addressline2`, `postalcode`, `city`, `stateprov`, `country` | `address_line1`, `address_line2`, `postal_code`, `city`, `state_prov`, `country` ✓ |
| Phones | `phone1`, `phone2`, `phone3` | **Missing** |
| Emails | `email1`, `email2` | ✓ |
| Tax rates | `nationaltaxratedesc`, `nationaltaxrate`, `localtaxratedesc`, `localtaxrate` | `local_tax_id`, `national_tax_id` (IDs only, no rate values) |
| Agency / Prospect | `agencyid`, `creditrating`, `networth`, `marketingnameplate` | **Missing** |
| SCD2 | `iscurrent`, `effectivedate`, `enddate`, `batchid` | Not in gold (in silver only) |

### DimAccount

| Column | databricks-tpc-di | tpcdi_benchmark (gold_dim_account) |
|--------|-------------------|------------------------------------|
| Surrogate key | `sk_accountid` BIGINT | `sk_account_id` BIGINT ✓ |
| Account ID | `accountid` | `account_id` ✓ |
| Broker | `sk_brokerid` BIGINT (surrogate FK) | `broker_id` BIGINT (natural key) |
| Customer | `sk_customerid` | `customer_id` (natural, not surrogate) |
| Account desc | `accountdesc` | `account_name` ✓ |
| Tax status | `taxstatus` TINYINT | `tax_status` INT ✓ |
| Status | `status` | `status_id` ✓ |
| SCD2 | `iscurrent`, `effectivedate`, `enddate` | Not in gold |

### DimCompany

| Column | databricks-tpc-di | tpcdi_benchmark (gold_dim_company) |
|--------|-------------------|------------------------------------|
| Surrogate key | `sk_companyid` BIGINT | `sk_company_id` BIGINT ✓ |
| Company ID | `companyid` BIGINT | `company_id` STRING ✓ |
| Name | `name` | `company_name` ✓ |
| Industry | `industry` STRING (industry name, FK) | `industry_id` STRING ✓ |
| Rating | `sprating` | `sector` (using sp_rating as sector placeholder) |
| Extra | `islowgrade` BOOLEAN | **Missing** |
| CEO, address, etc. | ✓ | ✓ (slightly different names) |
| SCD2 | `iscurrent`, `effectivedate`, `enddate` | Not in gold |

### DimSecurity

| Column | databricks-tpc-di | tpcdi_benchmark (gold_dim_security) |
|--------|-------------------|--------------------------------------|
| Surrogate key | `sk_securityid` BIGINT | `sk_security_id` STRING (uses symbol!) |
| Symbol | `symbol` | `symbol` ✓ |
| Issue type | `issue` | `issue_type` ✓ |
| Status, Name | ✓ | ✓ |
| Exchange | `exchangeid` | `exchange_id` ✓ |
| Company | `sk_companyid` BIGINT | `company_id` STRING |
| Shares, dates | ✓ | ✓ |
| Dividend | ✓ | ✓ |
| SCD2 | `iscurrent`, `effectivedate`, `enddate` | Not in gold |

### FactMarketHistory

| Column | databricks-tpc-di | tpcdi_benchmark (gold_fact_market_history) |
|--------|-------------------|--------------------------------------------|
| Keys | `sk_securityid`, `sk_companyid`, `sk_dateid` | `sk_date_id`, `sk_security_id` |
| Price / volume | `closeprice`, `dayhigh`, `daylow`, `volume` | `close_price`, `high_price`, `low_price`, `volume` ✓ |
| 52-week high/low | `fiftytwoweekhigh`, `sk_fiftytwoweekhighdate`, `fiftytwoweeklow`, `sk_fiftytwoweeklowdate` | **Missing** |
| Ratios | `peratio`, `yield` | **Missing** |
| Denormalized | — | `symbol`, `market_date` |

### FactHoldings

| Column | databricks-tpc-di | tpcdi_benchmark (gold_fact_holdings) |
|--------|-------------------|--------------------------------------|
| Trade keys | `tradeid`, `currenttradeid` | — |
| Surrogate keys | `sk_customerid`, `sk_accountid`, `sk_securityid`, `sk_companyid`, `sk_dateid`, `sk_timeid` | `sk_date_id`, `sk_account_id`, `sk_security_id` |
| Measures | `currentprice`, `currentholding` | `quantity`, `purchase_price`, `purchase_date` |
| Structure | One row per holding update; links to DimTrade | One row per holding from HoldingHistory; uses purchase_price from trade |

### FactCashBalances

| Column | databricks-tpc-di | tpcdi_benchmark (gold_fact_cash_balances) |
|--------|-------------------|------------------------------------------|
| Keys | `sk_customerid`, `sk_accountid`, `sk_dateid` (composite PK) | `sk_date_id`, `sk_account_id`, `account_id` |
| Cash | `cash` | `cash_balance` ✓ |
| Extra | `batchid` | `transaction_count` |

---

## Tables in databricks-tpc-di NOT in tpcdi_benchmark

| Table | Description |
|-------|-------------|
| **DimBroker** | Broker dimension; tpcdi_benchmark uses `broker_id` (natural key) in DimAccount |
| **DimTrade** | Trade dimension; tpcdi_benchmark has `gold_fact_trade` (fact) instead |
| **FactWatches** | Watch list fact; tpcdi_benchmark has `silver_watch_history` but no gold fact |
| **Financial** | Financial facts by company/quarter; tpcdi_benchmark has `silver_financials` only |
| **BatchDate** | Batch metadata; not in tpcdi_benchmark |
| **DimTime** | Time dimension; tpcdi_benchmark has `bronze_time` but no gold DimTime |
| **industry** | Lowercase; tpcdi_benchmark has `silver_industry` / `gold_dim_industry` |

---

## Tables in tpcdi_benchmark NOT in databricks-tpc-di (as named)

| Table | Notes |
|-------|-------|
| **gold_fact_trade** | Denormalized trade fact; databricks has DimTrade (dimension) |
| **gold_dim_trade_type** | databricks has TradeType (reference) |
| **gold_dim_status_type** | databricks has StatusType |
| **gold_dim_industry** | databricks has `industry` |
| **Bronze layer** | tpcdi_benchmark uses raw_line; databricks uses stage tables |

---

## Summary of Main Differences

1. **TPC-DI spec alignment**: databricks-tpc-di follows the spec closely (column names, FKs, PKs, partitioning). tpcdi_benchmark uses a simplified star schema with some spec deviations.
2. **Naming**: tpcdi_benchmark uses snake_case (`sk_customer_id`); databricks uses spec camelCase (`sk_customerid`).
3. **Surrogate keys**: databricks uses BIGINT surrogates throughout; tpcdi_benchmark sometimes uses natural keys (e.g. `sk_security_id` = symbol).
4. **SCD2 in gold**: databricks keeps `iscurrent`, `effectivedate`, `enddate` in gold dimensions; tpcdi_benchmark keeps them in silver and filters with `_select_current_version`.
5. **DimBroker**: databricks has a broker dimension; tpcdi_benchmark does not.
6. **FactMarketHistory**: databricks includes 52-week high/low and ratios; tpcdi_benchmark does not.
7. **FactWatches**: databricks has a gold fact; tpcdi_benchmark stops at silver_watch_history.

---

## Reference

- **databricks-tpc-di**: https://github.com/shannon-barrow/databricks-tpc-di  
- **Schema source**: `src/incremental_batches/dw_init.sql`
