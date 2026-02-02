# TPC-DI v1.1.0 Spec Alignment

This document maps **source file formats**, **delimiters**, and **schemas** from the [official TPC-DI v1.1.0 specification](https://www.tpc.org/tpc_documents_current_versions/pdf/tpc-di_v1.1.0.pdf) (Clause 2: Source Data Files, Clause 3: Data Warehouse) to this project's Bronze, Silver, and Gold layers.

---

## 1. File format and delimiter (Clause 2)

| Source file | Spec format | Spec delimiter | Bronze read | Silver parse |
|-------------|-------------|----------------|-------------|--------------|
| **Account.txt** | Plain text, variable-length fields | Vertical bar `\|` | `text` (raw line) | `split(raw_line, '\|')` |
| **BatchDate.txt** | Single row, single field | N/A (one field) | — | — |
| **CashTransaction.txt** | Plain text, variable-length fields | Vertical bar `\|` | `text` (raw line) | `split(raw_line, '\|')` |
| **Customer.txt** | Plain text, variable-length fields | Vertical bar `\|` | `text` (raw line) | `split(raw_line, '\|')` |
| **DailyMarket.txt** | Plain text, variable-length fields | Vertical bar `\|` | `text` (raw line) | `split(raw_line, '\|')` |
| **Date.txt** | Plain text, variable-length fields | Vertical bar `\|` | `text` (raw line) | `split(raw_line, '\|')` |
| **FINWIRE*yyyyQq** | Fixed-width (no field separators) | N/A | `text` (raw line) | Substring by position |
| **HoldingHistory.txt** | Plain text, variable-length fields | Vertical bar `\|` | `text` (raw line) | `split(raw_line, '\|')` |
| **HR.csv** | Plain text, variable-length fields | Comma `,` | `text` (raw line) | `split(raw_line, ',')` |
| **Industry.txt** | Plain text, variable-length fields | Vertical bar `\|` | `text` (raw line) | `split(raw_line, '\|')` |
| **Prospect.csv** | Plain text, variable-length fields | Comma `,` | `text` (raw line) | `split(raw_line, ',')` |
| **StatusType.txt** | Plain text, variable-length fields | Vertical bar `\|` | `text` (raw line) | `split(raw_line, '\|')` |
| **TaxRate.txt** | Plain text, variable-length fields | Vertical bar `\|` | `text` (raw line) | `split(raw_line, '\|')` |
| **Time.txt** | Plain text, variable-length fields | Vertical bar `\|` | `text` (raw line) | `split(raw_line, '\|')` |
| **Trade.txt** | Plain text, variable-length fields | Vertical bar `\|` | `text` (raw line) | `split(raw_line, '\|')` |
| **TradeHistory.txt** | Plain text, variable-length fields | Vertical bar `\|` | — (Batch1 only) | — |
| **TradeType.txt** | Plain text, variable-length fields | Vertical bar `\|` | `text` (raw line) | `split(raw_line, '\|')` |
| **WatchHistory.txt** | Plain text, variable-length fields | Vertical bar `\|` | `text` (raw line) | `split(raw_line, '\|')` |
| **CustomerMgmt.xml** | XML document | N/A | spark-xml / UDTF | Nested struct / UDTF |

- **Null values** (spec 2.2.2.1): no characters between delimiters (e.g. `\|\|`).
- **Character set**: single-byte UTF-8 (spec 2.2.1.2).

---

## 2. Source file field counts and spec column names

### 2.1 Pipe-delimited (Historical Load = no CDC columns; Incremental = CDC_FLAG, CDC_DSN first)

| File | Historical cols | Incremental cols | Spec field names (order) |
|------|-----------------|------------------|---------------------------|
| **Account.txt** | — | 8 | CDC_FLAG, CDC_DSN, CA_ID, CA_B_ID, CA_C_ID, CA_NAME, CA_TAX_ST, CA_ST_ID |
| **CashTransaction.txt** | 4 | 6 | Hist: CT_CA_ID, CT_DTS, CT_AMT, CT_NAME. Incr: CDC_FLAG, CDC_DSN, CT_CA_ID, CT_DTS, CT_AMT, CT_NAME |
| **Customer.txt** | — | many | CDC_FLAG, CDC_DSN, C_ID, C_TAX_ID, C_ST_ID, C_L_NAME, C_F_NAME, … |
| **DailyMarket.txt** | 6 | 8 | Hist: DM_DATE, DM_S_SYMB, DM_CLOSE, DM_HIGH, DM_LOW, DM_VOL. Incr: + CDC_FLAG, CDC_DSN |
| **Date.txt** | 18 | — | SK_DateID, DateValue, DateDesc, CalendarYearID, … HolidayFlag |
| **HoldingHistory.txt** | 4 | 6 | Hist: HH_H_T_ID, HH_T_ID, HH_BEFORE_QTY, HH_AFTER_QTY. Incr: + CDC_FLAG, CDC_DSN |
| **Industry.txt** | 3 | — | IN_ID, IN_NAME, IN_SC_ID |
| **StatusType.txt** | 2 | — | ST_ID, ST_NAME |
| **TaxRate.txt** | 3 | — | TX_ID, TX_NAME, TX_RATE |
| **Time.txt** | 10 | — | SK_TimeID, TimeValue, HourID, … OfficeHoursFlag |
| **Trade.txt** | 16 | 18 | Hist: T_ID, T_DTS, T_ST_ID, … Incr: + CDC_FLAG, CDC_DSN |
| **TradeType.txt** | 4 | — | TT_ID, TT_NAME, TT_IS_SELL, TT_IS_MRKT |
| **WatchHistory.txt** | 4 | 6 | Hist: W_C_ID, W_S_SYMB, W_DTS, W_ACTION. Incr: + CDC_FLAG, CDC_DSN |

### 2.2 Comma-delimited

| File | Cols | Spec field names (order) |
|------|------|---------------------------|
| **HR.csv** | 9 | EmployeeID, ManagerID, EmployeeFirstName, EmployeeLastName, EmployeeMI, EmployeeJobCode, EmployeeBranch, EmployeeOffice, EmployeePhone |
| **Prospect.csv** | 23 | AgencyID, LastName, FirstName, MiddleInitial, Gender, AddressLine1, AddressLine2, PostalCode, City, State, Country, Phone, Income, NumberCars, NumberChildren, MaritalStatus, Age, CreditRating, OwnOrRentFlag, Employer, NumberCreditCards, NetWorth |

### 2.3 FINWIRE (fixed-width)

- **CMP**: PTS(15), RecType(3)=CMP, CompanyName(60), CIK(10), Status(4), IndustryID(2), SPrating(4), FoundingDate(8), AddrLine1(80), AddrLine2(80), PostalCode(12), City(25), StateProvince(20), Country(24), CEOname(46), Description(150).
- **SEC**: PTS(15), RecType(3)=SEC, Symbol(15), IssueType(6), Status(4), Name(70), ExID(6), ShOut(13), FirstTradeDate(8), FirstTradeExchg(8), Dividend(12), CoNameOrCIK(60 or 10).
- **FIN**: PTS(15), RecType(3)=FIN, Year(4), Quarter(1), QtrStartDate(8), PostingDate(8), Revenue(17), Earnings(17), EPS(12), DilutedEPS(12), Margin(12), Inventory(17), Assets(17), Liabilities(17), ShOut(13), DilutedShOut(13), CoNameOrCIK(60 or 10).

---

## 3. Schema alignment: source → silver → gold vs spec

### 3.1 CashTransaction (spec Table 2.2.5)

- **Spec**: CT_CA_ID (Customer account identifier), CT_DTS, CT_AMT, CT_NAME. Incremental: CDC_FLAG, CDC_DSN first.
- **This project**: Silver uses `ct_ca_id` (spec) and `account_id = ct_ca_id` for gold joins. Key: `ct_key = ct_ca_id + ct_dts` (or equivalent). Gold FactCashBalances: SK_AccountID, SK_DateID, Cash (sum of CT_AMT by account/date).

### 3.2 WatchHistory (spec Table 2.2.18)

- **Spec**: W_C_ID (Customer identifier), W_S_SYMB, W_DTS, W_ACTION. Incremental: CDC_FLAG, CDC_DSN first.
- **This project**: Silver uses `w_c_id` (customer id), `w_s_symb`, `w_dts`, `w_action`. Composite key: `w_c_id + w_s_symb` (per spec FactWatches: customer + security).

### 3.3 Industry (spec Table 2.2.11, DW 3.2.13)

- **Spec**: IN_ID, IN_NAME, IN_SC_ID only (3 columns). Data Warehouse Industry table: IN_ID, IN_NAME, IN_SC_ID.
- **This project**: Silver has 3 columns (in_id, in_name, in_sc_id). No IN_SC_NAME in spec; gold sector_name derived or omitted.

### 3.4 Data Warehouse table names (spec Clause 3.2)

- Spec uses: DimAccount, DimBroker, DimCompany, DimCustomer, DimDate, DimSecurity, DimTime, DimTrade, DImessages, FactCashBalances, FactHoldings, FactMarketHistory, FactWatches, Industry, Financial, Prospect, StatusType, TaxRate, TradeType, Audit.
- This project gold: gold_dim_account, gold_dim_customer, gold_dim_date, gold_dim_company, gold_dim_security, gold_fact_trade, gold_fact_market_history, gold_fact_holdings, gold_fact_cash_balances, gold_fact_watches, etc. (snake_case with gold_ prefix).

---

## 4. Changes made for spec compliance

1. **CashTransaction.txt**: Use **CT_CA_ID** (account id), not T_ID. Incremental = 6 columns (CDC_FLAG, CDC_DSN, CT_CA_ID, CT_DTS, CT_AMT, CT_NAME); Historical = 4 (CT_CA_ID, CT_DTS, CT_AMT, CT_NAME). Silver: `ct_ca_id`, `account_id = ct_ca_id` for gold.
2. **WatchHistory.txt**: Use **W_C_ID** (customer id), not WH_W_ID. Silver: `w_c_id`, `wh_key = w_c_id + w_s_symb`.
3. **Industry.txt**: Exactly **3** fields (IN_ID, IN_NAME, IN_SC_ID). Silver: no fourth column; gold Industry/DimIndustry only IN_ID, IN_NAME, IN_SC_ID per spec.

---

## 5. Reference: spec clause and table numbers

- **Source file formats**: Clause 2.2 (Tables 2.2.1–2.2.19).
- **Data types (source)**: Table 2.2.1 (BOOLEAN, CHAR(n), DATE, DATETIME, NUM, SNUM), Table 2.2.2 (meta-types).
- **Data Warehouse tables**: Clause 3.2 (Tables 3.2.3–3.2.23).
- **Staging structure**: Clause 2.3 (Batch1, Batch2, Batch3).

## 6. Generator variance

If your data generator (e.g. DIGen/PDGF) omits CDC_DSN for incremental files (e.g. 5 columns instead of 6 for CashTransaction or WatchHistory), adjust the silver loaders: use `num_cols = 5` and `offset = 1` (skip only CDC_FLAG) so that data columns align with the spec field order.
