# CustomerMgmt.xml Structure (TPC-DI Batch Load)

Derived from sample `CustomerMgmt.xml` for batch load (Batch1).

## Root and row elements

```xml
<?xml version="1.0" encoding="UTF-8"?>
<TPCDI:Actions xmlns:TPCDI="http://www.tpc.org/tpc-di">
  <TPCDI:Action ActionType="NEW" ActionTS="2007-07-07T02:56:25">
    ...
  </TPCDI:Action>
</TPCDI:Actions>
```

- **Root**: `<TPCDI:Actions xmlns:TPCDI="http://www.tpc.org/tpc-di">`
- **Row tag**: `<TPCDI:Action>`
- **Action attributes**: `ActionType`, `ActionTS`  
  (spark-xml exposes as `_ActionType`, `_ActionTS`)

Use `rowTag="TPCDI:Action"` and `rootTag="TPCDI:Actions"` when reading with spark-xml.

## Action structure (batch load: all `ActionType="NEW"`)

Each `<TPCDI:Action>` contains a single `<Customer>`, which contains a single `<Account>`.

### Customer (attributes)

| Attribute | Example    | Spark-xml column |
|----------|------------|-------------------|
| C_ID     | "0"        | Customer._C_ID    |
| C_TAX_ID | "923-54-6498" | Customer._C_TAX_ID |
| C_GNDR   | "F"        | Customer._C_GNDR  |
| C_TIER   | "3"        | Customer._C_TIER  |
| C_DOB    | "1940-12-02" | Customer._C_DOB  |

### Customer.Name (elements)

| Element  | Example | Spark-xml column           |
|----------|---------|----------------------------|
| C_L_NAME | Joannis | Customer.Name.C_L_NAME     |
| C_F_NAME | Adara   | Customer.Name.C_F_NAME     |
| C_M_NAME | (empty) | Customer.Name.C_M_NAME     |

### Customer.Address (elements)

| Element     | Example              | Spark-xml column                |
|-------------|----------------------|---------------------------------|
| C_ADLINE1   | 4779 Weller Way      | Customer.Address.C_ADLINE1      |
| C_ADLINE2   | (empty)              | Customer.Address.C_ADLINE2      |
| C_ZIPCODE   | 92624                | Customer.Address.C_ZIPCODE      |
| C_CITY      | Columbus             | Customer.Address.C_CITY         |
| C_STATE_PROV| Ontario              | Customer.Address.C_STATE_PROV   |
| C_CTRY      | Canada               | Customer.Address.C_CTRY         |

### Customer.ContactInfo (elements)

| Element      | Example                    | Spark-xml column                    |
|--------------|----------------------------|-------------------------------------|
| C_PRIM_EMAIL | Adara.Joannis@moose-mail.com | Customer.ContactInfo.C_PRIM_EMAIL |
| C_ALT_EMAIL  | Adara.Joannis@gmx.com      | Customer.ContactInfo.C_ALT_EMAIL    |
| C_PHONE_1    | nested (C_CTRY_CODE, C_AREA_CODE, C_LOCAL, C_EXT) | Customer.ContactInfo.C_PHONE_1 |
| C_PHONE_2, C_PHONE_3 | similar               | ...                                 |

### Customer.TaxInfo (elements)

| Element     | Example | Spark-xml column              |
|-------------|---------|-------------------------------|
| C_LCL_TX_ID | CA3     | Customer.TaxInfo.C_LCL_TX_ID  |
| C_NAT_TX_ID | YT3     | Customer.TaxInfo.C_NAT_TX_ID  |

### Customer.Account (attributes and elements)

| Name     | Type      | Example | Spark-xml column                  |
|----------|-----------|---------|-----------------------------------|
| CA_ID    | attribute | "0"     | Customer.Account._CA_ID           |
| CA_TAX_ST| attribute | "1"     | Customer.Account._CA_TAX_ST       |
| CA_B_ID  | element   | 17713   | Customer.Account.CA_B_ID (broker) |
| CA_NAME  | element   | CJlm... | Customer.Account.CA_NAME          |

- `CA_B_ID`: broker ID (maps to DimBroker / SK_BrokerID).
- `CA_NAME`: account name (maps to AccountDesc).

## DimAccount mapping (from CustomerMgmt)

| DimAccount column | Source                                |
|-------------------|----------------------------------------|
| SK_AccountID      | Customer.Account._CA_ID                |
| SK_BrokerID       | Customer.Account.CA_B_ID (cast bigint) |
| SK_CustomerID     | Customer._C_ID                         |
| Status            | _ActionType                            |
| AccountDesc       | Customer.Account.CA_NAME               |
| TaxStatus         | Customer.Account._CA_TAX_ST            |
| IsActive          | `False` if ActionType=INACT else `True`|

## DimCustomer mapping (from CustomerMgmt)

| DimCustomer column | Source |
|--------------------|--------|
| SK_CustomerID      | Customer._C_ID |
| TaxID              | Customer._C_TAX_ID |
| Status             | _ActionType |
| LastName, FirstName, MiddleInitial | Customer.Name.C_L_NAME, C_F_NAME, C_M_NAME |
| Gender, Tier, DOB  | Customer._C_GNDR, _C_TIER, _C_DOB |
| Address, City, StateProv, Country, PostalCode | Customer.Address.* |
| Email1, Email2     | Customer.ContactInfo.C_PRIM_EMAIL, C_ALT_EMAIL |
| Phone1, Phone2, Phone3 | Customer.ContactInfo.C_PHONE_1/2/3 (e.g. C_LOCAL) |

## Incremental batches (Batch2, Batch3)

Same XML layout. `ActionType` can be:

- **NEW** – new customer (+ account)
- **ADDACCT** – add account to existing customer
- **UPDCUST** – update customer
- **UPDACCT** – update account
- **CLOSEACCT** – close account
- **INACT** – deactivate customer

Some actions may have only `Customer` or only `Account`; filter or join as needed for DimCustomer vs DimAccount.
