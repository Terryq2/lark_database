# README

## Overview

This repository was developed as part of a data analytics internship at a local cinema in Beijing, China.
It provides an **automated data synchronization interface** that coordinates between  [`Yuekeyun`](https://lark.yuekeyun.com/) (a system that Cinemas uses to log and track data) and [`Feishu`](https://www.feishu.cn).

At its core is the `DataSyncClient` class, which manages:

* Downloading financial or screening data from **Yuekeyun**.
* Merging and cleaning CSV results.
* Uploading the processed data to **Feishu Tables** or Wiki-linked spreadsheets.
* Handling special upload modes such as:

  * By date ranges
  * By quarter
  * Current-year-to-date
  * Most recent N days
  * Future screening schedules

The module is designed for **scheduled jobs** and **daily financial data sync tasks**, but can also be run on-demand.

A version of this library is currently hosted on a Aliyun server and syncs data every hour.

---

## Features

* **API Orchestration** — Manages credentials and configuration for both Yuekeyun and Feishu.
* **Flexible Queries** — Support for daily, monthly, and quarterly queries.
* **Data Merging** — Combines multiple CSVs from Yuekeyun into a single cleaned dataset.
* **Multiple Sync Modes**:

  * Most recent N days (`sync_most_recent_data`)
  * Current year data, with or without quarterly breakdown (`_upload_current_year_data`)
  * Yesterday-only sync (`sync_all_yesterday`)
  * Screening data with future schedule support (`sync_screening_data`)
* **Automated Table Name Composition** — Dynamically generates target table names (including quarter/year suffixes).
* **Safe Deletion** — Removes outdated records before inserting fresh data.

---
## Necessary environment variables variables
The following are variables which should be provided by Yuekeyun on demand.
* **SECRET_KEY**: This is the secret key provided by Yuekeyun
* **APP_KEY**: This is the app key provided by Yuekeyun
* **LEASE_CODE**: This is the id of your cinema provided by Yuekeyun
* **CHANNEL_CODE**: This is the channel code provided by Yuekeyun
* **CINEMA_LINK_ID**: This is the link id provided by Yuekeyun

* **FEISHU_APP_KEY**: This is the app key granted by Feishu. We use it to access data in your organization.
* **FEISHU_APP_SECRET**: This is the app key granted by Feishu. We use it to access data in your organization.
<br>

* **WIKI_APP_TOKEN**: This is the app token of the table you want to write into. Per the official guidelines, this is the string of characters
marked in the below location of the url of your table.
<img width="70%" height="70%" alt="image" src="https://github.com/user-attachments/assets/dc293c2a-6da7-4b23-87c7-9e789f9af268" />

---

## Installation

1. **Clone the repository**:

   ```bash
   git clone https://github.com/your-org/your-repo.git
   cd your-repo
   ```

2. **Install dependencies** (example: `pip`):

   ```bash
   pip install -r requirements.txt
   ```

3. **Prepare configuration files**:

   * **Environment file** (`.env`)
     Stores API keys, tokens, and endpoints.
   * **Schema configuration** (`config.json`)
     Defines financial categories, column mappings, and table naming rules.
     This comes with the repository but you can add on top of it to support more financial categories.
---

## Financial Categories

The following table lists the financial category codes used in this program and their meanings:

| Code  | Description                                   |
| ----- | --------------------------------------------- |
| `C01` | 影票订单数据 *(Movie ticket order data)*            |
| `C02` | 商品订单数据 *(Merchandise order data)*             |
| `C03` | 发卡数据 *(Card issuance data)*                   |
| `C04` | 卡充值数据 *(Card recharge data)*                  |
| `C05` | 卡消费数据 *(Card consumption data)*               |
| `C06` | 券回兑数据 *(Coupon redemption data)*              |
| `C07` | 商品进销存数据 *(Merchandise inventory data)*        |
| `C08` | 商品出入库数据 *(Merchandise inbound/outbound data)* |
| `C09` | 销售消耗原材料数据 *(Raw material consumption data)*   |
| `C10` | 销售消耗品项数据 *(Product consumption data)*         |
| `C11` | 会员卡续费数据 *(Membership card renewal data)*      |
| `C12` | 会员卡退卡数据 *(Membership card cancellation data)* |
| `C13` | 会员卡激活数据 *(Membership card activation data)*   |
| `C14` | 会员卡补卡换卡数 *(Membership card replacement data)* |
| `C15` | 货品操作明细数据 *(Merchandise operation details)*    |
| `C18` | 场次放映明细数据 *(Screening session details)*        |

---

## Decrypter
The `Decrypter` class is a utility designed to **decode encrypted messages** returned by the Yuekeyun API endpoint
`dme.lark.data.finance.getFinancialData`.

### Why is this needed?

* The Yuekeyun API returns financial data encrypted using AES-128 in ECB mode.
* The encryption key is derived from a **SHA1PRNG**-like process seeded by your API key.
* This class **mimics Java's SHA1PRNG** key derivation method (as implemented by Sun/Oracle JVM), which is not straightforwardly replicable in Python.
* The class only supports **AES-128 ECB with no padding**, consistent with the API's encryption scheme.

### Important Notes

* This implementation is a **best-effort approximation**, not a full SHA1PRNG spec implementation.
  As illustrated by the Stack Overflow post below, this only works because the behavior of SHA1PRNG is equivalent
  to hashing input twice when the length of the output key is 128 bits. 

The code was implemented with reference to the following question
['Stack Overflow'](https://stackoverflow.com/questions/64786678/javascript-equivalent-to-java-sha1prng)

---

## Usage

Here’s a minimal example of using `DataSyncClient`:

```python
from src.data_sync_client import DataSyncClient
from src.config import FinancialQueries

# Initialize
client = DataSyncClient(env_file_path='.env', config_file_path='schemas.json')

# Example 1: Download without upload
queries = FinancialQueries('C01', 'day', '2023-01-01')
client.download_data(queries)

# Example 2: Upload yesterday's data for a category
yesterday_queries = FinancialQueries('C02', 'day', '2023-01-14')
client.upload_data(yesterday_queries, table_name='Financial Table')

# Example 3: Sync most recent 14 days for C01
client.sync_most_recent_data('C01', 'Daily Financial Table', looking_back=14)

# Example 4: Sync current year's data by quarter for C07
client._upload_current_year_data('C07', 'Category 7 Table', upload_by_quarter=True)

# Example 5: Sync screening schedule (C18) from yesterday to 30 days ahead
client.sync_screening_data()
```


