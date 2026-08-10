# Real-Time E-Commerce Lakehouse Pipeline

A near-real-time streaming data pipeline built on Databricks Free Edition, simulating an e-commerce order system end-to-end — from event generation through a validated Medallion (Bronze/Silver/Gold) architecture, ready for BI reporting.

Built as a personal project to gain hands-on streaming/lakehouse experience alongside daily batch ETL work.

## Architecture

```
                 ┌─────────────────────┐
                 │  Python Event        │
                 │  Generator            │
                 │  (orders + customers) │
                 └──────────┬───────────┘
                             │ writes newline-delimited JSON
                             ▼
                 ┌───────────────────────┐
                 │   AWS S3               │
                 │   raw/orders/yyyy/mm/dd│
                 │   raw/customers/       │
                 └──────────┬─────────────┘
                             │ Databricks Auto Loader (cloudFiles)
                             ▼
   ┌───────────────────────────────────────────────┐
   │                  BRONZE                         │
   │   bronze_orders        bronze_customers          │
   │   (raw, untransformed, + ingestion timestamp)    │
   └──────────────────────┬──────────────────────────┘
                           │ Lakeflow Declarative Pipeline
                           ▼
   ┌───────────────────────────────────────────────────┐
   │                   SILVER                            │
   │   silver_orders          silver_customers            │
   │   silver_order_items      (deduped, validated)        │
   │   rejected_orders (DQ failures, kept for visibility)  │
   └──────────────────────┬────────────────────────────────┘
                           │ aggregation + join
                           ▼
   ┌───────────────────────────────────────────────────────┐
   │                    GOLD (8 tables)                       │
   │  gold_daily_revenue         gold_revenue_by_country        │
   │  gold_hourly_order_volume   gold_revenue_by_age_group       │
   │  gold_order_status_breakdown gold_new_vs_returning_revenue   │
   │  gold_avg_order_value        gold_top_products                │
   └──────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
                 ┌───────────────────┐
                 │   SQL Warehouse     │
                 └──────────┬──────────┘
                             │
                             ▼
                 ┌───────────────────┐
                 │      Power BI       │
                 └───────────────────┘
```

## Tech Stack

Python · AWS S3 · Databricks Auto Loader · Lakeflow Declarative Pipelines (Delta Live Tables) · Delta Lake · Databricks SQL Warehouse · Power BI

## Data Sources

- **Orders** (fact/event stream) — generated continuously in batches, simulating live order traffic
- **Customers** (dimension) — generated once as a fixed pool; orders reference real customer IDs from this pool, so repeat-customer behavior occurs naturally

## Medallion Layers

**Bronze** — Raw ingestion via Auto Loader, incremental file discovery from S3, zero transformation beyond an `_ingested_at` timestamp. Kept as an unmodified mirror of the source so any downstream layer can be reprocessed without re-fetching from S3.

**Silver** — Deduplication, type casting, and data quality enforcement via `expect_or_drop` expectations (e.g. positive amounts, valid order status, non-null IDs). Rows failing validation are not silently dropped — they're captured in `rejected_orders` for visibility. `silver_order_items` flattens the nested items array and is filtered to only include items belonging to orders that passed `silver_orders` validation (referential integrity across Silver tables).

**Gold** — Business-facing aggregations: revenue trends (daily/hourly), order status breakdown, average order value, top products, and customer-segmented revenue (by country, age group, new vs. returning).

## Key Design Decisions & Tradeoffs

- **Auto Loader over Kafka** — Databricks Free Edition restricts outbound network access to a limited set of trusted domains, blocking direct Kafka broker connections regardless of hosting. File-based incremental ingestion via Auto Loader was the right fit for this environment and for an order-event pattern generally.
- **Triggered pipeline, not Continuous** — Continuous execution requires an always-on cluster, which doesn't fit Free Edition's compute limits. Triggered runs (processing all available data, then stopping) is the practical choice; in production with dedicated compute, this would move to a scheduled or continuous trigger for lower latency.
- **Customers modeled as a fixed, one-time-generated pool** — since customer dimension data is slowly-changing by nature, not a continuous stream, it's generated once and reused, allowing genuine repeat-customer behavior in the order data.
- **Credentials via pipeline Spark configuration, not hardcoded** — AWS keys and the S3 bucket name are injected through the pipeline's Spark configuration rather than embedded in notebook code, keeping secrets out of source control.
- **DQ failures are visible, not silent** — `rejected_orders` mirrors the inverse of Silver's validation rules, so failed records remain queryable rather than disappearing.

## Setup / How to Run

**Prerequisites**
- Databricks workspace (Free Edition or higher) with Unity Catalog enabled
- AWS account with an S3 bucket and an IAM user scoped to that bucket (`s3:GetObject`, `s3:PutObject`, `s3:ListBucket`)

**1. Clone and connect the repo**
- Clone this repo, or connect it directly via Databricks Repos (Workspace → Repos → Add Repo)

**2. Configure credentials**
- Create a `config` notebook (not committed — see `.gitignore`) with:
  ```python
  AWS_ACCESS_KEY = "your-access-key-id"
  AWS_SECRET_KEY = "your-secret-access-key"
  BUCKET_NAME = "your-bucket-name"
  ```
- The event generator notebook loads these via `%run ./config`

**3. Generate the customer pool (run once)**
- Run the customer-generation cell in the event generator notebook a single time — this seeds a fixed pool of customers that orders will reference. Re-running it creates a disconnected duplicate pool, so this step is intentionally one-time only.

**4. Generate order events**
- Run the order-batch simulation cells to start writing order data to `s3://<bucket>/raw/orders/`

**5. Create the ETL Pipeline**
- In Databricks: Workflows → Create ETL Pipeline
- Add the Bronze, Silver, and Gold notebooks (in `/pipelines`) as source code
- Under pipeline Settings → Spark configuration, add:
  ```
  fs.s3a.access.key = your-access-key-id
  fs.s3a.secret.key = your-secret-access-key
  spark.streaming.orders.bucketName = your-bucket-name
  ```
- Set pipeline mode to **Triggered**
- Click **Start**

**6. Connect a BI tool**
- Point Power BI (or any SQL client) at your Databricks SQL Warehouse connection details, and query the `gold_*` tables directly

## Possible Future Improvements

- Continuous pipeline mode with dedicated compute for true low-latency streaming
- AWS Secrets Manager / Databricks Secrets for credential management (attempted during development; blocked by Free Edition API restrictions — see notes below)
- A third source (e.g. product catalog) to complete a full star-schema design
- Watermarking on streaming deduplication to bound state growth over time
