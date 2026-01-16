# Databricks E-commerce Sales Data Pipeline
This project implements a production-grade data engineering solution on **Azure Databricks** to process, enrich, and aggregate e-commerce sales data. The system ingests raw datasets related to customers, products, orders, and transactions, transforms them into analytics-ready tables, and exposes business-level aggregates for downstream reporting and decision-making. The solution emphasizes scalability, data quality, testability, and clear architectural separation of concerns.


**Full Problem Statement:** https://github.com/faraaznx/databricks-ecommerce-project/tree/main/Problem_Statement

## Assumptions

The following assumptions were deliberately made to model the solution as a production-grade data pipeline and to represent realistic variations in ingestion and load patterns across different data sources.

### Bronze Layer (Raw Ingestion)

- **Orders**
  - Orders are ingested from JSON files using an incremental load strategy.
  - A strict schema is enforced at ingestion time.
  - Any schema deviation results in a pipeline failure to prevent propagation of corrupt or unexpected data.

- **Products**
  - Product data is ingested from Excel files on an incremental basis, with new files arriving daily.
  - Schema enforcement is intentionally not applied at this stage.
  - Any unexpected or evolving fields are captured in a `rescue` column to support schema drift analysis without breaking ingestion.

- **Customers**
  - Customer data is ingested from a CSV file using a full-load pattern.
  - Each new file completely replaces the previous version.
  - It is assumed that the full customer population is always present in a single file.

### Silver Layer (Enriched Data)

- **Orders**
  - A strict schema is enforced on the enriched orders table.
  - Any deviation from the expected schema results in a pipeline failure.

- **Products**
  - Schema enforcement is applied only to the columns required for downstream aggregations.
  - Deviations in these required columns cause the pipeline to fail.

- **Customers**
  - Schema enforcement is applied only to the columns required for downstream aggregations.
  - Deviations in these required columns cause the pipeline to fail.


## Cases to Handle
- If there's no file or empty file.
- Corrupt rows
- When a schema is changed for the joining table (joining key is missing)
