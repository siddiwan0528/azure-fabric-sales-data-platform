# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "4af6fc00-e312-4313-8ff4-d3a2cacf3b2e",
# META       "default_lakehouse_name": "lh_sales_bronze",
# META       "default_lakehouse_workspace_id": "a8acdceb-3966-4eb9-ab23-cae23ed6dd5a",
# META       "known_lakehouses": [
# META         {
# META           "id": "4af6fc00-e312-4313-8ff4-d3a2cacf3b2e"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
# Inspect available databases and schemas

spark.range(5).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

# Actual schema.table reference from the Lakehouse Explorer
bronze_table = "sales_bronze.dbo"

bronze_df = spark.table(bronze_table)

print(f"Bronze row count: {bronze_df.count()}")
bronze_df.printSchema()

display(bronze_df.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

# 1. Read Bronze
bronze_df = spark.table("sales_bronze.dbo")

# 2. Normalize blank business keys
normalized_df = (
    bronze_df
    .withColumn(
        "CustomerID",
        F.when(
            F.trim(F.col("CustomerID")) == "",
            F.lit(None).cast("string")
        ).otherwise(F.trim(F.col("CustomerID")))
    )
    .withColumn(
        "ProductID",
        F.when(
            F.trim(F.col("ProductID")) == "",
            F.lit(None).cast("string")
        ).otherwise(F.trim(F.col("ProductID")))
    )
)

# 3. Remove exact duplicate rows only
dedup_df = normalized_df.dropDuplicates()

# 4. Identify records missing required IDs
missing_customer = F.col("CustomerID").isNull()
missing_product = F.col("ProductID").isNull()

quarantine_df = (
    dedup_df
    .withColumn(
        "DQReason",
        F.concat_ws(
            ";",
            F.when(missing_customer, F.lit("Missing CustomerID")),
            F.when(missing_product, F.lit("Missing ProductID"))
        )
    )
    .filter(F.col("DQReason") != "")
)

# 5. Keep only records with both required IDs
valid_df = dedup_df.filter(
    ~(missing_customer | missing_product)
)

# 6. Convert source strings to useful data types
silver_df = valid_df.select(
    F.col("OrderID"),
    F.col("OrderLineID"),
    F.to_date("OrderDate", "yyyy-MM-dd").alias("OrderDate"),
    F.to_timestamp("LoadTimestamp").alias("LoadTimestamp"),
    F.col("CustomerID"),
    F.col("ProductID"),
    F.col("Region"),
    F.col("SalesChannel"),
    F.col("PaymentMethod"),
    F.col("Quantity").cast("int").alias("Quantity"),
    F.col("UnitPrice").cast("decimal(18,2)").alias("UnitPrice"),
    F.col("DiscountPct").cast("decimal(5,4)").alias("DiscountPct"),
    F.col("SalesAmount").cast("decimal(18,2)").alias("SalesAmount"),
    F.col("CostAmount").cast("decimal(18,2)").alias("CostAmount"),
    F.col("ProfitAmount").cast("decimal(18,2)").alias("ProfitAmount"),
    F.col("OrderStatus")
)

# 7. Create a separate Silver schema
spark.sql("CREATE SCHEMA IF NOT EXISTS sales_silver")

# 8. Write both tables; overwrite makes reruns repeatable
(
    silver_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("sales_silver.sales")
)

(
    quarantine_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("sales_silver.sales_quarantine")
)

# 9. Print validation counts
print(f"Bronze rows: {bronze_df.count()}")
print(f"Exact duplicates removed: {bronze_df.count() - dedup_df.count()}")
print(f"Quarantine rows: {quarantine_df.count()}")
print(f"Silver rows: {silver_df.count()}")

display(silver_df.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

silver = spark.table("sales_silver.sales")
quarantine = spark.table("sales_silver.sales_quarantine")

print("Silver rows:", silver.count())
print("Quarantine rows:", quarantine.count())

print(
    "Null required IDs in Silver:",
    silver.filter(
        F.col("CustomerID").isNull() |
        F.col("ProductID").isNull()
    ).count()
)

print(
    "Invalid dates:",
    silver.filter(
        F.col("OrderDate").isNull() |
        F.col("LoadTimestamp").isNull()
    ).count()
)

print("Silver schema:")
silver.printSchema()

display(quarantine)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

# Read the validated Silver data
silver = spark.table("sales_silver.sales")

# Aggregate monthly sales KPIs by region
gold_monthly_region = (
    silver
    .withColumn(
        "MonthStart",
        F.trunc(F.col("OrderDate"), "month")
    )
    .groupBy("MonthStart", "Region")
    .agg(
        F.countDistinct("OrderID").alias("Orders"),
        F.sum("Quantity").alias("UnitsSold"),
        F.sum("SalesAmount").alias("Revenue"),
        F.sum("CostAmount").alias("Cost"),
        F.sum("ProfitAmount").alias("Profit")
    )
    .withColumn(
        "AverageOrderValue",
        F.when(
            F.col("Orders") > 0,
            F.round(F.col("Revenue") / F.col("Orders"), 2)
        )
    )
    .withColumn(
        "ProfitMarginPct",
        F.when(
            F.col("Revenue") != 0,
            F.round(
                F.col("Profit") / F.col("Revenue") * 100,
                2
            )
        )
    )
)

# Create the Gold schema
spark.sql("CREATE SCHEMA IF NOT EXISTS sales_gold")

# Save the Gold table; overwrite makes reruns repeatable
(
    gold_monthly_region.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("sales_gold.monthly_regional_kpis")
)

print("Gold KPI table created.")
print("Monthly regional KPI rows:", gold_monthly_region.count())

display(
    gold_monthly_region
    .orderBy("MonthStart", "Region")
    .limit(15)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

silver = spark.table("sales_silver.sales")
gold = spark.table("sales_gold.monthly_regional_kpis")

silver_totals = silver.agg(
    F.count("*").alias("rows"),
    F.countDistinct("OrderID").alias("orders"),
    F.sum("Quantity").alias("units"),
    F.sum("SalesAmount").alias("revenue"),
    F.sum("CostAmount").alias("cost"),
    F.sum("ProfitAmount").alias("profit")
).first()

gold_totals = gold.agg(
    F.count("*").alias("kpi_rows"),
    F.sum("Orders").alias("orders"),
    F.sum("UnitsSold").alias("units"),
    F.sum("Revenue").alias("revenue"),
    F.sum("Cost").alias("cost"),
    F.sum("Profit").alias("profit")
).first()

print("Gold KPI rows:", gold_totals["kpi_rows"])
print("Silver rows:", silver_totals["rows"])

checks = [
    ("Orders", silver_totals["orders"], gold_totals["orders"]),
    ("Units sold", silver_totals["units"], gold_totals["units"]),
    ("Revenue", silver_totals["revenue"], gold_totals["revenue"]),
    ("Cost", silver_totals["cost"], gold_totals["cost"]),
    ("Profit", silver_totals["profit"], gold_totals["profit"])
]

for name, silver_value, gold_value in checks:
    status = "PASS" if silver_value == gold_value else "CHECK"
    print(f"{name}: {status} | Silver={silver_value} | Gold={gold_value}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

# Existing, validated monthly regional sales KPIs
actual = spark.table(
    "sales_gold.monthly_regional_kpis"
)

# Target data from Bronze
targets_raw = spark.table(
    "sales_bronze.sales_targets_bronze"
)

targets = targets_raw.select(
    F.to_date(
        F.col("MonthStart"), "yyyy-MM-dd"
    ).alias("MonthStart"),
    F.trim(F.col("Region")).alias("Region"),
    F.col("SalesTarget")
        .cast("decimal(18,2)")
        .alias("SalesTarget")
)

# Validate the target records before publishing Gold
invalid_targets = targets.filter(
    F.col("MonthStart").isNull()
    | F.col("Region").isNull()
    | F.col("SalesTarget").isNull()
).count()

if invalid_targets:
    raise ValueError(
        f"Found {invalid_targets} invalid target records."
    )

# Retain all target periods, including periods
# without actual sales data yet.
gold_target = (
    targets
    .join(
        actual,
        on=["MonthStart", "Region"],
        how="left"
    )
    .withColumn(
        "Variance",
        F.when(
            F.col("Revenue").isNotNull(),
            F.col("Revenue") - F.col("SalesTarget")
        )
    )
    .withColumn(
        "AchievementPct",
        F.when(
            F.col("Revenue").isNotNull()
            & (F.col("SalesTarget") != 0),
            F.round(
                F.col("Revenue")
                / F.col("SalesTarget") * 100,
                2
            )
        )
    )
    .withColumn(
        "ActualsAvailable",
        F.col("Revenue").isNotNull()
    )
)

# Write a new Gold table; leave the original intact.
(
    gold_target.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        "sales_gold.monthly_regional_target_kpis"
    )
)

print("Target records:", targets.count())
print("Gold target KPI rows:", gold_target.count())
print(
    "Rows with actual sales:",
    gold_target.filter(F.col("ActualsAvailable")).count()
)
print(
    "Rows without actual sales:",
    gold_target.filter(~F.col("ActualsAvailable")).count()
)

display(
    gold_target
    .orderBy("MonthStart", "Region")
    .limit(15)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

gold = spark.table("sales_gold.monthly_regional_target_kpis")

print("Total rows:", gold.count())
print(
    "Duplicate month-region keys:",
    gold.groupBy("MonthStart", "Region")
        .count()
        .filter(F.col("count") > 1)
        .count()
)
print(
    "Rows with actual sales:",
    gold.filter(F.col("ActualsAvailable")).count()
)
print(
    "Rows without actuals:",
    gold.filter(~F.col("ActualsAvailable")).count()
)
print(
    "Actual rows missing achievement percentage:",
    gold.filter(
        F.col("ActualsAvailable")
        & F.col("AchievementPct").isNull()
    ).count()
)

display(
    gold.select(
        "MonthStart",
        "Region",
        "SalesTarget",
        "Revenue",
        "Variance",
        "AchievementPct",
        "ActualsAvailable"
    ).orderBy("MonthStart", "Region").limit(15)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

sales = spark.table("sales_silver.sales")

monthly_check = (
    sales
    .withColumn("OrderDate", F.to_date("OrderDate"))
    .withColumn("MonthStart", F.trunc("OrderDate", "month"))
    .groupBy("MonthStart")
    .agg(
        F.count("*").alias("OrderRows"),
        F.round(
            F.sum(F.col("SalesAmount").cast("double")), 2
        ).alias("Revenue"),
        F.countDistinct("Region").alias("Regions"),
        F.max("OrderDate").alias("LastOrderDate")
    )
    .orderBy(F.col("MonthStart").desc())
)

display(monthly_check.limit(6))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
