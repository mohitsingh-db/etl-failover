# Read from silver table
df_silver = spark.table("dr_test_catalog.stage.silver_data")

# Aggregation (e.g., count by name)
df_gold = df_silver.groupBy("name").count()

# Write to gold table
df_gold.write.mode("overwrite").saveAsTable("dr_test_catalog.main.gold_data")

