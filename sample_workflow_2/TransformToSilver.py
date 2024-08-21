# Read from raw table
df_raw = spark.table("dr_test_catalog.raw.raw_data")

# Transformation (e.g., filter values greater than 50)
df_silver = df_raw.filter(df_raw.value > 50)

# Write to silver table
df_silver.write.mode("overwrite").saveAsTable("dr_test_catalog.stage.silver_data")
