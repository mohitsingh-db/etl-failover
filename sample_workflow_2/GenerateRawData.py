import random
import string

# Generate random data
data = [(i, ''.join(random.choices(string.ascii_letters + string.digits, k=10)), random.randint(1, 100)) for i in range(100)]

# Create DataFrame
df = spark.createDataFrame(data, ["id", "name", "value"])

# Write to raw table
df.write.mode("overwrite").saveAsTable("dr_test_catalog.raw.raw_data")
