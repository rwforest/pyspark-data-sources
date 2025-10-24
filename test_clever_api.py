"""
Test Clever API integration with PySpark REST Data Source
Fetches schools, teachers, and students from Chess4Life (Dev) Sandbox district
"""
from pyspark.sql import SparkSession
from pyspark_datasources import rest_api_call

# Token for Chess4Life (Dev) Sandbox
CLEVER_TOKEN = "d5c461921595f99bbb7c893c4ef932c9b457c06b"
DISTRICT_ID = "5940d0b58ec81e0001541ef3"

def test_clever_with_pyspark():
    """Test Clever API using PySpark REST data source"""

    # Initialize Spark
    spark = SparkSession.builder \
        .appName("CleverAPI") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print("=" * 70)
    print("Testing Clever API with PySpark REST Data Source")
    print("=" * 70)
    print(f"District: Chess4Life (Dev) Sandbox")
    print(f"District ID: {DISTRICT_ID}")
    print()

    # Create dummy input (GET requests don't need input data)
    dummy_df = spark.createDataFrame([("query",)], ["type"])

    # 1. Get District Information
    print("1. Getting District Information...")
    districts_df = rest_api_call(
        dummy_df,
        url="https://api.clever.com/v3.0/districts",
        method="GET",
        authType="Bearer",
        oauthToken=CLEVER_TOKEN
    )
    print(f"   Columns: {districts_df.columns}")
    print(f"   Count: {districts_df.count()}")
    districts_df.select("output").show(truncate=False)

    # 2. Get Schools (limit to first 10)
    print("\n2. Getting Schools (first 10)...")
    schools_df = rest_api_call(
        dummy_df,
        url="https://api.clever.com/v3.0/schools?limit=10",
        method="GET",
        authType="Bearer",
        oauthToken=CLEVER_TOKEN
    )
    print(f"   Columns: {schools_df.columns}")
    print(f"   Count: {schools_df.count()}")
    schools_df.select("output").show(truncate=False)

    # 3. Get Teachers (limit to first 10)
    print("\n3. Getting Teachers (first 10)...")
    teachers_df = rest_api_call(
        dummy_df,
        url="https://api.clever.com/v3.0/users?role=teacher&limit=10",
        method="GET",
        authType="Bearer",
        oauthToken=CLEVER_TOKEN
    )
    print(f"   Columns: {teachers_df.columns}")
    print(f"   Count: {teachers_df.count()}")
    teachers_df.select("output").show(truncate=False)

    # 4. Get Students (limit to first 10)
    print("\n4. Getting Students (first 10)...")
    students_df = rest_api_call(
        dummy_df,
        url="https://api.clever.com/v3.0/users?role=student&limit=10",
        method="GET",
        authType="Bearer",
        oauthToken=CLEVER_TOKEN
    )
    print(f"   Columns: {students_df.columns}")
    print(f"   Count: {students_df.count()}")
    students_df.select("output").show(truncate=False)

    # 5. Get specific school details
    print("\n5. Getting Specific School Details (Slothgreat)...")
    school_id = "5940d254203e37907e0000f0"
    school_detail_df = rest_api_call(
        dummy_df,
        url=f"https://api.clever.com/v3.0/schools/{school_id}",
        method="GET",
        authType="Bearer",
        oauthToken=CLEVER_TOKEN
    )
    print(f"   Columns: {school_detail_df.columns}")
    print(f"   Count: {school_detail_df.count()}")
    school_detail_df.select("output").show(truncate=False)

    # 6. Get users for a specific school
    print("\n6. Getting Users for School (Slothgreat, first 5)...")
    school_users_df = rest_api_call(
        dummy_df,
        url=f"https://api.clever.com/v3.0/schools/{school_id}/users?limit=5",
        method="GET",
        authType="Bearer",
        oauthToken=CLEVER_TOKEN
    )
    print(f"   Columns: {school_users_df.columns}")
    print(f"   Count: {school_users_df.count()}")
    school_users_df.select("output").show(truncate=False)

    print("\n" + "=" * 70)
    print("✓ All Clever API tests completed successfully!")
    print("=" * 70)

    spark.stop()

if __name__ == "__main__":
    test_clever_with_pyspark()
