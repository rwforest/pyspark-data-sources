#!/usr/bin/env python3
"""
Comprehensive test script for REST Data Source
Tests the fixed partition bug and basic functionality
"""

import sys
from pyspark.sql import SparkSession

def test_import():
    """Test 1: Import REST Data Source"""
    print("\n" + "="*70)
    print("TEST 1: Import REST Data Source")
    print("="*70)

    try:
        from pyspark_datasources import RestDataSource
        print("✅ RestDataSource imported successfully")
        print(f"   Class name: {RestDataSource.name()}")
        return True
    except Exception as e:
        print(f"❌ Failed to import RestDataSource: {e}")
        return False


def test_registration(spark):
    """Test 2: Register REST Data Source"""
    print("\n" + "="*70)
    print("TEST 2: Register REST Data Source")
    print("="*70)

    try:
        from pyspark_datasources import RestDataSource
        spark.dataSource.register(RestDataSource)
        print("✅ RestDataSource registered successfully")
        return True
    except Exception as e:
        print(f"❌ Failed to register RestDataSource: {e}")
        return False


def test_partitions_method(spark):
    """Test 3: Verify partitions() method works (the bug fix)"""
    print("\n" + "="*70)
    print("TEST 3: Verify partitions() method (BUG FIX TEST)")
    print("="*70)

    try:
        from pyspark_datasources import RestDataSource

        # Create test input
        data = [(1,), (2,)]
        df = spark.createDataFrame(data, ["id"])
        df.createOrReplaceTempView("partition_test_input")

        # This would fail with the old bug: TypeError: 'int' object is not callable
        print("   Creating RestReader with partitions option...")
        result_df = spark.read.format("rest") \
            .option("url", "https://jsonplaceholder.typicode.com/posts") \
            .option("input", "partition_test_input") \
            .option("method", "GET") \
            .option("partitions", "2") \
            .load()

        print("   Calling count() to trigger execution...")
        count = result_df.count()

        print(f"✅ partitions() method works correctly!")
        print(f"   Retrieved {count} results with 2 partitions")
        return True

    except TypeError as e:
        if "'int' object is not callable" in str(e):
            print(f"❌ BUG STILL EXISTS: {e}")
            print("   The partition naming conflict bug is not fixed!")
        else:
            print(f"❌ Different TypeError: {e}")
        return False
    except Exception as e:
        print(f"❌ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_basic_get_request(spark):
    """Test 4: Basic GET request to JSONPlaceholder"""
    print("\n" + "="*70)
    print("TEST 4: Basic GET Request")
    print("="*70)

    try:
        # Create test input
        data = [(1,), (2,), (3,)]
        df = spark.createDataFrame(data, ["id"])
        df.createOrReplaceTempView("get_test_input")

        print("   Making GET requests to JSONPlaceholder API...")
        result_df = spark.read.format("rest") \
            .option("url", "https://jsonplaceholder.typicode.com/posts") \
            .option("input", "get_test_input") \
            .option("method", "GET") \
            .option("partitions", "2") \
            .load()

        count = result_df.count()
        print(f"✅ GET request successful!")
        print(f"   Retrieved {count} results")

        # Show schema
        print("\n   Schema:")
        result_df.printSchema()

        # Show sample data
        print("\n   Sample data:")
        result_df.show(3, truncate=False)

        return True

    except Exception as e:
        print(f"❌ GET request failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_post_request(spark):
    """Test 5: POST request to JSONPlaceholder"""
    print("\n" + "="*70)
    print("TEST 5: POST Request with JSON")
    print("="*70)

    try:
        # Create test input for POST
        data = [
            ("Test Post 1", "This is test content 1", 1),
            ("Test Post 2", "This is test content 2", 1),
        ]
        df = spark.createDataFrame(data, ["title", "body", "userId"])
        df.createOrReplaceTempView("post_test_input")

        print("   Making POST requests to JSONPlaceholder API...")
        result_df = spark.read.format("rest") \
            .option("url", "https://jsonplaceholder.typicode.com/posts") \
            .option("input", "post_test_input") \
            .option("method", "POST") \
            .option("postInputFormat", "json") \
            .load()

        count = result_df.count()
        print(f"✅ POST request successful!")
        print(f"   Created {count} posts")

        # Show sample data
        print("\n   Sample results:")
        result_df.show(2, truncate=False)

        return True

    except Exception as e:
        print(f"❌ POST request failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_multiple_partitions(spark):
    """Test 6: Test with different partition counts"""
    print("\n" + "="*70)
    print("TEST 6: Multiple Partition Configurations")
    print("="*70)

    try:
        # Create larger test input
        data = [(i,) for i in range(1, 11)]
        df = spark.createDataFrame(data, ["id"])
        df.createOrReplaceTempView("multi_partition_test")

        for num_partitions in [1, 3, 5]:
            print(f"\n   Testing with {num_partitions} partition(s)...")

            result_df = spark.read.format("rest") \
                .option("url", "https://jsonplaceholder.typicode.com/posts") \
                .option("input", "multi_partition_test") \
                .option("method", "GET") \
                .option("partitions", str(num_partitions)) \
                .load()

            count = result_df.count()
            print(f"   ✓ {num_partitions} partition(s): {count} results")

        print(f"\n✅ All partition configurations work!")
        return True

    except Exception as e:
        print(f"❌ Partition test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_with_options(spark):
    """Test 7: Test various options"""
    print("\n" + "="*70)
    print("TEST 7: Various Configuration Options")
    print("="*70)

    try:
        data = [(1,)]
        df = spark.createDataFrame(data, ["id"])
        df.createOrReplaceTempView("options_test")

        # Test with timeout options
        print("   Testing with custom timeouts...")
        result_df = spark.read.format("rest") \
            .option("url", "https://jsonplaceholder.typicode.com/posts") \
            .option("input", "options_test") \
            .option("method", "GET") \
            .option("connectionTimeout", "5000") \
            .option("readTimeout", "10000") \
            .load()

        count = result_df.count()
        print(f"   ✓ Custom timeouts: {count} result")

        # Test without input in output
        print("   Testing includeInputsInOutput=N...")
        result_df2 = spark.read.format("rest") \
            .option("url", "https://jsonplaceholder.typicode.com/posts") \
            .option("input", "options_test") \
            .option("method", "GET") \
            .option("includeInputsInOutput", "N") \
            .load()

        columns = result_df2.columns
        print(f"   ✓ includeInputsInOutput=N: columns={columns}")

        print(f"\n✅ All options work correctly!")
        return True

    except Exception as e:
        print(f"❌ Options test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Run all tests"""
    print("\n" + "="*70)
    print("REST DATA SOURCE - COMPREHENSIVE TEST SUITE")
    print("Testing the fixed partition bug and basic functionality")
    print("="*70)

    # Test 1: Import
    if not test_import():
        print("\n❌ FAILED: Cannot continue without successful import")
        sys.exit(1)

    # Create Spark session
    print("\n" + "="*70)
    print("Creating Spark Session...")
    print("="*70)

    try:
        spark = SparkSession.builder \
            .appName("REST DataSource Test") \
            .master("local[2]") \
            .config("spark.sql.shuffle.partitions", "2") \
            .getOrCreate()

        print("✅ Spark session created")
        print(f"   Version: {spark.version}")

    except Exception as e:
        print(f"❌ Failed to create Spark session: {e}")
        sys.exit(1)

    # Run all tests
    results = []

    results.append(("Import", test_import()))
    results.append(("Registration", test_registration(spark)))
    results.append(("Partitions Fix", test_partitions_method(spark)))
    results.append(("GET Request", test_basic_get_request(spark)))
    results.append(("POST Request", test_post_request(spark)))
    results.append(("Multiple Partitions", test_multiple_partitions(spark)))
    results.append(("Various Options", test_with_options(spark)))

    # Stop Spark
    spark.stop()

    # Summary
    print("\n" + "="*70)
    print("TEST SUMMARY")
    print("="*70)

    passed = sum(1 for _, result in results if result)
    total = len(results)

    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status}: {test_name}")

    print("\n" + "="*70)
    print(f"TOTAL: {passed}/{total} tests passed")
    print("="*70)

    if passed == total:
        print("\n🎉 ALL TESTS PASSED! 🎉")
        print("\nThe REST Data Source is working correctly!")
        print("The partition bug fix is verified and functional.")
        sys.exit(0)
    else:
        print(f"\n⚠️  {total - passed} test(s) failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
