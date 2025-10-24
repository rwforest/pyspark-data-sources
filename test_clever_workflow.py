"""
Test Clever API workflow:
1. Get districts (no params)
2. Get schools for each district
3. Get teachers/students for each school
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
import json

# Token for Chess4Life (Dev) Sandbox
CLEVER_TOKEN = ""

def test_clever_workflow():
    """Test Clever API hierarchical data fetching"""

    # Initialize Spark
    spark = SparkSession.builder \
        .appName("CleverAPIWorkflow") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Register REST data source
    from pyspark_datasources.rest import RestDataSource
    spark.dataSource.register(RestDataSource)

    print("=" * 70)
    print("Clever API Workflow Test")
    print("=" * 70)

    # Step 1: Get Districts (no parameters needed)
    print("\n1. Getting Districts (no input parameters)...")

    # For GET requests with no parameters, we still need dummy input
    # but use queryType='inline' so params aren't appended
    dummy_input = '[{"placeholder": "dummy"}]'

    districts_raw = spark.read.format("rest") \
        .option("url", "https://api.clever.com/v3.0/districts") \
        .option("method", "GET") \
        .option("authType", "Bearer") \
        .option("oauthToken", CLEVER_TOKEN) \
        .option("inputData", dummy_input) \
        .option("queryType", "inline") \
        .option("includeInputsInOutput", "N") \
        .option("partitions", "1") \
        .load()

    print("   Raw districts response:")
    districts_raw.show(truncate=False)

    # Parse the JSON response to extract district IDs
    # The output column contains JSON like: {"data": [{"data": {"id": "..."}}]}
    print("\n   Parsing district data...")

    # Collect the response
    # The REST data source stores the parsed dict as a string representation
    district_response = districts_raw.select("output").first()["output"]
    # Need to evaluate the string representation to get the actual dict
    import ast
    if isinstance(district_response, str):
        try:
            # Try JSON first
            district_data = json.loads(district_response)
        except json.JSONDecodeError:
            # Fall back to ast.literal_eval for Python dict strings
            district_data = ast.literal_eval(district_response)
    else:
        district_data = district_response

    # Extract district IDs
    district_ids = []
    if "data" in district_data:
        for district in district_data["data"]:
            district_info = district.get("data", {})
            district_id = district_info.get("id")
            district_name = district_info.get("name")
            if district_id:
                district_ids.append({"district_id": district_id, "district_name": district_name})
                print(f"   - District ID: {district_id}, Name: {district_name}")

    if not district_ids:
        print("   No districts found!")
        spark.stop()
        return

    # Step 2: Get Schools for each district
    print(f"\n2. Getting Schools for district: {district_ids[0]['district_name']}...")

    # Create input DataFrame with district IDs
    districts_df = spark.createDataFrame(district_ids)
    districts_df.show()

    # Collect district data and serialize for REST call
    district_input = json.dumps(districts_df.collect()[0].asDict())
    input_for_schools = f'[{district_input}]'

    # Get schools - the URL already includes getting all schools
    # Clever API: GET /v3.0/schools returns all schools for the authenticated district
    schools_raw = spark.read.format("rest") \
        .option("url", "https://api.clever.com/v3.0/schools?limit=10") \
        .option("method", "GET") \
        .option("authType", "Bearer") \
        .option("oauthToken", CLEVER_TOKEN) \
        .option("inputData", input_for_schools) \
        .option("queryType", "inline") \
        .option("includeInputsInOutput", "N") \
        .option("partitions", "1") \
        .load()

    print("   Raw schools response:")
    schools_raw.show(truncate=False)

    # Parse schools
    import ast
    school_response = schools_raw.select("output").first()["output"]
    if isinstance(school_response, str):
        try:
            school_data = json.loads(school_response)
        except json.JSONDecodeError:
            school_data = ast.literal_eval(school_response)
    else:
        school_data = school_response

    school_ids = []
    if "data" in school_data:
        for school in school_data["data"][:3]:  # First 3 schools
            school_info = school.get("data", {})
            school_id = school_info.get("id")
            school_name = school_info.get("name")
            if school_id:
                school_ids.append({"school_id": school_id, "school_name": school_name})
                print(f"   - School ID: {school_id}, Name: {school_name}")

    if not school_ids:
        print("   No schools found!")
        spark.stop()
        return

    # Step 3: Get Teachers for a specific school
    print(f"\n3. Getting Teachers for school: {school_ids[0]['school_name']}...")

    school_id = school_ids[0]['school_id']

    # Create input with school ID
    school_input = json.dumps([{"school_id": school_id}])

    # Use inline query type with {school_id} placeholder in URL
    teachers_raw = spark.read.format("rest") \
        .option("url", f"https://api.clever.com/v3.0/schools/{school_id}/users?role=teacher&limit=5") \
        .option("method", "GET") \
        .option("authType", "Bearer") \
        .option("oauthToken", CLEVER_TOKEN) \
        .option("inputData", school_input) \
        .option("queryType", "inline") \
        .option("includeInputsInOutput", "N") \
        .option("partitions", "1") \
        .load()

    print("   Raw teachers response:")
    teachers_raw.show(truncate=False)

    # Parse teachers
    import ast
    teacher_response = teachers_raw.select("output").first()["output"]
    if isinstance(teacher_response, str):
        try:
            teacher_data = json.loads(teacher_response)
        except json.JSONDecodeError:
            teacher_data = ast.literal_eval(teacher_response)
    else:
        teacher_data = teacher_response

    if "data" in teacher_data:
        print(f"   Found {len(teacher_data['data'])} teachers:")
        for teacher in teacher_data["data"]:
            teacher_info = teacher.get("data", {})
            teacher_name = teacher_info.get("name", {})
            print(f"   - {teacher_name.get('first')} {teacher_name.get('last')}")

    # Step 4: Get Students for the same school
    print(f"\n4. Getting Students for school: {school_ids[0]['school_name']}...")

    students_raw = spark.read.format("rest") \
        .option("url", f"https://api.clever.com/v3.0/schools/{school_id}/users?role=student&limit=5") \
        .option("method", "GET") \
        .option("authType", "Bearer") \
        .option("oauthToken", CLEVER_TOKEN) \
        .option("inputData", school_input) \
        .option("queryType", "inline") \
        .option("includeInputsInOutput", "N") \
        .option("partitions", "1") \
        .load()

    print("   Raw students response:")
    students_raw.show(truncate=False)

    # Parse students
    import ast
    student_response = students_raw.select("output").first()["output"]
    if isinstance(student_response, str):
        try:
            student_data = json.loads(student_response)
        except json.JSONDecodeError:
            student_data = ast.literal_eval(student_response)
    else:
        student_data = student_response

    if "data" in student_data:
        print(f"   Found {len(student_data['data'])} students:")
        for student in student_data["data"]:
            student_info = student.get("data", {})
            student_name = student_info.get("name", {})
            print(f"   - {student_name.get('first')} {student_name.get('last')}")

    print("\n" + "=" * 70)
    print("✓ Clever API workflow completed successfully!")
    print("=" * 70)

    spark.stop()

if __name__ == "__main__":
    test_clever_workflow()
