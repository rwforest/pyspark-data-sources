#!/usr/bin/env python3
"""
Fetch ALL schools, teachers, and students from Clever API and save as CSV files.

This script:
1. Fetches all schools in the district
2. For each school, fetches all teachers
3. For each school, fetches all students
4. Saves everything as CSV files
"""

from pyspark.sql import SparkSession
from pyspark_datasources import rest_api_call
import ast
import time

# Initialize Spark
spark = SparkSession.builder \
    .appName("CleverAPIFetchAll") \
    .master("local[*]") \
    .getOrCreate()

# Your Clever API token
CLEVER_TOKEN = ""

print("=" * 80)
print("CLEVER API - FETCH ALL DATA")
print("=" * 80)

# Create dummy input for GET requests
dummy_input = spark.createDataFrame([{"placeholder": "dummy"}])

# ============================================================================
# STEP 1: Fetch ALL Schools
# ============================================================================
print("\n[1/3] Fetching ALL schools...")

all_schools = []
schools_url = "https://api.clever.com/v3.0/schools"
has_more = True

while has_more:
    print(f"  Fetching: {schools_url}")

    schools_df = rest_api_call(
        dummy_input,
        url=schools_url,
        method="GET",
        authType="Bearer",
        oauthToken=CLEVER_TOKEN,
        queryType="inline",
        partitions="1"
    )

    # Parse response
    school_response = schools_df.select("output").first()["output"]
    school_data = ast.literal_eval(school_response)

    # Extract schools from this page
    for school in school_data.get("data", []):
        school_info = school.get("data", {})
        all_schools.append({
            "id": school_info.get("id"),
            "name": school_info.get("name"),
            "school_number": school_info.get("school_number"),
            "nces_id": school_info.get("nces_id"),
            "sis_id": school_info.get("sis_id"),
            "state_id": school_info.get("state_id"),
            "district": school_info.get("district"),
            "principal": school_info.get("principal", {}).get("name") if school_info.get("principal") else None,
            "principal_email": school_info.get("principal", {}).get("email") if school_info.get("principal") else None,
            "location": str(school_info.get("location")) if school_info.get("location") else None,
        })

    # Check for next page
    links = school_data.get("links", [])
    next_link = next((link for link in links if link.get("rel") == "next"), None)

    if next_link and next_link.get("uri"):
        schools_url = next_link.get("uri")
        time.sleep(0.5)  # Rate limiting
    else:
        has_more = False

print(f"  ✓ Fetched {len(all_schools)} schools")

# Save schools to CSV
schools_df = spark.createDataFrame(all_schools)
schools_output = "/Users/yipman/Downloads/github/pyspark-data-sources/output/schools.csv"
schools_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(schools_output)
print(f"  ✓ Saved to {schools_output}")

# ============================================================================
# STEP 2: Fetch ALL Teachers for Each School
# ============================================================================
print("\n[2/3] Fetching ALL teachers for each school...")

all_teachers = []
total_teachers = 0

for idx, school in enumerate(all_schools, 1):
    school_id = school["id"]
    school_name = school["name"]

    print(f"  [{idx}/{len(all_schools)}] {school_name} ({school_id})")

    # Fetch teachers for this school (with pagination)
    teachers_url = f"https://api.clever.com/v3.0/schools/{school_id}/users?role=teacher"
    has_more = True
    school_teacher_count = 0

    while has_more:
        teachers_df = rest_api_call(
            dummy_input,
            url=teachers_url,
            method="GET",
            authType="Bearer",
            oauthToken=CLEVER_TOKEN,
            queryType="inline",
            partitions="1"
        )

        # Parse response
        teacher_response = teachers_df.select("output").first()["output"]
        teacher_data = ast.literal_eval(teacher_response)

        # Extract teachers from this page
        for teacher in teacher_data.get("data", []):
            teacher_info = teacher.get("data", {})
            teacher_name = teacher_info.get("name", {})

            all_teachers.append({
                "school_id": school_id,
                "school_name": school_name,
                "teacher_id": teacher_info.get("id"),
                "first_name": teacher_name.get("first"),
                "middle_name": teacher_name.get("middle"),
                "last_name": teacher_name.get("last"),
                "email": teacher_info.get("email"),
                "district": teacher_info.get("district"),
                "sis_id": teacher_info.get("sis_id"),
                "state_id": teacher_info.get("state_id"),
                "title": teacher_info.get("title"),
                "credentials": str(teacher_info.get("credentials")) if teacher_info.get("credentials") else None,
            })
            school_teacher_count += 1

        # Check for next page
        links = teacher_data.get("links", [])
        next_link = next((link for link in links if link.get("rel") == "next"), None)

        if next_link and next_link.get("uri"):
            teachers_url = next_link.get("uri")
            time.sleep(0.5)  # Rate limiting
        else:
            has_more = False

    total_teachers += school_teacher_count
    print(f"      → {school_teacher_count} teachers")

    # Small delay to avoid rate limiting
    time.sleep(0.2)

print(f"  ✓ Fetched {total_teachers} total teachers across all schools")

# Save teachers to CSV
if all_teachers:
    teachers_df = spark.createDataFrame(all_teachers)
    teachers_output = "/Users/yipman/Downloads/github/pyspark-data-sources/output/teachers.csv"
    teachers_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(teachers_output)
    print(f"  ✓ Saved to {teachers_output}")
else:
    print("  ! No teachers found")

# ============================================================================
# STEP 3: Fetch ALL Students for Each School
# ============================================================================
print("\n[3/3] Fetching ALL students for each school...")

all_students = []
total_students = 0

for idx, school in enumerate(all_schools, 1):
    school_id = school["id"]
    school_name = school["name"]

    print(f"  [{idx}/{len(all_schools)}] {school_name} ({school_id})")

    # Fetch students for this school (with pagination)
    students_url = f"https://api.clever.com/v3.0/schools/{school_id}/users?role=student"
    has_more = True
    school_student_count = 0

    while has_more:
        students_df = rest_api_call(
            dummy_input,
            url=students_url,
            method="GET",
            authType="Bearer",
            oauthToken=CLEVER_TOKEN,
            queryType="inline",
            partitions="1"
        )

        # Parse response
        student_response = students_df.select("output").first()["output"]
        student_data = ast.literal_eval(student_response)

        # Extract students from this page
        for student in student_data.get("data", []):
            student_info = student.get("data", {})
            student_name = student_info.get("name", {})

            all_students.append({
                "school_id": school_id,
                "school_name": school_name,
                "student_id": student_info.get("id"),
                "first_name": student_name.get("first"),
                "middle_name": student_name.get("middle"),
                "last_name": student_name.get("last"),
                "email": student_info.get("email"),
                "district": student_info.get("district"),
                "sis_id": student_info.get("sis_id"),
                "state_id": student_info.get("state_id"),
                "student_number": student_info.get("student_number"),
                "grade": student_info.get("grade"),
                "gender": student_info.get("gender"),
                "dob": student_info.get("dob"),
                "hispanic_ethnicity": student_info.get("hispanic_ethnicity"),
                "race": student_info.get("race"),
                "credentials": str(student_info.get("credentials")) if student_info.get("credentials") else None,
            })
            school_student_count += 1

        # Check for next page
        links = student_data.get("links", [])
        next_link = next((link for link in links if link.get("rel") == "next"), None)

        if next_link and next_link.get("uri"):
            students_url = next_link.get("uri")
            time.sleep(0.5)  # Rate limiting
        else:
            has_more = False

    total_students += school_student_count
    print(f"      → {school_student_count} students")

    # Small delay to avoid rate limiting
    time.sleep(0.2)

print(f"  ✓ Fetched {total_students} total students across all schools")

# Save students to CSV
if all_students:
    students_df = spark.createDataFrame(all_students)
    students_output = "/Users/yipman/Downloads/github/pyspark-data-sources/output/students.csv"
    students_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(students_output)
    print(f"  ✓ Saved to {students_output}")
else:
    print("  ! No students found")

# ============================================================================
# SUMMARY
# ============================================================================
print("\n" + "=" * 80)
print("SUMMARY")
print("=" * 80)
print(f"Schools:  {len(all_schools)}")
print(f"Teachers: {total_teachers}")
print(f"Students: {total_students}")
print("\nOutput files:")
print(f"  • {schools_output}")
print(f"  • {teachers_output}")
print(f"  • {students_output}")
print("\nNote: CSV files are saved in directories. Look for part-*.csv files.")
print("=" * 80)
