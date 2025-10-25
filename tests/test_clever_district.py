"""
Test Clever API token to see which district it has access to
"""
import requests
import json

# Token provided
CLEVER_TOKEN = "d5c461921595f99bbb7c893c4ef932c9b457c06b"

# Clever API base URL
BASE_URL = "https://api.clever.com/v3.0"

def test_district_access():
    """Query Clever API to get district information"""

    headers = {
        "Authorization": f"Bearer {CLEVER_TOKEN}",
        "Accept": "application/json"
    }

    print("=" * 70)
    print("Testing Clever API Token Access")
    print("=" * 70)
    print(f"Token: {CLEVER_TOKEN}")
    print()

    # Try to get districts
    print("1. Attempting to get districts...")
    response = requests.get(f"{BASE_URL}/districts", headers=headers)
    print(f"   Status Code: {response.status_code}")

    if response.status_code == 200:
        data = response.json()
        print(f"   Response: {json.dumps(data, indent=2)}")

        # Extract district info
        if 'data' in data and len(data['data']) > 0:
            districts = data['data']
            print(f"\n   ✓ Found {len(districts)} district(s):")
            for district in districts:
                district_data = district.get('data', {})
                print(f"     - ID: {district_data.get('id')}")
                print(f"       Name: {district_data.get('name')}")
                print(f"       State: {district_data.get('state')}")
    else:
        print(f"   ✗ Error: {response.text}")

    print()

    # Try to get schools
    print("2. Attempting to get schools...")
    response = requests.get(f"{BASE_URL}/schools", headers=headers)
    print(f"   Status Code: {response.status_code}")

    if response.status_code == 200:
        data = response.json()
        if 'data' in data:
            schools = data['data']
            print(f"   ✓ Found {len(schools)} school(s)")
            if len(schools) > 0:
                print(f"   First few schools:")
                for school in schools[:3]:
                    school_data = school.get('data', {})
                    print(f"     - ID: {school_data.get('id')}")
                    print(f"       Name: {school_data.get('name')}")
    else:
        print(f"   ✗ Error: {response.text}")

    print()

    # Try to get users (teachers)
    print("3. Attempting to get teachers...")
    response = requests.get(f"{BASE_URL}/users?role=teacher&limit=5", headers=headers)
    print(f"   Status Code: {response.status_code}")

    if response.status_code == 200:
        data = response.json()
        if 'data' in data:
            teachers = data['data']
            print(f"   ✓ Found teacher data (showing first {len(teachers)})")
            for teacher in teachers:
                teacher_data = teacher.get('data', {})
                print(f"     - ID: {teacher_data.get('id')}")
                print(f"       Name: {teacher_data.get('name', {})}")
    else:
        print(f"   ✗ Error: {response.text}")

    print()

    # Try to get users (students)
    print("4. Attempting to get students...")
    response = requests.get(f"{BASE_URL}/users?role=student&limit=5", headers=headers)
    print(f"   Status Code: {response.status_code}")

    if response.status_code == 200:
        data = response.json()
        if 'data' in data:
            students = data['data']
            print(f"   ✓ Found student data (showing first {len(students)})")
            for student in students:
                student_data = student.get('data', {})
                print(f"     - ID: {student_data.get('id')}")
                print(f"       Name: {student_data.get('name', {})}")
    else:
        print(f"   ✗ Error: {response.text}")

    print()
    print("=" * 70)

if __name__ == "__main__":
    test_district_access()
