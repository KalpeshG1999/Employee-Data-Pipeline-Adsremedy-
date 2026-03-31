import pandas as pd
import random
from faker import Faker
from datetime import datetime, timedelta

fake = Faker()

# CONFIG
NUM_RECORDS = 1200

departments = ["IT", "HR", "Finance", "Sales", "Marketing", "Analytics"]
job_titles = [
    "Software Engineer", "Data Analyst", "Manager",
    "HR Executive", "Accountant", "Sales Executive"
]
states = ["NY", "CA", "TX", "WA", "FL", "IL"]
status_list = ["Active", "Inactive", "On Leave"]

data = []

# GENERATE DATA
for i in range(NUM_RECORDS):

    # Employee ID with duplicates
    employee_id = random.randint(1000, 1100)

    # Names 
    first_name = fake.first_name()
    last_name = fake.last_name()

    if random.random() < 0.3:
        first_name = first_name.upper()
    if random.random() < 0.3:
        last_name = last_name.lower()

    # Email 
    if random.random() < 0.2:
        email = "invalid_email"
    else:
        email = fake.email()

    # Hire date 
    hire_date = fake.date_between(start_date='-5y', end_date='+2y')

    # Birth date
    birth_date = fake.date_between(start_date='-60y', end_date='-20y')

    # Salary 
    salary = random.randint(30000, 120000)
    if random.random() < 0.5:
        salary = f"${salary:,}"  # with $ and comma

    # Manager ID 
    manager_id = random.choice([None, random.randint(1000, 1050)])

    # Address fields
    address = fake.address().replace("\n", ", ")
    city = fake.city()
    state = random.choice(states)
    zip_code = fake.zipcode()

    # Department & Job
    department = random.choice(departments)
    job_title = random.choice(job_titles)

    # Status 
    status = random.choice(status_list)
    if random.random() < 0.3:
        status = status.lower()

    # Introduce missing values randomly
    if random.random() < 0.1:
        email = None
    if random.random() < 0.1:
        salary = None

    data.append([
        employee_id,
        first_name,
        last_name,
        email,
        hire_date,
        job_title,
        department,
        salary,
        manager_id,
        address,
        city,
        state,
        zip_code,
        birth_date,
        status
    ])

# CREATE DATAFRAME
columns = [
    "employee_id",
    "first_name",
    "last_name",
    "email",
    "hire_date",
    "job_title",
    "department",
    "salary",
    "manager_id",
    "address",
    "city",
    "state",
    "zip_code",
    "birth_date",
    "status"
]

df = pd.DataFrame(data, columns=columns)

# ADD DUPLICATES (extra)
df = pd.concat([df, df.sample(50)], ignore_index=True)

# SAVE CSV
output_path = "data/employees_raw.csv"
df.to_csv(output_path, index=False)

print(f"✅ Data generated successfully: {output_path}")
print(f"Total records: {len(df)}")