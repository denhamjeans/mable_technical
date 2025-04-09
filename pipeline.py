"""Practical Data Modelling: Pipeline to Ingest, Clean, Transform Mable Data 

Context
-------
Mable is an online marketplace that enables Clients to best match with Support Providers to fulfil their care needs. 
At Mable, when either a Client or Support Providers creates an account, a User is created, as well as a linked Client Profile or Support Providers profile depending on the user type. 
When a Support Worker performs a shift for a Client, they lodge a Service Log for approval by the Client, detailing the hours, rate and services provided.
When talking to our Data Analysts and Scientists, they want to be able to perform analysis on the below:
● Hours, rate and value of care being delivered by the users state
● Understand time to approval of service logs

Notes
-----
- No environment setup for this exercise, although this is important in a real-world scenario.
"""

import duckdb as dd
import pandas as pd
from ydata_profiling import ProfileReport

## Import Data into pandas
client_profiles = pd.read_csv(r"data/client_profiles.csv", header=0)
support_provider_profiles =  pd.read_csv(r"data/support_provider_profiles.csv", header=0)
service_logs = pd.read_csv(r"data/service_logs.csv", header=0)
users = pd.read_csv(r"data/users.csv", header=0)

data_dict = {
    'client_profiles': client_profiles, 
    'support_provider_profiles': support_provider_profiles, 
    'service_logs': service_logs, 
    'users': users    
}

## Profile the data
for table_name, data in data_dict.items():
    profile = ProfileReport(data, title=f"{table_name} Profiling Report", explorative=True)
    profile.to_file(f"outputs/{table_name}_profile.html") # save the report to a file


 
## 1. Create Bronze Layer: Insert Raw Data into DB
# Create / connect to database
con = dd.connect('mable.db')

# drop for demo purposes if table already exists

for table_name in data_dict.keys():
    con.execute(f"DROP TABLE IF EXISTS {table_name}_bronze")
    con.sql(f"""CREATE TABLE {table_name}_bronze AS (
            SELECT *
        FROM read_csv('data/{table_name}.csv', header = true)
    )
    """)
    # print(con.sql(f'SELECT * FROM {table_name}_bronze'))

# Result of showing tables after creating the raw tables
con.sql('SHOW TABLES')




## 3. Create Silver Layer: Data Transformation and Cleaning

# DB Schema Setup based on profiling/inspection
# set PRIMARY KEY as NOT NULL


# -- client_profile table
con.execute("""
    CREATE OR REPLACE TABLE client_profiles_silver (
        id INTEGER NOT NULL PRIMARY KEY,
        user_id INTEGER,
        customer_segment TEXT,
        created_at DATETIME,
        updated_at DATETIME
    );
""")
con.execute("""
    INSERT INTO client_profiles_silver
        SELECT * FROM client_profiles_bronze
""")
con.sql('SELECT * FROM client_profiles_silver')


# -- support_provider_profiles table
con.execute("""
    CREATE OR REPLACE TABLE support_provider_profiles_silver (
        id INTEGER NOT NULL PRIMARY KEY,
        user_id INTEGER,
        worker_segment TEXT,
        created_at DATETIME,
        updated_at DATETIME
    );
""")
con.execute("""
    INSERT INTO support_provider_profiles_silver
        SELECT * FROM support_provider_profiles_bronze
""")
con.sql('SELECT * FROM support_provider_profiles_silver')


# -- service_logs table
# Create schema
con.execute("""
    CREATE OR REPLACE TABLE service_logs_silver (
        id INTEGER NOT NULL PRIMARY KEY,
        client_id INTEGER,
        worker_id TEXT,
        status TEXT,
        start_time TEXT,
        end_time TEXT,
        hourly_rate TEXT,
        created_at DATETIME,
        updated_at DATETIME
    );
""")
# Insert data into the table
con.execute("""
    INSERT INTO service_logs_silver
        WITH service_logs_clean AS (
            SELECT *
            FROM service_logs_bronze
            WHERE worker_id IS NOT NULL
                AND status <> 'rejected'
        )
                
        PIVOT service_logs_clean
        ON status
        USING max(updated_at) AS time
        GROUP BY id, client_id, worker_id, start_time, end_time, hourly_rate;
""")
con.sql('SELECT * FROM support_provider_profiles_silver').show(max_width=1000)


con.sql("""
WITH service_logs_clean AS (
    SELECT *
    FROM service_logs_bronze
    WHERE worker_id IS NOT NULL
        AND status != 'rejected'
)

PIVOT service_logs_clean
ON status
USING max(updated_at) AS time
GROUP BY id, client_id, worker_id, start_time, end_time, hourly_rate
ORDER BY id;
""").show(max_width=1000)


# -- users table
con.execute("""
    CREATE OR REPLACE TABLE users_silver (
        id INTEGER NOT NULL PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        user_type TEXT,
        state TEXT,
        created_at DATETIME,
        updated_at DATETIME
    );
""")
con.execute("""
    INSERT INTO users_silver
        SELECT
            *
        FROM users_bronze
        QUALIFY row_number() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1
""")
con.sql('SELECT * FROM users_silver')

## -- Changes Summary
# - users_silver: Only show most recently modified rows from SCDII table. 
# - service_logs_silver: Widen table to show submitted_time, approved_time, and rejected_time.
# - service_logs_silver: Remove rows with NA values.
# - 



## 4. Create Gold Layer: Data Aggregation and Analysis
