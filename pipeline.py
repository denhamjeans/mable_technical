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


# -- dim_provider_profiles table
# - combine support and provider dimensions using UNION ALL
# - create hash_id as primary key to test for uniqueness
# - create user_type column to identify the type of user
# - create segment column to identify the segment of user (for both client and support provider)

# Create schema
con.execute("""
    CREATE OR REPLACE TABLE dim_profiles_silver (
        id INTEGER NOT NULL,
        user_id INTEGER,
        user_type TEXT,
        segment TEXT,
        created_at DATETIME,
        updated_at DATETIME,
        hash_id TEXT NOT NULL PRIMARY KEY
    );
""")

# Insert data into the table
con.execute("""
    INSERT INTO dim_profiles_silver
        SELECT
            id,
            user_id,
            'worker' AS user_type,
            worker_segment AS segment,
            created_at,
            updated_at,
            HASH(id, 'worker') AS hash_id
        FROM support_provider_profiles_bronze
        UNION ALL
        SELECT
            id,
            user_id,
            'client' AS user_type,
            customer_segment AS segment,
            created_at,
            updated_at,
            HASH(id, 'client') AS hash_id
        FROM client_profiles_bronze
""")
con.sql('SELECT * FROM dim_profiles_silver').show(max_width=1000)


## -- service_logs table
# - test for uniqueness
# - pivcot the table structure such that the status updates are timestamped as distinct columns.
# - remove rows with NA values
# - remove rows with rejected status
# - standarize user_type as 'worker' rather than support provider

# Create schema
con.execute("""
    CREATE OR REPLACE TABLE fact_service_logs_silver (
        id INTEGER NOT NULL PRIMARY KEY,
        client_id INTEGER,
        worker_id INTEGER,
        start_time DATETIME,
        end_time DATETIME,
        hourly_rate DOUBLE,
        approved_time DATETIME,
        submitted_time DATETIME
    );
""")
# Insert data into the table
con.execute("""
    INSERT INTO fact_service_logs_silver
        WITH service_logs_clean AS (
            SELECT
                id,
                client_id,
                worker_id,
                status,
                start_time,
                end_time,
                CAST(REPLACE(hourly_rate, '$','') AS DOUBLE) AS hourly_rate,
                created_at,
                updated_at
            FROM service_logs_bronze
            WHERE worker_id IS NOT NULL
                AND status <> 'rejected'
            QUALIFY row_number() OVER (PARTITION BY id, status ORDER BY updated_at DESC) = 1
        )
                
        PIVOT service_logs_clean
        ON status
        USING max(updated_at) AS time
        GROUP BY id, client_id, worker_id, start_time, end_time, hourly_rate
        ORDER BY id;
""")
con.sql('SELECT * FROM fact_service_logs_silver').show(max_width=1000)



## -- users table
# - test for uniqueness
# - deduplicate the table so that only the most recent row is kept

con.execute("""
    CREATE OR REPLACE TABLE dim_users_silver (
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
    INSERT INTO dim_users_silver
        SELECT
            *
        FROM users_bronze
        QUALIFY row_number() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1
""")
con.sql('SELECT * FROM dim_users_silver')





## 4. Create Gold Layer: Data Aggregation and Analysis
#- No changes in the gold layer

# Create schema
con.execute("""
    CREATE OR REPLACE TABLE dim_profiles_gold (
        id INTEGER NOT NULL,
        user_id INTEGER,
        user_type TEXT,
        segment TEXT,
        created_at DATETIME,
        updated_at DATETIME,
        hash_id TEXT NOT NULL PRIMARY KEY
    );
""")

# Insert data into the table
con.execute("""
    INSERT INTO dim_profiles_gold
        SELECT *
        FROM dim_profiles_silver
""")
con.sql('SELECT * FROM dim_profiles_gold').show(max_width=1000)


## -- service_logs table
# - add aggregate colummns for analysis

# Create schema
con.execute("""
    CREATE OR REPLACE TABLE fact_service_logs_gold (
        id INTEGER NOT NULL PRIMARY KEY,
        client_id INTEGER,
        worker_id INTEGER,
        start_time DATETIME,
        end_time DATETIME,
        hourly_rate DOUBLE,
        approved_time DATETIME,
        submitted_time DATETIME,
        time_to_approval INTERVAL
    );
""")
# Insert data into the table
con.execute("""
    INSERT INTO fact_service_logs_gold
        SELECT
            id,
            client_id,
            worker_id,
            start_time,
            end_time,
            hourly_rate,
            approved_time,
            submitted_time,
            (strptime(LEFT(approved_time, 19), '%Y-%m-%d %H:%M:%S') - strptime(LEFT(submitted_time, 19), '%Y-%m-%d %H:%M:%S')) AS time_to_approval,
        FROM fact_service_logs_silver
""")
con.sql('SELECT * FROM fact_service_logs_gold').show(max_width=1000)

# -- users table
# - no changes in the gold layer

#Create schema
con.execute("""
    CREATE OR REPLACE TABLE dim_users_gold (
        id INTEGER NOT NULL PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        user_type TEXT,
        state TEXT,
        created_at DATETIME,
        updated_at DATETIME
    );
""")
# Insert data into the table
con.execute("""
    INSERT INTO dim_users_gold
        SELECT *
        FROM users_silver
""")
con.sql('SELECT * FROM dim_users_gold')



##Analysis

con.sql("""
    WITH service_logs AS (
        SELECT 
            fact_service_logs_gold.*,
            worker_profile.segment AS worker_segment,
            worker_user.first_name AS worker_first_name,
            client_profile.segment AS client_segment,
            client_user.first_name AS client_first_name
        FROM fact_service_logs_gold
        LEFT JOIN dim_profiles_gold AS worker_profile
            ON fact_service_logs_gold.worker_id = worker_profile.id
            AND worker_profile.user_type = 'worker'
        LEFT JOIN dim_profiles_gold AS client_profile
            ON fact_service_logs_gold.client_id = client_profile.id
            AND client_profile.user_type = 'client'
        LEFT JOIN dim_users_gold AS client_user
            ON fact_service_logs_gold.client_id = client_user.id
    )
""").show(max_width=1000)


# NOTE:
# - user table can be consolidated
# - We should hash + salt the names for analysis here.