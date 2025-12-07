# import Necessary Libraries

from pyspark.sql import SparkSession
from pyspark.sql import DataFrameWriter
from pyspark.sql.functions import monotonically_increasing_id
import pandas as pd
from sqlalchemy import create_engine
import os
import psycopg2

# set JAVA HOME
# Point JAVA_HOME to your Java 8 install
os.environ["JAVA_HOME"] = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.17.10-hotspot" 

# Make sure Java 8's bin is at the very front of PATH
os.environ["PATH"] = os.environ["JAVA_HOME"] + r"\bin;" + os.environ["PATH"]

# Intialise my Spark Session
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Nuga Bank ETL") \
    .config("spark.jars", r"C:\Users\Admin\OneDrive\Documents\10Alytics_Training\pyspark\Nuga_Project\Nuga_bank_etl_case_study\postgresql-42.7.8.jar") \
    .getOrCreate()


# Extract this historical data into a spark dataframe
df = spark.read.csv(r'dataset\rawdata\nuga_bank_transactions.csv', header=True, inferSchema=True)


# fill up the missing values
df_clean = df.fillna({
    'Customer_Name': 'Unknown',
    'Customer_Address': 'Unknown',
    'Customer_City': 'Unknown',
    'Customer_State': 'Unknown',
    'Customer_Country': 'Unknown',
    'Company': 'Unknown',
    'Job_Title': 'Unknown',
    'Email': 'Unknown', 
    'Phone_Number': 'Unknown',
    'Credit_Card_Number': '0',
    'IBAN': 'Unknown',
    'Currency_Code': 'Unknown',
    'Random_Number': '0.0',
    'Category': 'Unknown',
    'Group': 'Unknown',
    'Is_Active': 'Unknown',
    'Description': 'Unknown',
    'Gender': 'Unknown',
    'Marital_Status': 'Unknown'
    
})

# drop the missing values in the last_updated column
df_clean = df_clean.na.drop(subset=['Last_Updated'])

# Data Transformation to 2NF
# Transaction table
transaction = df_clean.select('Transaction_Date', 'Amount','Transaction_Type')  \
                    .withColumn('Transaction_Id', monotonically_increasing_id()) \
                    .select('Transaction_Id','Transaction_Date', 'Amount','Transaction_Type')

# Customer table
customer = df_clean.select('Customer_Name', 'Customer_Address', 'Customer_City', 'Customer_State', 'Customer_Country')  \
                    .withColumn('Customer_Id', monotonically_increasing_id()) \
                    .select('Customer_Id','Customer_Name', 'Customer_Address', 'Customer_City', 'Customer_State', 'Customer_Country')


# Employee table
employee = df_clean.select('Company', 'Job_Title', 'Email', 'Phone_Number', 'Gender', 'Marital_Status')  \
                    .withColumn('Employee_Id', monotonically_increasing_id()) \
                    .select('Employee_Id','Company', 'Job_Title', 'Email', 'Phone_Number', 'Gender', 'Marital_Status')


# create fact table
fact_table = df_clean.join(transaction, ['Transaction_Date', 'Amount','Transaction_Type'], 'inner') \
                     .join(customer, ['Customer_Name', 'Customer_Address', 'Customer_City', 'Customer_State', 'Customer_Country'], 'inner') \
                     .join(employee, ['Company', 'Job_Title', 'Email', 'Phone_Number', 'Gender', 'Marital_Status'], 'inner') \
                     .select('Transaction_Id','Customer_Id','Employee_Id','Credit_Card_Number', 'IBAN', 'Currency_Code', \
                            'Random_Number', 'Category', 'Group', 'Is_Active', 'Last_Updated', 'Description')


# Develop Functions to get Databse connection
def get_db_connection():
    connection = psycopg2.connect(
        host='localhost',
        database='nuga_bank_etl',
        user='postgres',
        password='London123'
    )
    return connection

# connect to sql database
conn = get_db_connection()


# Create a function to create tables
def create_table():
    conn = get_db_connection()
    cursor = conn.cursor()

    create_table_query = '''
                        DROP TABLE IF EXISTS customer;
                        DROP TABLE IF EXISTS transaction;
                        DROP TABLE IF EXISTS employee;
                        DROP TABLE IF EXISTS fact_table;

                        CREATE TABLE customer (
                            Customer_Id BIGINT,
                            Customer_Name VARCHAR(10000),
                            Customer_Address VARCHAR(10000),
                            Customer_City VARCHAR(10000),
                            Customer_State VARCHAR(10000),
                            Customer_Country VARCHAR(10000)
                        );

                        CREATE TABLE transaction (
                            Transaction_Id BIGINT,
                            Transaction_Date DATE,
                            Amount FLOAT,
                            Transaction_Type VARCHAR(10000)
                        );

                        CREATE TABLE employee (
                            Employee_Id BIGINT,
                            Company VARCHAR(10000),
                            Job_Title VARCHAR(10000),
                            Email VARCHAR(10000),
                            Phone_Number VARCHAR(10000),
                            Gender VARCHAR(10000),
                            Marital_Status VARCHAR(10000)
                        );

                        CREATE TABLE fact_table (
                            Transaction_Id  BIGINT,
                            Customer_Id     BIGINT,
                            Employee_Id     BIGINT,
                            Credit_Card_Number  BIGINT,
                            IBAN     VARCHAR(10000),
                            Currency_Code   VARCHAR(10000),
                            Random_Number  FLOAT,
                            Category VARCHAR(10000),
                            "Group"  VARCHAR(10000),
                            Is_Active VARCHAR(10000),
                            Last_Updated   DATE,
                            Description   VARCHAR(10000)
                        );
    '''

    cursor.execute(create_table_query)
    conn.commit()
    cursor.close()
    conn.close()
    print("Tables created successfully.")

create_table()

# loading data into table
url = "jdbc:postgresql://localhost:5432/nuga_bank_etl"
properties = {
    "user" : "postgres",
    "password" : "London123",
    "driver" : "org.postgresql.Driver"
}

customer.write.jdbc(url=url, table="customer", mode="append", properties=properties)
employee.write.jdbc(url=url, table="employee", mode="append", properties=properties)
transaction.write.jdbc(url=url, table="transaction", mode="append", properties=properties)
fact_table.write.jdbc(url=url, table="fact_table", mode="append", properties=properties)

print('Database, table and data loaded successfully')