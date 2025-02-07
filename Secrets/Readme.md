We create the below tasks in a workflow using airflow and astro sdk, by building a pipeline b/w s3 & snowflake:

![alt text](image.png)

Pre-requisites for installation: Docker & Astro CLI (easy way to setup airflow locally - open source)

Astro CLI: https://www.astronomer.io/docs/astro/cli/install-cli/

In folder sdk, initilize astro dev using command:
`astro dev init`


Add the smowflake, aws requirement in requirement.txt
Add the env variables in .env:

`AIRFLOW__CORE__ENABLE_XCOM_PICKLING=True`

`AIRFLOW__ASTRO_SDK__SQL_SCHEMA=ASTRO_SDK_SCHEMA`

Then run airflow by command: 
`astro dev start`

Go to localhost:8080 and enter default credentials - 

Username: admin
Password: admin

Path to S3 bucket file: s3://nj-astrosdk-bucket/orders_data_header.csv
![alt text](image-1.png)


1. Create AWS account & a s3 bucket in it, generate the secret key-pair
2. Create snowflake account and create DB with tables using the SNOWFLAKE_QUERIES
3. Create connections to S3 and SNOWFLAKE on airflow console
4. Write the DAG for the pipeline generation
