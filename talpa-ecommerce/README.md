
# Take-home Assignment
For the Data Engineer role at Talpa E-commerce.

## The assignment

The following modules and steps were developed to complete the assignment:

1. 👨‍🔧 Exploratory Data Analysis

Using Python Notebooks, a detailed data analysis was carried out. This can be viewed in `analysis\exploratory_analysis.ipynb`.
		It is important to know your data and understand the degree of problems you will encounter in subsequent steps. Notebook is a convinient way to perform fast analysis, understand data structure and quickly plot necessary graphs to see visual representation of data and distribution among entities.
		
  Couple assumptions made:
			
   - While it might be more logical to construct a comprehensive relational schema following Bill Inmon's normalized architecture for transactional data (ensuring full ACID support and redundancy), the immediate requirement for a JSON endpoint directed me to a more streamlined approach
			
   - The analysis indicated a probable outlier: an order with the order_date of 10/17/2042

3. 👨‍🔧 Build a data lake on AWS S3

All raw data from customers is meant to go on S3 first via CSV files for the following processing using Airflow. IAM and data retention with using hot/cold storages are set up to achieve maximum security, speed and economy.
		
5. 👨‍🔧 Build a datawarehouse to store silver/gold data layers.

Snowflake is one of the top players on OLAP datawarehouses market and the central DWH of Talpa E-commerce. I decided to use Snowflake as a main storage in addition to DBT to manage the DWH schema.
		
7. 👨‍🔧 Build an Airflow batch pipeline to load raw files from the lake into Snowflake.

To achieve better team coordination and project usability, Airflow module can be built using Docker-compose for local environments via prepared shell scripts. For production, it can be easily deployed on k8s using the included docker file or pre-generated docker-image.
		
  The pipeline is meant to be simple: we only need to connect to S3, ensure the file is in the source bucket using the S3Sensor, and COPY it to Snowflake using the S3ToSnowflakeOperator. In addition, if necessary, after the processing, we can move the file to "/processed" folder.

9. 👨‍🔧 Build an API-endpoint to query the data.

The API is built on Django which is the most convenient way to go with Python. The endpoint sole purpose is to request the dataset from Snowflake and return JSON response.
*Please note that it is my first attempt to build an API on Django and the used approaches might be far from production development. The reasoning behind the choice was to try something new and to broaden my skill set :)*

11. 👨‍🔧 Deployed and tested the solution
		
  The entire solution was deployed on Docker, with the necessary setups on S3 and Snowflake. The schema was generated through DBT, run the pipeline on Airflow, and the API was tested on a local Django server.

## Out-of-scope features

*To better serve the JSON for the users*, we might want to include the next features in the next releases:

- Authentication and authorization (SSO, MFA)
- Logging (ELK)
- Deployment, IaC and scalability (k8s or EKS, Helm, Terraform)
- Testing (functional, automated, unit, integration, smoke, performance, stress, AB, etc.)
- Data observability, data quality and lineage (MonteCarlo, DBT expectations)
- Regular backups (currently only Snowflake's fail-safe and time travel)
- Storage size improvements (S3 or other object storage, HTTP and DB-level compression, cold storage)
- Monitoring and notifications (Prometheus or AWS-native tools)
- Fault-resistance (Multi-zone/multi-region deployment)
- DB role/user access setup (DBT, AWS IAM, Terraform)
- Other security (Vault, AWS-native vaults and key storages)
- Data management standards (DAMA/DMBOK/DCAM/LEAN)
- Policies and standards (GDPR, CCPA)
- Airflow improvements (k8s, Celery, Redis)
- Documentation (Confluence)
- Analytics and reporting (Spark, PowerBI/QuickSight/Tableau)
- Various other improvements and end-of-life support
