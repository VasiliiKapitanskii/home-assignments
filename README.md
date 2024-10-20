# home-assignments
A collection of solutions to various home assignments from potential employers.
The projects are presented as-is in their original form, without post-review modifications, fixes, or improvements.
They were developed without prior experience in real-world projects, primarily to familiarize myself with a company's tech stack or explore new tools out of curiosity.

 - Just Eat Takeaway, Data (Warehouse) Engineering (Estimated dev effort: 1 day, plus 1 day to learn Dagster and ClickHouse from scratch)
   - Exploratory data analysis: Python Notebooks, DuckDB
   - DWH Model (ER Diagram), see er_diagram
   - AWS: Data lake setup
   - Docker containerized
     - ClickHouse: Central columnar data warehouse (DWH)
     - Dagster: Orchestration, data pipelines, dbt runner, and data lineage
     - dbt: Medallion-layered DWH schema with a Star schema atop, data quality enforcement, and SQL transformations
   - Miscellaneous: GitHub, Docker, flake8, SQLFluff, pre-commit, makefile

 - Talpa eCommerce, Data Engineering (Estimated effort: ~5-7 hours, excluding designing and planning)
   - AWS: Data lake setup
   - Snowflake: Central columnar data warehouse (DWH)
   - Airflow: Orchestration and data pipelines
   - dbt: Medallion-layered DWH schema, data quality enforcement, and SQL transformations
   - Django: Simple API to serve JSON data
   - Exploratory data analysis: Python Notebooks
   - Miscellaneous: GitHub, Docker, flake8, SQLFluff

 - B9, Data Engineering (Estimated effort: ~2-3 hours)
   - SQL tasks: Focused on advanced SQL techniques
     - Recursive CTEs
     - Window functions
   - Python task: Developed a synchronous/asynchronous data pipeline for currency rate retrieval and reporting
     - REST API requests
     - Chart plotting
     - Libraries: typing, asyncio, matplotlib, aiohttp

 - Siren Group, Data Engineering (Estimated effort: ~2-3 hours)
   - Python task: Built a sales leads analysis and quality assurance data pipeline
     - Libraries: pandas, tabulate
   - dbt task: Data modeling and testing
     - dbt data modelling and tests
     - dbt_expectations for data quality checks
