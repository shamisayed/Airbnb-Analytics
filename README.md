# Visualizing Airbnb Trends with Big Data Analytics

The primary goal is to automate the data processing and set up a pipeline that converts, merges, and stores the dataset in Parquet format in S3 bucket. This data is then queried using Athena and visualized in Power BI. The project not only leverages big data technologies but also focuses on enabling Airbnb stakeholders to make data-driven decisions through effective data visualization. 

<ins>Data Sources</ins>:
The Airbnb global listing dataset includes extensive information on how Airbnb is really being used in cities around the world. It comprises a CSV file (2 GB) and a GeoJSON file (3 GB) containing geospatial data.

<ins>Technologies Used</ins>:
AWS (EC2, Hadoop on EMR, S3, Glue, Athena), PySpark, Power BI (visualizes the processed data, providing insights through interactive dashboards), AWS CloudFormation (automates the provisioning of AWS infrastructure), GitHub Actions (manages the continuous integration and deployment (CI/CD) process).

<ins>Workflow</ins>

1. Raw Data Ingestion & Conversion to DataFrames:  
The raw data consisting of the CSV and GeoJSON files are stored in a designated Amazon S3 bucket. The raw data is then loaded and converted into Pandas and GeoPandas DataFrames, ensuring structured data ready for further processing.

2. Merging and Storing as Parquet: 
The DataFrames are merged based on geospatial data, then stored in Parquet format in S3 bucket. Parquet's storage efficiency and speed in querying makes it the preferred format for this pipeline. The use of Parquet significantly reduces storage costs and improves query performance in subsequent steps.

3. Glue and Athena Integration: 
AWS Glue crawlers scan the Parquet data stored in S3, creating and updating a metadata catalog in Amazon Athena. This metadata catalog allows the data to be queried using standard SQL queries. Athena's serverless architecture ensures that data queries are fast and cost-effective, enabling the business to derive insights quickly.

4. Data Visualization (Power BI):
The final stage of the data pipeline involves visualizing the processed data using Power BI. Power BI connects to Amazon Athena, allowing for real-time data analysis and visualization. The integration of Power BI with the pipeline ensures that decision-makers have access to up-to-date insights, which are crucial for strategic planning and operations.

5.  Automation with CFT: 
To ensure the entire pipeline is automated and can be deployed with minimal manual intervention, the project uses AWS CloudFormation (CFT) and GitHub Actions. CloudFormation templates are used to provision and manage AWS resources, defining the infrastructure as code. This approach ensures that the environment is consistent and repeatable across different stages of development, testing, and production.
GitHub Actions are employed to automate the deployment process. Every change in the code repository triggers a GitHub Action, which initiates the deployment of updated infrastructure and pipeline components using CloudFormation. This continuous integration and continuous deployment (CI/CD) setup ensures that the pipeline is always up-to-date and operates efficiently.
