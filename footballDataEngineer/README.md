# System Architecture
![](https://github.com/munna710/Football_data_analytics/blob/main/footballDataEngineer/image/system_architecture.png)
This project is designed to efficiently crawl data from Wikipedia using Apache Airflow, clean and process the data with PostgreSQL, and then push it to Azure Data Lake Gen2 for further processing and analysis.
![](https://github.com/munna710/Football_data_analytics/blob/main/footballDataEngineer/image/Dashboard%201.png)
## Workflow
1. Utilize Apache Airflow to schedule and monitor the workflow of crawling data from Wikipedia.
2. Cleanse and preprocess the crawled data using PostgreSQL.
3. Store the cleaned data in Azure Data Lake Gen2 leveraging Azure Data Factory for optimized storage and accessibility.
4. Process and analyze the data with Databricks, Tableau, Power BI, or Looker Studio as per requirements.

## Technologies Used
- Apache Airflow: For orchestrating complex workflows of data extraction from Wikipedia.
- PostgreSQL: Employed for cleaning and preprocessing of extracted raw data.
- Azure Data Lake Gen2 & Azure Data Factory: For storing processed datasets ensuring scalability and security.
## Getting Started
1. Clone the repository.
<pre>git clone https://github.com/munna710/Football_data_analytics.git</pre>
2. Install Python dependencies.
<pre>pip install -r requirements.txt</pre>
## Running the Code With Docker
1. Start your services on Docker with
<pre>docker compose up -d</pre>
2. Trigger the DAG on the Airflow UI.

