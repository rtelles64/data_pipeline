# Data Pipeline
A music streaming company, *Sparkify*, has decided to introduce more automation and monitoring to their data warehouse ETL pipelines with Apache Airflow.

They expect to utilize high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. They have also noted that data quality plays a big part when analyses are executed on top of the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

## Overview
This project introduces the core concepts of Apache Airflow. This project implements custom operators to perform tasks such as staging data, filling the data warehouse, and running checks on the data as the final step.

Below is an example DAG data flow within the pipeline:

![Example DAG](example-dag.png)

## Add Airflow Connections
1. Go to Airflow UI:
   - Open http://localhost:8080/ in Google Chrome (other browsers occasionally have issues rendering the Airflow IU)

2. Click on the **Admin** tab and select **Connections**.

![Admin connections](images/admin-connections.png)

3. Under **Connections**, select **Create**.

![Create connection](images/create-connection.png)

4. On the create connection page, enter the following values:
   - **Conn Id**: Enter `aws_credentials`
   - **Conn Type**: Enter `Amazon Web Services`
   - **Login**: Enter your **Access key ID** from the IAM User credentials
   - **Password**: Enter your **Secret access key** from the IAM User credentials

   Once you've entered these values, select **Save and Add Another**.

![Connection AWS credentials](images/connection-aws-credentials.png)

5. On the next create connection page, enter the following values:
   - **Conn Id**: Enter `redshift`.
   - **Conn Type**: Enter `Postgres`.
   - **Host**: Enter the endpoints of your Redshift cluster, excluding the port at the end. You can find this by selecting your cluster in the **Clusters** page of the Amazon Redshift console. See where this is located in the screenshot below. *IMPORTANT*: Make sure **NOT** to include the port at the end of the Redshift endpoint string.
   - **Schema**: Enter `dev`. This is the Redshift database you want to connect to.
   - **Login**: Enter `awsuser`.
   - **Password**: Enter the password you created when launching your Redshift cluster.
   - **Port**: Enter `5439`.

   Once you've entered these values, select **Save**.

![Cluster details](images/cluster-details.png)

![Connection Redshift](images/connection-redshift.png)

You are now configured to run Airflow with Redshift.

## Datasets
Here are the s3 links for each dataset:
- Log data: `s3://udacity-dend/log-data`
- Song data: `s3://udacity-dend/song_data`

## Project Template
There are three major components for this project:
- `dags`: all the imports and task templates
- `plugins/operators`: contains operator templates
- `plugins/helpers`: contains all SQL transformations

## DAG Configuration
The parameters for the DAG are according to these guidelines:
- The DAG does not have dependencies on past runs
- On failure, the tasks are retried 3 times
- Retries happen every 5 minutes
- Catchup is turned off
- Do not email on retry

Task dependencies are configured according to the [example DAG](##Overview) above.

## Operators
There are four different operators that will stage the data, transform the data, and run checks on data quality. Using Airflow's built-in functionalities as connections and hooks ensures that Airflow does all the heavy-lifting when possible.

All of the operators and tasks will run SQL statements against the Redshift database, utilizing parameters that make these operators flexible, reusable, and configurable to later apply to other data pipelines with Redshift and with other databases.

### Stage Operator
The stage operator is expected to be able to load any JSON formatted files from S3 to Amazon Redshift. The operator creates and runs a SQL COPY statement based on the parameters provided. The operator's parameters should specify where in S3 the file is loaded and what the target table is.

The parameters should be able to distinguish between JSON files. An important requirement of the stage operator is containing a templated field that allows it to load timestamped files from S3 based on the execution time and run backfills.

### Fact and Dimension Operators
The dimension and fact operators utilized the provided SQL helper class to run data transformations. Most of the logic is within the SQL transformations and the operator is expected to take as input a SQL statement and target database on which to run the query against.

Dimension loads are often done with the truncate-insert pattern where the target table is emptied before the load. Thus, there is a parameter that allows switching between insert modes when loading dimensions.

Fact tables are usually so massive that they should only allow append type functionality.

### Data Quality Operator
The data quality operator is used to run checks on the data itself. Its main functionality is to receive one or more SQL-based test cases along with the expected results and execute the tests. For each test, the test result and expected result need to be checked and if there is no match, the operator raises an exception and the task retries and eventually fails.

#### Example Test
The data quality operator checks if certain columns contain NULL values by counting all rows that have NULL in the column. Since we do not want any NULLs, the expected result would be 0 and the test would compare the SQL statement's outcome to the expected result.

## Running/Rerunning the DAG
After updating the DAG, run `/opt/airflow/start.sh` to start the Airflow web server. Once the Airflow web server is ready, you can update the Airflow UI by refreshing the [page](http://localhost:8080/).

> NOTE: Files located in the S3 bucket `udacity-dend` are very large, so Airflow can take up to 10 minutes to make the connection.
