1. Data Modelling

Create_DB_tables_pg.sql depicts how the data should be formatted after it has landed;
Please describe how you would design a base and subsequent mart layers for OLAP purposes?
Assume the business will need to perform frequent analysis of:

- Revenue
- Item Sales
- Variant Price Changes
- By Product
- By Customer
- By Date
- if payment has been made and the time it occurred

**Solution**:

Design of warehouse:-

* Create a schema in warehouse to hold the raw data from the OLTP systems. 

* Create a schema in warehouse to hold the transformation views for refining data and have the constraints before loading to target area.

* create a schema in warehouse to hold the history of changes of the data.

* create a schema in warehouse to hold the latest information.

Base contains the enterprise wide warehouse and if any refined requirement on top of enterprise data, we can create data mart

Design  data mart for sales/revenue :-

* Create a schema for data mart and name as per the subject area.

* identify the dimensions like orders, products, customer, date

* identify the facts that involve numbers that is of interest and can be of summarization like sale,revenue,price.

* fine tune the granularity with primary keys  and in the below case it is  id for most of the dim and facts

* build the star schema data mart i,e fact surrounded by the dimensions of a required subject area

 

The below diagram is the OLTP model of the given use case:-

![image](https://github.com/naidu1202/sl-assessment/assets/76042228/f332c751-677a-4ec0-87b6-9c00e4f50105)

can consider the below generic design of the star schema and can be customized to our required use case.

![image](https://github.com/naidu1202/sl-assessment/assets/76042228/d0def650-a6bf-4212-8202-70ea8992c9a0)



2. Data provided - Batch wise from OLTP

- Order
- Order Line
- Transaction Status
- Product
- Variant

Notes:
- The transaction table is a webhook which lands whenever an update is made on any transaction.

Discuss your architecture considerations as well as pk/fks (soft or hard), indexes and partitions where appropriate - you may assume the real orders table is about 20-30 M records.
 
**Solution:-**

The process of loading the data from the OLTP systems to warehouse is the data ingestion.  The consideration factors for the ingestion is as follows:

* size

* frequency

* format

For this use case, we are choosing the snowflake as warehouse. for the data ingestion, we can consider the  format as parquet which is optimized columnar format and help in processing the big volume of data and also natively supports by Apache spark

The attached sample code below is the AWS glue job that runs on pyspark has the code to read the data from AWS S3 of a CSV format and write to parquet in different AWS S3 folder. It also contains how to write to PostgreSQL from AWS S3.

Please refer the AWS glue job in the code section 

Snowflake can integrate to the AWS S3 by creating the stages. Once the integration is done, we can use either stream/tasks or copy command to have the data from S3 to snowflake. we will be loading the  data to snowflake with variant format which is used for both semi/structured format.

As snowflake is already micro partitioned, we don’t need to explicitly set the partition. however we can define the clustering which further bucket the micro partition

Consideration for snowflake table design can follow as below.

* Data type of columns

* constraints unique, primary key, referential integrity

 for the given tables we can follow the PK and FK as follows

 order (PK: id)

 order_lines(pk: id, fk: order.id)

 transaction(pk:id, fk: order.id)

 products(pk:id)

 variant(pk:id, fk: products: id)

 clustering key of table 

 orders(cluster key: processed date from processed timestamp)

 can add the event date for all the tables and have the clustering key depend on the size.

* Table vs view


3. Data Pipelining


How would you push these data sets assume each new file is **incremental** from S3. Assume that there is a central data warehouse which the BI tool connects to (not S3).

1. Provide a DFD or orchestration diagram, or write up an orchestration plan and tools you may use.
 
**Solution:-**

We will orchestrate the data pipeline using the airflow which will poll the AWS services and also calls snowflake for the data loading to warehouse.

we use the following tools for the data pipeline.

* Airflow

* Glue

* RDS

* S3

* snowflake

![image](https://github.com/naidu1202/sl-assessment/assets/76042228/f5ac3b88-c538-4c66-91a8-831ef4dc03b2)


 

4. Cloud Engineering

The Data Scientist has developed an unsupervised model to help analyse traffic flow and conversions into our online shop. 

The model is expected to analyse web-traffic data (sourced from the data warehouse) and output some model results daily. 
The model is delivered to you as a python module.
Model output from the main function is expected to be a dataframe.

You have been tasked to deploy the solution, and allow BI to develop Tableau dashboards based on all the data science model's output for downstream business users.
Discuss how you would implement this system, and provide a simple systems diagram. 

1. Provide a systems diagram only.
**Solution:-**

will can use the Gitlab for the automatic CI/CD and use docker/ECS for the ML model deployment.  once the deployment is made  with CI/CD, it automatically run. we can have the output of the data frame in the warehouse. BI needs to plug in the warehouse for the output generated by the model.

![image](https://github.com/naidu1202/sl-assessment/assets/76042228/cea1ce1d-182a-4e75-84d8-e99ad5d492d8)



5. Analytical SQL

### Problem 1

Sometimes products for whatever reason stop selling and a symptom can be an item that was selling well faces a stock out or delisting (or something else). Write a query that shows products that have sold for more than 30 days in the last 60 days, but hasn't had sales for the last week.

You may assume a sales table schema of your preference.

1. Date
2. Product_id
3. Total Items sold to date 
4. number of days with sales
5. number of dates in the recent history where sales have ceased

What would be the best way to implement this in Date/Product_id/Sum(sales)... type Data Mart with other sales information (i.e. without filters and group by)?
 
**Solution:-**

Query to list product that has good sales in last 60 days but didn’t  sale in last 1 week.

 SELECT *
 FROM products
 WHERE product_id IN (
     SELECT product_id
     FROM sales
     WHERE sale_date > current_date - interval '60' day
     GROUP BY product_id
     HAVING COUNT(DISTINCT sale_date) > 30
 )
 AND product_id NOT IN (
     SELECT product_id
     FROM sales
     WHERE sale_date > current_date - interval '7' day
 )

 

we can use  the dimensional modelling star schema model for analytical queries needs and can also opt for snowflake model too in case if it involves multiple facts.


### Problem 2 - SQL Optimization

Our Data Analyst needs to compute a `fulfillment promised date` for each of the orderline deliveries based on the Service Level Agreements (SLA) of the logistics provider selected for the delivery. 

The fulfillment promised date is computed based on the fulfillment creation date plus the number of promised working days for delivery provided by the SLA of the logistics provider.

The `working_days` table contains the working days of the world up to 2030.

Part of the query he is using is as follows:

```
select
...
sum(is_working_day::integer order by working_days.date) as number_of_work_days

...
from order_line
left join working_days
on order_line.fulfillment_creation_date < working_days.date
```

He is facing query timeout issues consistently when computing the provider KPIs. What are the steps you would take to help him with this problem?
 
**Solution:-**

For SQL optimization, we can follow the below check list for troubleshooting.

* Verify the execution plan to identify the step is having the issue

* see if any stale stats exists on the tables involved

* see if any indexing help for the query performance.

* verify the joins condition and ensure there is appropriate join condition and if possible joining on uniqueness row

for the above query issue, it looks like there is no proper join condition to filter approproate rows. we may need to limit the rows and then can apply even cartesian product if the working days is just one row.

 
