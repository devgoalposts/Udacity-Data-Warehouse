## ETL Pipeline using S3 and Amazon Redshift using JSON

Take logs from a music streaming app and connect contextualize that data with a subset of the [Million Song Dataset](http://millionsongdataset.com/).

## Motivation

Learn how to create a ETL pipeline using data stored in the cloud. This involved taking data from  AWS S3, putting it into staging tables in AWS Redshift, then transforming the data from the staging table into the appropriate table within the designed schema.

## Schema

![ERD Diagram](RedShiftProject.png)

## Tests

* [✅] run `python3 etl.py` without getting an errors
  * ~7 Min Runtime on 2 Node Cluser
    
## Eplanation of the files in the repository

`sql_queries.py` is where the core code of this project resides. It includes the queries to create the tables, copy the data from S3 into redshift, and sql insert queries that transform the data within redshift. This file even organizes the queries into lists so they can be easily be run and called together.

`etl.py` is where all the required sql queries from sql_queries.py get called in order to get the data from S3 into the correct schema inside redshift.

The purposes for each of the python notebooks is help me understand the data I am playing with so I can do small ETL experiments that do not require iterating over all the files in the S3 Bucket. I also used it to document my thinking process while coming up with these queries, for example when I write select queries separately before writing the insert query.
