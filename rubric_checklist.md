## Problems

* I should cast directly into the staging table rather than during the transformation step
* song_id    is not distinct in songs     table
* start_time is not distinct in time      table
* artist_id  is not distinct in artists   table
* user_id    is not distinct in the users table 
* Set the column names for all tables as the same

## Rubric Checklist

* Table Creation
  * [✅] Table creation script runs without errors.
  * [✅] Staging tables are properly defined.
  * [✅] Fact and dimensional tables for a star schema are properly defined.
* ETL
  * [✅] ETL script runs without errors.
  * [🌗] ETL script properly processes transformations in Python.
* Code Quality
  * [✅] The project shows proper use of documentation.
  * [✅] The project code is clean and modular.
* Suggestions
  * [🌗]  Add data quality checks
    * [🌗] Check Distinct users, artists, time
      * Did this at the end of the jupyter notebook
    * [✅] Fix issue with users getting indexed multiple times
  * [❌] Create a dashboard for analytic queries on your new database

## Emoji

* ✅ 🌗 ❌
