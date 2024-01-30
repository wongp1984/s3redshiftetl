## Project Objective
The objective of this project is to build an ETL pipeline which extracts Sparkify's song data and user activity data from JSON log data currently stored in AWS S3, stages them in Redshift, transforms these data into a set of dimensional tables and fact table as an OLAP and loads them into Redshift for their analytics team to continue finding insights into what songs their users are listening to.


## OLAP Database Design
The original songs data and user data are stored in JSON files. These data are loaded and staged into two staging table, namely `staging_events_table` and `staging_events_table`. With an aim to faciliate analysis of songs playing behaviour, the songs playing information including who and when a song is played is stored in a FACT table called `songplays` with keys referencing a number of DIMENSION tables, including `songs`, `users`, `artists` and `time`. Such STAR schema design forms an OLAP cube to allow analytics team to rollup, slice and dice the database. In addition, to improve joining performance, all dimension tables have the "all" distribution strategy and designated sorting keys in the cluster to reduce the  need of shuffling, while the `songplay` table, which is fact table, has a designated distribution key and sort key.


## Query of Final Database
The number of records loaded in each table will be queried. In addition, the following queries are executed to select the Top N statistics from the database: 
- Top 10 ever most played songs 
- Top 5 highest usage time of day by hour for songs
- Top 10 hottest artists with most played songs


## How to Execute the Codes
1. Setup the cluster and database parameters in the `dwh.cfg` to create the cluster in AWS Redshift
2. Execute the `create_tables.py` module. For example, 

    `python3 create_tables.py`

3. Execute the `etl.py` module. For example,

    `python3 etl.py`
