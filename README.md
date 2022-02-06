## Database Purpose 
This database is an analytical database used to provide historical data analysis for Sparkify. It is important to note that it is *not* an operational database used for serving transactions to Sparkify application users. Implementing this analytical database seperately from their transactional database allows analytical queries to be run without impacting performance for app users, it also allows the analytical database to implement a denormalized data model that is more suited for analytical queries.

This database will enable Sparkify to identify what type of music appeals most to which users, so that they can tailor their app & music offering. For example, if a certain artist is most played by Females in the 20-30 year age bracket, then going forward they would promote that artist for all Females in the 20-30 year age bracket. This is just one example of how insights can be drawn from the analytic database and then used to tailor product or service offerings to different user groups.

## Database Schema Design and ETL Process
**Database design**

The database is a traditional star schema where the fact table records songplay events, and the dimensional tables (users, artists, songs, time) all provide further information to help describe each songplay event.

The database was designed in order to optimize queries for songplay analysis. After analyzing the source files provided, it was clear that the log_data provided captured the event where a Sparkify user plays a song, and hence a central 'Songplay' fact table was created. The columns for this were specified based on the project instructions & the datatypes were determined following analysis of the source file. The physical schema for this table can be see in the *songplay_table_create* method in the *sql_queries.py* file

After analysing the song_data files it was clear that users, artists, songs and time where appropriate dimenions to describe each songplay event & the columns for these tables were specified based on the project instructions & the datatypes were determined following analysis of the source files. The physical data models are specified in the *sql_queries.py* file.

**ETL design**

The ETL process was designed to first copy all files from S3 into staging tables on redshift, then in a subsequent step data is copied from redshift staging tables to analytical tables. This follows a traditional ETL pattern where the first 'Extract' step is to take data unchanged from source to staging, then to 'Transform' as a subsequent step. This is preferred so that we always have an exact copy of source replica data available, making it easier to re run our piplelines in case we need to troubleshoot

## Files in Repository
**dwh.cfg**

Contains the required config necessary for the create_dwh.py, create_tables.py, etl.py & delete_dwh.py files to execute successfully.

**create_dwh.py**

This file contains required python code to create a redshift cluster when executed via the command line or a Jupyter notebook. 
Pre-requisites: 
- dwh.cfg config file must be in the same directory as this file when it is executed

**create_tables.py**

This file contains required python code to create the required sparkify staging and analytical tables. 
Pre-requisites: 
- dwh.cfg config file must be in the same directory as this file when it is executed
- create_dwh.py file must have already been executed and resulting redshift cluster status is equal to Available

**sql_queries.py**

Contains all SQL queries needed to:
- Drop the existing database (and all tables)
- Create the database (and all tables, including staging & analytical tables)
- Select rows
- Insert rows

**delete_dwh.py**

This file contains required python code to drop the sparkify datawarehouse and tables.

**Project_Overview.ipynb**

Acts as a development & unit test environment where the last execution of the 'create_dwh.py', 'create_tables.py', 'etl.py' & 'delete_dwh.py' files is recorded

**etl.py**

Contains functions necessary to process all song data files and log data files in order to populate the staging and analytical databases.

**How to run this package**

In order to create the required database & execute the ETL pipeline:
1. Create an IAM role & user with admin access, able to access your AWS account programmatically
2. Populate the dwh.cfg config file with the required IAM info, DWH cluster info (i.e the required specs for the cluster to be created) and the S3 source info (Note: this is described in the 'Implementing datawarehouses on AWS' lesson)
3. Execute the 'create_dwh.py' script via the command line or a Jupyter Notebook
4. Check via the Amazon Admin UI that the provisioned Redshift cluster is 'Available'
5. Execute the 'create_tables.py' script via the command line or a Jupyter Notebook
6. Execute the 'etl.py' script via the command line or a Jupyter Notebook
7. Validate the data was copied by querying via the Redshift Query Editor
9. Execute the 'delete_dwh.py' script via the command line of a Jupyter Notebook to avoid any unnessecary costs