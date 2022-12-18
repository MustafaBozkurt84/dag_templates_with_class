# ETL DAGs for Airflow
This repository contains DAGs for extracting data from PostgreSQL, loading data into Snowflake, and transferring data between Snowflake and PostgreSQL. It also includes DAGs for transferring data from Redshift to Snowflake.

## Setting up the environment
Before using these DAGs, you will need to install the necessary libraries and set up connections in Airflow.

## Using the DAGs
The DAGs can be found in the dags directory. Each DAG has its own set of parameters that can be specified in the default_args dictionary at the top of the DAG file.

## Additional resources
This repository also includes ETL classes in the etl directory for performing data integration tasks in a more flexible way.

Make sure to test your DAGs and ETL processes thoroughly before deploying them to a production environment.

We hope these DAGs and ETL classes are helpful in your data integration tasks! If you have any questions or suggestions, feel free to reach out.
