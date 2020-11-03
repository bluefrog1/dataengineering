from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators import PostgresOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                               LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

AWS_KEY = os.environ.get('AWS_KEY')
AWS_SECRET = os.environ.get('AWS_SECRET')

# Defining default_args for future dag
default_args = {
    'owner': 'udacity',
    'start_date': datetime(2020, 10, 22),
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 3, 
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'catchup': False        
}

# Creating dag 
dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          template_searchpath='/home/workspace/airflow/'
        )

# Credential data and redshift parameters for connection
connection_info = {
        'conn_id': "redshift",
        'aws_credentials_id': "aws_credentials",                
        's3_bucket': "udacity-dend",
        'file_format': f"s3://udacity-dend/log_json_path.json"
}

# Operator that starts pipline
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# Operator that create tables
create_tables_task = PostgresOperator(
    task_id="Create_tables",
    dag=dag,
    postgres_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    sql='create_tables.sql'
)


# Operator that moves log data from S3 server to Redshift
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    conn_id=connection_info["conn_id"],
    aws_credentials_id=connection_info["aws_credentials_id"],
    table="staging_events",                 
    s3_bucket=connection_info["s3_bucket"],
    s3_key="log_data/2018/11/",
    file_format=connection_info["file_format"]  
)

# Operator that moves song data from S3 server to Redshift
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    conn_id=connection_info["conn_id"],
    aws_credentials_id=connection_info["aws_credentials_id"],
    table="staging_songs",                 
    s3_bucket=connection_info["s3_bucket"],
    s3_key="song_data/A/B/C/",
    file_format='auto'
)

# Operator that transform data from staged tables to fact table
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    conn_id=connection_info["conn_id"],
    aws_credentials_id=connection_info["aws_credentials_id"],
    table="songplays", 
    query=SqlQueries.songplay_table_insert
)

# Operators that transform data from staged tables to dimension tables
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    mode='truncate',
    conn_id=connection_info["conn_id"],
    aws_credentials_id=connection_info["aws_credentials_id"],
    table="users", 
    query=SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    conn_id=connection_info["conn_id"],
    aws_credentials_id=connection_info["aws_credentials_id"],
    table="songs", 
    query=SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    conn_id=connection_info["conn_id"],
    aws_credentials_id=connection_info["aws_credentials_id"],
    table="artists", 
    query=SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    conn_id=connection_info["conn_id"],
    aws_credentials_id=connection_info["aws_credentials_id"],
    table="time", 
    query=SqlQueries.time_table_insert
)

# Checking quality of final star schema
# check_query should contain list of dictionary, where one dictionary contain information about one test
# check_type contain information about type of test(it can be null checking, count of rows, ect.)
check_query=[{"table":"songplays", "check_type":"null", "column":"userid", "expected_results":0}]

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    conn_id=connection_info["conn_id"],
    aws_credentials_id=connection_info["aws_credentials_id"],
    query=check_query
)

# Operator that ends the pipeline
end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Define dependancies
start_operator >>  (stage_events_to_redshift, stage_songs_to_redshift) >> load_songplays_table
load_songplays_table >> (load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table) >> run_quality_checks >> end_operator
