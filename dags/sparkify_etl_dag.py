from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.common.sql.operators.sql import (
    SQLColumnCheckOperator,
    SQLTableCheckOperator,
)
from airflow.utils.task_group import TaskGroup
from operators import StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator
from helpers import SqlQueries


default_args = {
    "owner": "karlina",
    "start_date": datetime(2018, 11, 1),
    "end_date": datetime(2018, 11, 30),
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
}

with DAG(
    "sparkify_etl_dag",
    default_args=default_args,
    description="Load and transform data in Redshift with Airflow",
    schedule_interval="@hourly",
    max_active_runs=1,
) as dag:

    start_operator = DummyOperator(task_id="Begin_execution")

    # Stage events from S3 to Redshift
    # The events data in S3 is partitioned by day
    # StageToRedshiftOperator clears the stage_events database and load the events data from S3 for a particular day (based on execution date)
    stage_events_to_redshift = StageToRedshiftOperator(
        task_id="Stage_events",
        aws_credentials_id="aws_credentials",
        redshift_conn_id="redshift",
        table="staging_events",
        s3_bucket="udacity-dend",
        s3_key="log_data/{execution_date.year}/{execution_date.month:02d}/{execution_date.year}-{execution_date.month:02d}-{execution_date.day:02d}-events.json",
        truncate=True,
        json_option="s3://udacity-dend/log_json_path.json",
    )

    # Stage songs from S3 to Redshift
    # The song data is partitioned in nested 3 level alphabetical folders
    # The song data is quite big and our Airflow cluster may not be able to handle the entire size, hence we will load the data by partitions
    # Stage_songs_group clears the stage_songs database and load the songs data from S3 by partitions
    with TaskGroup(group_id="Stage_songs") as stage_songs_group:
        clear_stage_songs = PostgresOperator(
            task_id="Clear_stage_songs",
            postgres_conn_id="redshift",
            sql="DELETE FROM STAGING_SONGS;",
        )
        import string

        for i in string.ascii_uppercase[:26]:
            clear_stage_songs >> StageToRedshiftOperator(
                task_id=f"Stage_songs_A_{i}",
                aws_credentials_id="aws_credentials",
                redshift_conn_id="redshift",
                table="staging_songs",
                s3_bucket="udacity-dend",
                s3_key=f"song_data/A/{i}/",
                truncate=False,
            )

    # Check the quality of stage_events
    # Ensure that there is data in the staging_events table
    check_staging_events_quality = SQLTableCheckOperator(
        task_id="check_staging_events_table",
        conn_id="redshift",
        table="staging_events",
        checks={"row_count_check": {"check_statement": "COUNT(*) >0"}},
    )

    # Check the quality of stage_songs
    # Ensure that there is data in the staging_songs table
    check_staging_songs_quality = SQLTableCheckOperator(
        task_id="check_staging_songs_table",
        conn_id="redshift",
        table="staging_songs",
        checks={"row_count_check": {"check_statement": "COUNT(*) >0"}},
    )

    # Create songplays fact table
    # Since we are going to run the DAG hourly and the events data come in a day size, there might be exact same songplays rows in the songplays table
    # We selectively truncate the rows which belong to the same date, before inserting the data
    load_songplays_table = LoadFactOperator(
        task_id="Load_songplays_fact_table",
        redshift_conn_id="redshift",
        sql_insert_statement=SqlQueries.songplay_table_insert,
        sql_truncate_statement=SqlQueries.songplay_table_truncate,
        target_table="songplays",
    )

    # Create user dimension table
    # To prevent inserting duplicate users, we truncate the rows which have the same userid as the ones we are going to insert
    load_user_dimension_table = LoadDimensionOperator(
        task_id="Load_user_dim_table",
        redshift_conn_id="redshift",
        sql_insert_statement=SqlQueries.user_table_insert,
        sql_truncate_statement=SqlQueries.user_table_truncate,
        target_table="users",
    )

    # Create song dimension table
    # We refresh the page for each dag run
    load_song_dimension_table = LoadDimensionOperator(
        task_id="Load_song_dim_table",
        redshift_conn_id="redshift",
        sql_insert_statement=SqlQueries.song_table_insert,
        sql_truncate_statement=SqlQueries.song_table_truncate,
        target_table="songs",
    )

    # Create artist dimension table
    # We refresh the page for each dag run
    load_artist_dimension_table = LoadDimensionOperator(
        task_id="Load_artist_dim_table",
        redshift_conn_id="redshift",
        sql_insert_statement=SqlQueries.artist_table_insert,
        sql_truncate_statement=SqlQueries.artist_table_truncate,
        target_table="artists",
    )

    # Create time dimension table
    # We refresh the time table for each dag run
    load_time_dimension_table = LoadDimensionOperator(
        task_id="Load_time_dim_table",
        redshift_conn_id="redshift",
        sql_insert_statement=SqlQueries.time_table_insert,
        sql_truncate_statement=SqlQueries.time_table_truncate,
        target_table="time",
    )

    # Run data quality check for the fact and dimension table
    # we utilize community provided SQLColumnCheckOperator to ensure the value for each column are within the correct values.
    with TaskGroup(group_id="Run_data_quality_checks") as data_quality_checks_group:
        check_songplays_columns = SQLColumnCheckOperator(
            task_id="check_songplays_columns",
            conn_id="redshift",
            table="songplays",
            column_mapping={
                "start_time": {
                    "min": {"greater_than": datetime(2017, 12, 1)},
                },
                "userid": {"null_check": {"equal_to": 0}},
                "level": {"distinct_check": {"leq_to": 2}},
            },
        )

        check_songs_columns = SQLColumnCheckOperator(
            task_id="check_songs_columns",
            conn_id="redshift",
            table="songs",
            column_mapping={
                "title": {"null_check": {"equal_to": 0}},
                "artistid": {"null_check": {"equal_to": 0}},
            },
        )

        check_time_columns = SQLColumnCheckOperator(
            task_id="check_time_columns",
            conn_id="redshift",
            table="time",
            column_mapping={
                "hour": {
                    "min": {"geq_to": 0},
                    "max": {"leq_to": 23},
                    "null_check": {"equal_to": 0},
                },
                "day": {
                    "min": {"geq_to": 1},
                    "max": {"leq_to": 31},
                    "null_check": {"equal_to": 0},
                },
                "week": {
                    "min": {"geq_to": 1},
                    "max": {"leq_to": 52},
                    "null_check": {"equal_to": 0},
                },
                "month": {
                    "min": {"geq_to": 1},
                    "max": {"leq_to": 12},
                    "null_check": {"equal_to": 0},
                },
                "year": {"min": {"greater_than": 2017}, "null_check": {"equal_to": 0}},
                "weekday": {
                    "min": {"geq_to": 0},
                    "max": {"leq_to": 7},
                    "null_check": {"equal_to": 0},
                },
            },
        )

        check_users_columns = SQLColumnCheckOperator(
            task_id="check_users_columns",
            conn_id="redshift",
            table="users",
            column_mapping={
                "first_name": {"null_check": {"equal_to": 0}},
                "last_name": {"null_check": {"equal_to": 0}},
                "gender": {"distinct_check": {"leq_to": 2}},
                "level": {"distinct_check": {"leq_to": 2}},
            },
        )

        check_artists_columns = SQLColumnCheckOperator(
            task_id="check_artists_columns",
            conn_id="redshift",
            table="artists",
            column_mapping={"name": {"null_check": {"equal_to": 0}}},
        )

    end_operator = DummyOperator(task_id="Stop_execution")

    start_operator >> stage_events_to_redshift
    start_operator >> stage_songs_group

    stage_events_to_redshift >> check_staging_events_quality
    stage_songs_group >> check_staging_songs_quality

    check_staging_events_quality >> load_songplays_table
    check_staging_songs_quality >> load_songplays_table

    load_songplays_table >> load_song_dimension_table
    load_songplays_table >> load_user_dimension_table
    load_songplays_table >> load_artist_dimension_table
    load_songplays_table >> load_time_dimension_table

    load_song_dimension_table >> data_quality_checks_group
    load_user_dimension_table >> data_quality_checks_group
    load_artist_dimension_table >> data_quality_checks_group
    load_time_dimension_table >> data_quality_checks_group

    data_quality_checks_group >> end_operator
