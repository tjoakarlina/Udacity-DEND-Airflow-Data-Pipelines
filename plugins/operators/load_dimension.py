from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """
        load data to dimension table from staging tables
    """
    template_fields = ("sql_insert_statement", "sql_truncate_statement")
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 sql_insert_statement,
                 target_table,
                 sql_truncate_statement,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_insert_statement = sql_insert_statement
        self.target_table = target_table
        self.sql_truncate_statement = sql_truncate_statement


    def execute(self, context):
        """
            Perform upsert to the target table based on the provided sql_insert_statement and sql_truncate_statement
        """
        self.log.info(f'LoadDimensionOperator for table {self.target_table}')

        redshift_hook = PostgresHook(self.redshift_conn_id)

        select_sql_statement = self.sql_insert_statement.format(**context)
        truncate_sql_statement = self.sql_truncate_statement.format(**context)
        upsert_sql_statement = f"""
                    begin transaction;

                    {truncate_sql_statement}
                    INSERT INTO {self.target_table} {select_sql_statement}

                    end transaction;
                """

        redshift_hook.run(upsert_sql_statement)
