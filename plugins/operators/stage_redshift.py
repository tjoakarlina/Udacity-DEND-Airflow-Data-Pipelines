from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    """ Stage data from S3 to Redshift table"""
    ui_color = '#358140'
    template_fields = ("s3_key",)
    copy_sql = """
            COPY {}
            FROM '{}'
            ACCESS_KEY_ID '{}'
            SECRET_ACCESS_KEY '{}'
            JSON '{}';
        """

    @apply_defaults
    def __init__(self,
                 aws_credentials_id,
                 redshift_conn_id,
                 table,
                 s3_bucket,
                 s3_key,
                 truncate,
                 json_option="auto",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.truncate = truncate
        self.json_option = json_option


    def execute(self, context):
        """
            COPY data from the specified S3 bucket and key to the specified Redshift table
        """
        self.log.info(f'StageToRedshiftOperator from s3://{self.s3_bucket}/{self.s3_key} to redshift table {self.table}')
        aws_hook = AwsHook(self.aws_credentials_id, client_type="s3")
        aws_credentials = aws_hook.get_credentials()

        redshift_hook = PostgresHook(self.redshift_conn_id)

        if self.truncate:
            self.log.info("Clearing data from destination Redshift table")
            redshift_hook.run("DELETE FROM {};".format(self.table))


        rendered_key = self.s3_key.format(**context)

        keepalive_kwargs = {
            "keepalives": 1,
            "keepalives_idle": 30,
            "keepalives_interval": 5,
            "keepalives_count": 5,
        }


        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        self.log.info(f"Copy data from {s3_path} to redshift")
        sql_query = self.copy_sql.format(self.table, s3_path, aws_credentials.access_key, aws_credentials.secret_key, self.json_option)
        redshift_hook.run(sql_query, parameters=keepalive_kwargs)









