from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 conn_id="",
                 aws_credentials_id="",
                 table="",
                 query="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=conn_id
        self.aws_credentials_id=aws_credentials_id
        self.table=table
        self.query=query

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        self.log.info("Success: AWS Hook Defined.")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Success: Redshift Hook Defined.")
        
        # Deleting data from fact table
        self.log.info(f"Deleting data from {self.table}")
        redshift.run(f"DELETE FROM {self.table}")
        # Inserting data to fact table
        redshift.run(f"INSERT INTO {self.table} ({self.query})")
