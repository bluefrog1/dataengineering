from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 conn_id="",
                 aws_credentials_id="",
                 table="",
                 query="",
                 mode="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=conn_id
        self.aws_credentials_id=aws_credentials_id
        self.table=table
        self.query=query
        self.mode=mode

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        self.log.info("Success: AWS Hook Defined.")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Success: Redshift Hook Defined.")
        
        # Deleting data from dimention table
        self.log.info(f"Deleting data from {self.table}")
        redshift.run(f"DELETE FROM {self.table}")
        
        # Inserting data from dimention table
        if self.mode.lower() == 'truncate':
            query = f"""
                TRUNCATE {self.table}; INSERT INTO {self.table} {self.query}
                """
        if self.mode.lower() == 'insert':
            query = f"""
                INSERT INTO {self.table} {self.query}
                """
        redshift.run(query)
