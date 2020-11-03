from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 conn_id="",
                 aws_credentials_id="",
                 table="",
                 query="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
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
        
        # Execute data tests depends on check_type parameter.
        # check_type = "null" means that test check specific column of table on null values
        for check_query in self.query:
            if check_query["check_type"] == "null":
                records=redshift.get_records(f"SELECT * FROM {check_query['table']} WHERE {check_query['column']} is NULL")
                if len(records) == check_query['expected_results']:
                    self.log.info(f"Table {check_query['table']} has no NULL data in {check_query['column']} column.")
                else:
                    raise ValueError(f"Data quality check failed. {check_query['column']} in table {check_query['table']} has a {len(records)} null values. Expected value is {check_query['expected_results']}")
        
        
        
        