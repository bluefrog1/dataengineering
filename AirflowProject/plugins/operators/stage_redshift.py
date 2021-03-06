from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)

    @apply_defaults
    def __init__(self,
                 conn_id="",
                 aws_credentials_id="",
                 table="",                 
                 s3_bucket="", 
                 s3_key="",
                 file_format="",
                 *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=conn_id
        self.aws_credentials_id=aws_credentials_id
        self.table=table
        self.s3_bucket=s3_bucket
        self.s3_key=s3_key
        self.file_format=file_format     

    def execute(self, context):            
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        self.log.info("Success: AWS Hook Defined.")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Success: Redshift Hook Defined.")
            
        # Deleting data from staging table
        self.log.info(f"Deleting data from {self.table}")
        redshift.run(f"DELETE FROM {self.table}")
        
        # Rendering S3
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        
        # Copying data from S3 to Redshift
        self.log.info(f"Copying data from {s3_path} to Redshift")
       
        copy_query = """
            COPY {table}
            FROM 's3://{s3_bucket}/{s3_key}'
            with credentials
            'aws_access_key_id={access_key};aws_secret_access_key={secret_key}'
            format json '{file_format}' ;
        """.format(table=self.table,
                   s3_bucket=self.s3_bucket,
                   s3_key=self.s3_key,
                   access_key=credentials.access_key,
                   secret_key=credentials.secret_key,
                   file_format=self.file_format)
       
        redshift.run(copy_query)
        