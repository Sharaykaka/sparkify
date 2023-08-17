from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):

    ui_color="#358140"

#the sql query will run if we need backfilling
    copy_sql_date = """
        COPY {} FROM '{}/{}/{}/'
        ACCESS_KEY_ID '{}
        SECRET_ACCESS_KEY '{}'
        REGION '{}'
        {} 'auto';
    """

    copy_sql = """

        COPY {} FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION '{}'
        {} 'auto';
    """

    @apply_defaults
    def __init__(self,
                redshift_conn_id="",
                aws_conn_id="",
                table="",
                s3_path="",
                region="us-west-2",
                data_format="",
                provide_context = True,
                *args,**kwargs):
        
        super(StageToRedshiftOperator, self).__init__(*args,**kwargs)
        self.log.info("Initializing StageToRedshiftOperator with parameters:")


        self.redshift_conn_id = redshift_conn_id
        self.log.info(f"Redshift Connection ID: {redshift_conn_id}")
        self.aws_conn_id = aws_conn_id
        self.log.info(f"AWS Connection ID: {aws_conn_id}")
        self.table = table
        self.log.info(f"Table: {table}")
        self.s3_path = s3_path
        self.log.info(f"s3 path: {s3_path}")
        self.region = region
        self.log.info(f"region: {region}")
        self.data_format = data_format
        self.log.info(f"data_format: {data_format}")
        #if we provide exection date then i will do the backfilling for that date
        self.execution_date = kwargs.get('execution_date')

        # self.log.info("Initializing StageToRedshiftOperator with parameters:")
        # self.log.info(f"Redshift Connection ID: {redshift_conn_id}")
        # self.log.info(f"AWS Connection ID: {aws_conn_id}")
        # self.log.info(f"Table: {table}")

    def execute(self, context):
        self.log.info("setting up aws conn id")
        aws = AwsHook(self.aws_conn_id)
        self.log.info("DONE setting up aws conn id")

        credentials = aws.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Deleting data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))
        self.log.info("Copying data from S3 to Redshift")


        if self.execution_date:
            self.log.info("execution date found,will do the backfilling for that date")
            formatted_sql = StageToRedshiftOperator.copy_sql_time.format(
                self.table,
                self.s3_path,
                self.execution_date.strftime("%Y"),
                self.execution_date.strftime("%d"),
                credentials.access_key,
                credentials.secret_key,
                self.region,
                self.data_format,
                self.execution_date
            )
        else:
            self.log.info("execution not date found will not do the backfilling for that date")
            self.log
            formatted_sql = StageToRedshiftOperator.copy_sql.format(
                self.table,
                self,s3_path,
                credentials.access_key,
                credentials.secret_key,
                self.region,
                self.data_format,
                self,execution_date
            )

        redshift.run(formatted_sql)