from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook  # easy connect to AWS

class StageToRedshiftOperator(BaseOperator):
    """
    Loads JSON formatted files from S3 to Redshift.

    Attributes
    ----------
    copy_qry : str
        Copy query to execute
    redshift_conn_id : str
        Redshift connection id
    aws_credentials : str
        AWS credentials
    table : str
        Table name
    s3_bucket : str
        S3 bucket name
    s3_key : str
        S3 bucket data path
    copy_json_option : str, optional
        JSON copy type (default is "auto")
    region : str
        AWS region where source data is located

    Methods
    -------
    execute(context)
        Executes S3 to Redshift staging
    """
    ui_color = '#358140'

    copy_qry = """
        COPY {} FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION AS '{}'
        FORMAT AS JSON '{}'
    """

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # redshift_conn_id=your-connection-name
                 redshift_conn_id="",
                 aws_credentials="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 copy_json_option="auto",
                 region="",
                 *args, **kwargs):
        '''
        Parameters
        ----------
        redshift_conn_id : str
            Redshift connection id
        aws_credentials : str
            AWS credentials
        table : str
            Table name
        s3_bucket : str
            S3 bucket name
        s3_key : str
            S3 bucket data path
        copy_json_option : str, optional
            JSON copy type (default is "auto")
        region : str
            AWS region where source data is located
        '''

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials = aws_credentials
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.copy_json_option = copy_json_option
        self.region = region

    def execute(self, context):
        '''
        Executes S3 to Redshift staging.

        Parameters
        ----------
        context : dict
            Airflow context parameters
        '''
        # self.log.info('StageToRedshiftOperator not implemented yet')

        self.log.info("Getting credentials")
        aws_hook = AwsHook(self.aws_credentials)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination table")
        redshift.run(f"DELETE FROM {self.table};")

        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = f"s3://{self.s3_bucket}/{rendered_key}"
        copy_sql = self.copy_qry.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.region,
            self.copy_json_option
        )

        redshift.run(copy_sql)
