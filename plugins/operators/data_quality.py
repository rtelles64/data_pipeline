from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    """
    Runs quality checks on data.

    Attributes
    ----------
    redshift_conn_id : str
        Redshift connection id
    test_query : str
        Query to run quality check on
    expected_result : str
        Result to run test query against

    Methods
    -------
    execute(context)
        Executes data quality check
    """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 test_query="",
                 expected_result="",
                 *args, **kwargs):
        '''
        Parameters
        ----------
        redshift_conn_id : str
            Redshift connection id
        test_query : str
            Query to run quality check on
        expected_result : str
            Result to run test query against
        '''

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.test_query = test_query
        self.expected_result = expected_result

    def execute(self, context):
        '''
        Executes data quality check.

        Parameters
        ----------
        context : dict
            Airflow context parameters

        Raises
        ------
        ValueError
            If test query does not match expected result
        '''
        # self.log.info('DataQualityOperator not implemented yet')

        self.log.info("Obtaining credentials")
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Running quality check")
        records = redshift_hook.get_records(self.test_query)

        if records[0][0] != self.expected_result:
            raise ValueError(f"""
                Data quality check failed:
                    {records[0][0]} does not equal {self.expected_result}
            """)
        else:
            self.log.info("Quality check passed")
