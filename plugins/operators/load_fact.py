from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """
    Loads fact table.

    Attributes
    ----------
    redshift_conn_id : str
        Redshift connection id
    table : str
        Table name
    select_qry : str
        Query to load

    Methods
    -------
    execute(context)
        Executes fact table load
    """

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 table="",
                 select_qry="",
                 *args, **kwargs):
        '''
        Parameters
        ----------
        redshift_conn_id : str
            Redshift connection id
        table : str
            Table name
        select_qry : str
            Query to load
        '''

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.select_qry = select_qry

    def execute(self, context):
        '''
        Executions fact table load.

        Parameters
        ----------
        context : dict
            Airflow context parameters
        '''
        # self.log.info('LoadFactOperator not implemented yet')

        self.log.info("Getting credentials")
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Loading data into Fact table")
        table_insert = f"""
            INSERT INTO {self.table} {self.select_qry};
        """

        redshift_hook.run(table_insert)
