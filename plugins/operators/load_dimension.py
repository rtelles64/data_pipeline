from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """
    Loads dimension table.

    Attributes
    ----------
    redshift_conn_id : str
        Redshift connection id
    table : str
        Table name
    select_qry : str
        Query to load
    append_insert : bool, optional
        Insertion mode when loading dimension table (default is False)
    primary_key : str
        Table primary key

    Methods
    -------
    execute(context)
        Executes dimension table load
    """

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 table="",
                 select_qry="",
                 append_insert=False,
                 primary_key="",
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
        append_insert : bool, optional
            Insertion mode when loading dimension table (default is False)
        primary_key : str
            Table primary key
        '''

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.select_qry = select_qry
        self.append_insert = append_insert
        self.primary_key = primary_key

    def execute(self, context):
        '''
        Executes dimension table load.

        Parameters
        ----------
        context : dict
            Airflow context parameters
        '''
        # self.log.info('LoadDimensionOperator not implemented yet')
        self.log.info("Getting credentials")
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.append_insert:
            table_insert = f"""
                CREATE TEMP TABLE stage_{self.table} (LIKE {self.table});

                INSERT INTO stage_{self.table} {self.select_qry};

                DELETE FROM {self.table} USING stage_{self.table}
                WHERE {self.table}.{self.primary_key} =
                    stage_{self.table}.{self.primary_key};

                INSERT INTO {self.table}
                SELECT * FROM stage_{self.table};
            """
        else:
            table_insert = f"""
                INSERT INTO {self.table} {self.select_qry};
            """

            self.log.info("Clearing data from Dimension table")
            redshift_hook.run(f"TRUNCATE TABLE {self.table};")

        self.log.info("Loading data into Dimension table")
        redshift_hook.run(table_insert)
