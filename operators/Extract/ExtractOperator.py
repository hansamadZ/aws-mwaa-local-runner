import json
from airflow.models.baseoperator import BaseOperator

# define the class inheriting from an existing operator class
class ExtractOperator(BaseOperator):
    """
    Operator takes in json string and returns a data_dict
    """

    # define the .__init__() method that runs when the DAG is parsed
    def __init__(self, data_string, *args, **kwargs):
        # initialize the parent operator
        super().__init__(*args, **kwargs)
        # assign class variables
        self.data_string = data_string

    # define the .execute() method that runs when a task uses this operator.
    # The Airflow context must always be passed to '.execute()', so make
    # sure to include the 'context' kwarg.
    def execute(self, context):
        # write to Airflow task logs
        self.log.info(self.data_string)
        data_dict = json.loads(self.data_string)
        # the return value of '.execute()' will be pushed to XCom by default
        return data_dict
