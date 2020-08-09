
from spark_utils import SparkUtils
import shell_utils

class TransformCommand:
    def __init__(self):
        pass

    def convert(self, config):
        submit = SparkUtils.prep_spark_submit(config)
        shell_utils.run_command(submit)

