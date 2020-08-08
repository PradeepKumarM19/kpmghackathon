from string import Template

class SparkUtils:
    def prep_spark_submit(self, configuration):
        spark_submit_template = Template('spark-submit --master $mode $configs $py_files $app_path $arguments')
        mode = configuration['mode']
        configs = self.prepare_configuration(configuration)
        py_files = self.prepare_py_files(configuration)
        app_path = configuration['app_path']
        arguments = self.prepare_arguments(configuration)
        spark_submit = spark_submit_template.substitute(
            mode = mode,
            configs = configs,
            py_files = py_files,
            app_path = app_path,
            arguments = arguments,
        )
        return spark_submit

    def prepare_configuration(self, configuration):
        config = [f"--${key} ${value}" for key, value in configuration["spark_conf"].items()]
        return " ".join(config)

    def prepare_py_files(self, configuration):
        return f"--py_files ${configuration['egg_path']}"

    def prepare_arguments(self, configuration):
        return " ".join(configuration['arguments'])

    @staticmethod
    def read_file(spark, input_path, sep=',', header='true'):
        return (
            spark.read.option("delimiter",sep).option("header",header).csv(input_path)
        )

    @staticmethod
    def read_table(spark, input_table):
        return spark.read.table(input_table)

    @staticmethod
    def read_data(spark, input_type, input_name):
        if input_type == 'table':
            return self.read_table(input)
        elif input_type == 'file':
            return self.read_file(spark, input_name)
        else:
            raise ValueError("incompatible input type provided!")
