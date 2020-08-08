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

