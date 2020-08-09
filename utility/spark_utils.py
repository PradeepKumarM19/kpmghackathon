from string import Template

#class SparkUtils:
def prep_spark_submit(configuration):
    spark_submit_template = Template('spark-submit --master $mode $configs $py_files $app_path $arguments')
    mode = configuration['mode']
    configs = prepare_configuration(configuration)
    py_files = prepare_py_files(configuration)
    app_path = configuration['app_path']
    arguments = prepare_arguments(configuration)
    spark_submit = spark_submit_template.substitute(
        mode = mode,
        configs = configs,
        py_files = py_files,
        app_path = app_path,
        arguments = arguments,
    )
    return spark_submit

def prepare_configuration(configuration):
    config = [f"--${key} ${value}" for key, value in configuration["spark_conf"].items()]
    return " ".join(config)

def prepare_py_files(configuration):
    return f"--py_files ${configuration['egg_path']}"

def prepare_arguments(configuration):
    return " ".join(configuration['arguments'])


def read_file(spark, input_path, sep=',', header='true'):
    return (
        spark.read.option("delimiter",sep).option("header",header).csv(input_path)
    )


def read_table(spark, input_table):
    return spark.read.table(input_table)


def read_data(spark, input_type, input_name):
    if input_type == 'table':
        return read_table(spark, input_name)
    elif input_type == 'file':
        return read_file(spark, input_name)
    else:
        raise ValueError("incompatible input type provided!")
