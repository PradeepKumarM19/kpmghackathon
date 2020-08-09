
set CURRENTDIR=%~dp0
spark-submit "%CURRENTDIR%starter.py" --config_file "%CURRENTDIR%config.csv"