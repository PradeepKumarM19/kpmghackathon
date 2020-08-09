import subprocess

def run_command(command):
    if type(command) == "string":
        command = command.split(" ")
    subprocess.run(command)
