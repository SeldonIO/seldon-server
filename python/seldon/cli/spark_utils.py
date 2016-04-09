import pprint
from subprocess import call
import sys

def pp(o):
    p = pprint.PrettyPrinter(indent=4)
    p.pprint(o)

def run_spark_job(command_data, job_info, client_name):
    conf_data = command_data["conf_data"]
    spark_home = conf_data["spark_home"]
    seldon_spark_home = conf_data["seldon_spark_home"]
    seldon_version = conf_data["seldon_version"]
    zk_hosts = conf_data["zk_hosts"]
    seldon_models = conf_data["seldon_models"]
    seldon_logs = conf_data["seldon_logs"]

    cmd = job_info["cmd"].replace("%SPARK_HOME%", spark_home)

    cmd_args = job_info["cmd_args"]

    replacements = [
        ("%CLIENT_NAME%", client_name),
        ("%SPARK_HOME%", spark_home),
        ("%SELDON_SPARK_HOME%", seldon_spark_home),
        ("%SELDON_VERSION%", seldon_version),
        ("%ZK_HOSTS%", zk_hosts),
        ("%SELDON_MODELS%", seldon_models),
        ("%SELDON_LOGS%", seldon_logs),
    ]

    def appy_replacements(item):
        for rpair in replacements:
            if rpair[1] != None:
                item = item.replace(rpair[0],rpair[1])
        return item

    cmd_args = map(appy_replacements, cmd_args)

    print "Running spark job"
    pp([cmd]+cmd_args)
    sys.stdout.flush()
    call([cmd]+cmd_args)

