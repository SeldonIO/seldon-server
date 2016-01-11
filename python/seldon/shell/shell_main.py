from cmd2 import Cmd, make_option, options
import os,sys
import argparse
from kazoo.client import KazooClient
import json
import os.path
import errno

import cmd_client
import cmd_attr
import cmd_model
import cmd_alg
import cmd_db
import cmd_memcached
import cmd_import
import pprint

gdata = {
    "zk_client": None,
    "conf_data": None,
    "conf_path": os.path.expanduser("~/.seldon/seldon.conf"),
    'help_cmd_strs_list' : [
        ("attr", "configure attributes for client"),
        ("alg", "configure algs for client"),
        ("client", "configure client"),
        ("model", "configure model for client"),
        ("db", "configure databases"),
        ("memcached", "configure memcached"),
        ("import", "static import of data")
    ],
    'help_formatting' : { 'lmargin_size' : 4, 'cmd_size' : 40 },
}

def pp(o):
    p = pprint.PrettyPrinter(indent=4)
    p.pprint(o)

def get_seldon_banner():
    return '''\
   _____      __    __
  / ___/___  / /___/ /___  ____
  \__ \/ _ \/ / __  / __ \/ __ \\
 ___/ /  __/ / /_/ / /_/ / / / /
/____/\___/_/\__,_/\____/_/ /_/ \
'''

def get_default_conf():
    return '''\
{
    "default_algorithms": {
        "assocRuleRecommender": {
            "config": []
        },
        "dynamicClusterCountsRecommender": {
            "config": []
        },
        "externalItemRecommendationAlgorithm": {
            "config": [
                {
                    "name": "io.seldon.algorithm.inclusion.itemsperincluder",
                    "value": 100000
                },
                {
                    "name": "io.seldon.algorithm.external.url",
                    "value": "http://127.0.0.1:5000/recommend"
                },
                {
                    "name": "io.seldon.algorithm.external.name",
                    "value": "example_alg"
                }
            ],
            "includers": [
                "recentItemsIncluder"
            ]
        },
        "globalClusterCountsRecommender": {
            "config": []
        },
        "itemCategoryClusterCountsRecommender": {
            "config": []
        },
        "itemClusterCountsRecommender": {
            "config": []
        },
        "itemSimilarityRecommender": {
            "config": []
        },
        "mfRecommender": {
            "config": []
        },
        "mostPopularRecommender": {
            "config": []
        },
        "recentItemsRecommender": {
            "config": []
        },
        "recentMfRecommender": {
            "config": [
                {
                    "name": "io.seldon.algorithm.general.numrecentactionstouse",
                    "value": "1"
                }
            ]
        },
        "semanticVectorsRecommender": {
            "config": []
        },
        "userTagAffinityRecommender": {
            "config": []
        }
    },
    "default_models": {
        "cluster-by-dimension": {},
        "matrix-factorization": {
            "config": {
                "activate": true,
                "alpha": 1,
                "days": 1,
                "inputPath": "%SELDON_MODELS%",
                "iterations": 5,
                "lambda": 0.01,
                "outputPath": "%SELDON_MODELS%",
                "rank": 30,
                "startDay": 1
            },
            "training": {
                "job_info": {
                    "cmd": "%SPARK_HOME%/bin/spark-submit",
                    "cmd_args": [
                        "--class",
                        "io.seldon.spark.mllib.MfModelCreation",
                        "--master",
                        "local[1]",
                        "%SELDON_SPARK_HOME%/target/seldon-spark-%SELDON_VERSION%-jar-with-dependencies.jar",
                        "--client",
                        "%CLIENT_NAME%",
                        "--zookeeper",
                        "%ZK_HOSTS%"
                    ]
                },
                "job_type": "spark"
            }
        },
        "semvec": {},
        "similar-items": {},
        "tagaffinity": {},
        "tagcluster": {}
    },
    "seldon_models": "~/.seldon/seldon-models",
    "seldon_spark_home": "~/seldon-server/offline-jobs/spark",
    "seldon_version": "0.93",
    "spark_home": "~/apps/spark",
    "zk_hosts": "localhost:2181",
    "zkroot": "~/.seldon/zkroot"
}
'''

def completions_helper(help_cmd_strs_list, text, line):
    completions = [x[0].partition(' ')[2].partition(' ')[0] for x in help_cmd_strs_list if len(x[0].split(' '))>1]
    mline = line.partition(' ')[2]
    offs = len(mline) - len(text)
    return [s[offs:] for s in completions if s.startswith(mline)]

def json_to_dict(json_data):
    return json.loads(json_data)

def dict_to_json(d, expand=False):
    return json.dumps(d, sort_keys=True, indent=4, separators=(',', ': ')) if expand else json.dumps(d, sort_keys=True, separators=(',',':'))

def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc: # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else: raise

def getOpts():
    parser = argparse.ArgumentParser(description='Seldon Shell')
    parser.add_argument('-d', "--debug", action='store_true', help="turn on debugging")
    parser.add_argument('-q', "--quiet", action='store_true', help="only display important messages, useful in non-interactive mode")
    parser.add_argument('--zk-hosts', help="the zookeeper hosts", required=False)
    parser.add_argument('--version', action='store_true', help="print the version", required=False)
    parser.add_argument('args', nargs=argparse.REMAINDER) # catch rest (non-options) as args
    opts = parser.parse_args()
    return opts

class CmdLineApp(Cmd):

    def do_ping(self, arg, opts=None):
        print "pong"

    def do_conf(self, arg, opts=None):
        pp(gdata["conf_data"])

    def do_gdata(self, arg, opts=None):
        pp(gdata)

    def do_db(self, arg, opts=None):
        command_data = {
                'zkdetails' : {'zkroot': gdata['conf_data']['zkroot'], 'zk_client': gdata['zk_client']},
                'help_formatting' : gdata["help_formatting"],
        }
        cmd_db.cmd_db(arg, command_data)

    def complete_db(self, text, line, start_index, end_index):
        help_cmd_strs_list = cmd_db.gdata["help_cmd_strs_list"]
        return completions_helper(help_cmd_strs_list, text, line)

    def do_memcached(self, arg, opts=None):
        command_data = {
                'zkdetails' : {'zkroot': gdata['conf_data']['zkroot'], 'zk_client': gdata['zk_client']},
                'help_formatting' : gdata["help_formatting"],
        }
        cmd_memcached.cmd_memcached(arg, command_data)

    def complete_memcached(self, text, line, start_index, end_index):
        help_cmd_strs_list = cmd_memcached.gdata["help_cmd_strs_list"]
        return completions_helper(help_cmd_strs_list, text, line)

    def do_client(self, arg, opts=None):
        command_data = {
                'zkdetails' : {'zkroot': gdata['conf_data']['zkroot'], 'zk_client': gdata['zk_client']},
                'help_formatting' : gdata["help_formatting"],
        }
        cmd_client.cmd_client(arg, command_data)

    def complete_client(self, text, line, start_index, end_index):
        help_cmd_strs_list = cmd_client.gdata["help_cmd_strs_list"]
        return completions_helper(help_cmd_strs_list, text, line)

    def do_alg(self, arg, opts=None):
        command_data = {
                'zkdetails' : {'zkroot': gdata['conf_data']['zkroot'], 'zk_client': gdata['zk_client']},
                'default_algorithms' : gdata['conf_data']['default_algorithms'],
                'help_formatting' : gdata["help_formatting"],
        }
        cmd_alg.cmd_alg(arg, command_data)

    def complete_alg(self, text, line, start_index, end_index):
        help_cmd_strs_list = cmd_alg.gdata["help_cmd_strs_list"]
        return completions_helper(help_cmd_strs_list, text, line)

    def do_attr(self, arg, opts=None):
        command_data = {
                'zkdetails' : {'zkroot': gdata['conf_data']['zkroot'], 'zk_client': gdata['zk_client']},
                'help_formatting' : gdata["help_formatting"],
        }
        cmd_attr.cmd_attr(arg, command_data)

    def complete_attr(self, text, line, start_index, end_index):
        help_cmd_strs_list = cmd_attr.gdata["help_cmd_strs_list"]
        return completions_helper(help_cmd_strs_list, text, line)

    def do_model(self, arg, opts=None):
        command_data = {
                'zkdetails' : {'zkroot': gdata['conf_data']['zkroot'], 'zk_client': gdata['zk_client']},
                'default_models' : gdata['conf_data']['default_models'],
                'seldon_models' : gdata['conf_data']['seldon_models'],
                'conf_data' : gdata['conf_data'],
                'help_formatting' : gdata["help_formatting"],
        }
        cmd_model.cmd_model(arg, command_data)

    def complete_model(self, text, line, start_index, end_index):
        help_cmd_strs_list = cmd_model.gdata["help_cmd_strs_list"]
        return completions_helper(help_cmd_strs_list, text, line)

    def do_import(self, arg, opts=None):
        command_data = {
                'conf_data' : gdata['conf_data'],
                'help_formatting' : gdata["help_formatting"],
        }
        cmd_import.cmd_import(arg, command_data)

    def complete_import(self, text, line, start_index, end_index):
        help_cmd_strs_list = cmd_import.gdata["help_cmd_strs_list"]
        return completions_helper(help_cmd_strs_list, text, line)

    def do_help(self, arg, opts=None):
        lmargin_size=4
        cmd_size=40
        lmargin_pad=" "
        for help_strs in gdata["help_cmd_strs_list"]:
            cmd_str = help_strs[0]
            cmd_help = help_strs[1]
            print "{lmargin_pad:<{lmargin_size}}{cmd_str:<{cmd_size}} - {cmd_help}".format(**locals())

    def do_test(self, arg, opts=None):
        conf_data = gdata['conf_data']
        print dict_to_json(conf_data, False)

    do_m = do_memcached # an alias
    do_c = do_client # an alias
    do_a = do_alg # an alias
    do_t = do_test # an alias

def expand_conf():
    conf_data = gdata['conf_data']

    expansion_list = ["zkroot","seldon_models","spark_home","seldon_spark_home"]
    for expansion_item in expansion_list:
        conf_data[expansion_item] = os.path.expanduser(conf_data[expansion_item])

def check_conf():
    def create_default_conf(fpath):
        default_conf = get_default_conf()
        mkdir_p(os.path.dirname(fpath))
        d = json_to_dict(default_conf)
        zkroot = os.path.expanduser(d["zkroot"])
        mkdir_p(zkroot)
        seldon_models = os.path.expanduser(d["seldon_models"])
        mkdir_p(seldon_models)
        default_conf = json.dumps(d, sort_keys=True, indent=4, separators=(',', ': '))
        f = open(fpath, 'w')
        f.write(default_conf)
        f.write('\n')
        f.close()
        print "Created conf file [{fpath}] with default settings.".format(**locals())
        print "Edit this file and re-run."
        sys.exit(0)
    fpath = gdata['conf_path']
    if not os.path.isfile(fpath):
        create_default_conf(fpath)
    else:
        with open(fpath) as data_file:
            gdata["conf_data"] = json.load(data_file)

def start_zk_client(opts):
    zk_hosts = opts.zk_hosts
    if not opts.quiet:
        sys.stdout.write("connecting to "+zk_hosts)
    gdata["zk_client"] = KazooClient(hosts=zk_hosts)
    gdata["zk_client"].start()
    res = "SUCCEEDED" if gdata["zk_client"].connected else "FAILED"
    if not opts.quiet:
        print " [{res}]".format(**locals())

def stop_zk_client():
    if gdata["zk_client"].connected:
        gdata["zk_client"].stop()

def main():
    check_conf()
    expand_conf()
    opts = getOpts()
    if opts.version == True:
        from seldon import __version__
        print __version__
        sys.exit(0)

    if not opts.quiet:
        print get_seldon_banner(); print ""

    if opts.zk_hosts != None:
        gdata["conf_data"]["zk_hosts"] = opts.zk_hosts
    else:
        opts.zk_hosts = gdata["conf_data"]["zk_hosts"]

    if opts.debug:
        pp(opts)
        pp(gdata)

    start_zk_client(opts)
    Cmd.prompt = "seldon> " if not opts.quiet else ""
    sys.argv = [sys.argv[0]] # the --zk-hosts argument upsets app
    app = CmdLineApp()
    app.cmdloop()
    stop_zk_client()

if __name__ == '__main__':
    main()

