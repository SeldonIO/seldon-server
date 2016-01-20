import sys
import argparse
import pprint
import os
import json
from kazoo.client import KazooClient

import cmd_memcached

gdata = {
    'zk_client': None,
    'conf_data': None,
    'conf_path': os.path.expanduser("~/.seldon/seldon.conf"),
    'cmds': {
        'memcached' : cmd_memcached.cmd_memcached,
    }
}

def pp(o):
    p = pprint.PrettyPrinter(indent=4)
    p.pprint(o)

def getOpts():
    parser = argparse.ArgumentParser(prog='seldon-cli', description='Seldon Cli')
    parser.add_argument('--version', action='store_true', help="print the version", required=False)
    parser.add_argument('--debug', action='store_true', help="debugging flag", required=False)
    parser.add_argument('--zk-hosts', help="the zookeeper hosts", required=False)
    parser.add_argument('-q', "--quiet", action='store_true', help="only display important messages, useful in non-interactive mode")
    parser.add_argument('args', nargs=argparse.REMAINDER) # catch rest (non-options) as args
    opts = parser.parse_args()
    return opts

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

    if len(opts.args) < 1:
        print "Need command"
        sys.exit(1)

    if opts.zk_hosts != None:
        gdata["conf_data"]["zk_hosts"] = opts.zk_hosts
    else:
        opts.zk_hosts = gdata["conf_data"]["zk_hosts"]

    cmd = opts.args[0]
    command_args = opts.args[1:]

    cmds = gdata['cmds']
    if cmds.has_key(cmd):
        start_zk_client(opts)
        command_data = {
                'conf_data' : gdata['conf_data'],
                'zkdetails' : {'zkroot': gdata['conf_data']['zkroot'], 'zk_client': gdata['zk_client']},
        }
        cmds[cmd](command_data, command_args)
        stop_zk_client()
    else:
        print "Invalid command[{}]".format(cmd)
        sys.exit(1)

if __name__ == '__main__':
    main()

