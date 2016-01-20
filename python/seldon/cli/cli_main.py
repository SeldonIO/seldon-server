import sys
import argparse
import pprint

import cmd_memcached

cmds = {
    "memcached" : cmd_memcached.cmd_memcached,
}

def pp(o):
    p = pprint.PrettyPrinter(indent=4)
    p.pprint(o)

def getOpts():
    parser = argparse.ArgumentParser(prog='seldon-cli', description='Seldon Cli')
    parser.add_argument('--version', action='store_true', help="print the version", required=False)
    parser.add_argument('args', nargs=argparse.REMAINDER) # catch rest (non-options) as args
    opts = parser.parse_args()
    return opts

def main():
    opts = getOpts()
    if opts.version == True:
        from seldon import __version__
        print __version__
        sys.exit(0)

    if len(opts.args) < 1:
        print "Need command"
        sys.exit(1)

    cmd = opts.args[0]
    command_data = {}
    command_args = opts.args[1:]

    if cmds.has_key(cmd):
        cmds[cmd](command_data, command_args)
    else:
        print "Invalid command[{}]".format(cmd)
        sys.exit(1)

if __name__ == '__main__':
    main()

