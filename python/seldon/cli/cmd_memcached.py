import pprint
import argparse

def pp(o):
    p = pprint.PrettyPrinter(indent=4)
    p.pprint(o)

def getOpts(args):
    parser = argparse.ArgumentParser(prog='seldon-cli memcached', description='Seldon Cli')
    parser.add_argument('--action', help="the action to use", required=False)
    parser.add_argument('args', nargs=argparse.REMAINDER) # catch rest (non-options) as args
    opts = parser.parse_args(args)
    return opts

def action_setup(command_data, opts):
    print "Doing action setup"
    pp(command_data)
    pp(opts)

def action_commit(command_data, opts):
    print "Doing action commit"
    pp(command_data)
    pp(opts)

def action_default(command_data, opts):
    print "Doing action default"
    pp(command_data)
    pp(opts)

def cmd_memcached(command_data, command_args):
    actions = {
        "default" : action_default,
        "setup" : action_setup,
        "commit" : action_commit,
    }

    opts = getOpts(command_args)

    action = opts.action
    if action == None:
        actions["default"](command_data, opts)
    else:
        if actions.has_key(action):
            actions[action](command_data, opts)
        else:
            print "Invalid action[{}]".format(action)

