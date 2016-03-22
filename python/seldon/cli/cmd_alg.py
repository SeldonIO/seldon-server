import pprint
import argparse

def pp(o):
    p = pprint.PrettyPrinter(indent=4)
    p.pprint(o)

def getOpts(args):
    parser = argparse.ArgumentParser(prog='seldon-cli client', description='Seldon Cli')
    parser.add_argument('--action', help="the action to use", required=True)
    parser.add_argument('--client-name', help="the name of the client", required=False)
    parser.add_argument('args', nargs=argparse.REMAINDER) # catch rest (non-options) as args
    opts = parser.parse_args(args)
    return opts

def action_list(command_data, opts):
    print "Default recommenders:"
    default_algorithms = command_data["conf_data"]["default_algorithms"]
    for recommender in default_algorithms:
        print "    {recommender}".format(**locals())

def cmd_alg(command_data, command_args):
    actions = {
        "list" : action_list,
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
