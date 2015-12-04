gdata = {
    'help_cmd_strs_list' : [
        ("import", "todo"),
        ("import items <clientName> </path/to/items.csv>", "import items for client"),
        ("import users <clientName> </path/to/users.csv>", "import users for client"),
        ("import actions <clientName> </path/to/actions.csv>", "import actions for client")
    ],
}

def subcmd_default(command_data):
    print "todo default!"

def subcmd_help(command_data):
    lmargin_size=command_data["help_formatting"]["lmargin_size"]
    cmd_size=command_data["help_formatting"]["cmd_size"]+10
    lmargin_pad=" "
    for help_strs in gdata["help_cmd_strs_list"]:
        cmd_str = help_strs[0]
        cmd_help = help_strs[1]
        print "{lmargin_pad:<{lmargin_size}}{cmd_str:<{cmd_size}} - {cmd_help}".format(**locals())

def cmd_import(args, command_data):
    if args == "":
        subcmd_default(command_data)
    else:
        subcmd = args.split()[0]
        subcmd_args = args.split()[1:]
        command_data["subcmd"] = subcmd
        command_data["subcmd_args"] = subcmd_args
        if subcmds.has_key(subcmd):
            subcmds[subcmd](command_data)
        else:
            print "unkown subcmd[%s]" % subcmd

subcmds = {
    "help" : subcmd_help,
}

