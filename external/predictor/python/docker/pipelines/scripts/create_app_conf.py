import sys, getopt, argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog='microservice')
    parser.add_argument('--model_name', help='name of model', required=True)
    parser.add_argument('--pipeline', help='location of prediction pipeline', required=True)
    parser.add_argument('--aws_key', help='aws key', required=False)
    parser.add_argument('--aws_secret', help='aws secret', required=False)

    args = parser.parse_args()
    opts = vars(args)

    f = open('server_config.py',"w")
    f.write("PIPELINE='"+args.pipeline+"'\n")
    f.write("MODEL='"+args.model_name+"'\n")
    if not args.aws_key is None:
        f.write("AWS_KEY='"+args.aws_key+"'\n")
        f.write("AWS_SECRET='"+args.aws_secret+"'\n")
    f.close()

