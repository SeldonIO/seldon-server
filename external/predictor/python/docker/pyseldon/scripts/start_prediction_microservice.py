import sys, getopt, argparse
from seldon.microservice import Microservices

if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog='microservice')
    parser.add_argument('--model_name', help='name of model', required=True)
    parser.add_argument('--pipeline', help='location of prediction pipeline', required=True)
    parser.add_argument('--aws_key', help='aws key', required=False)
    parser.add_argument('--aws_secret', help='aws secret', required=False)

    args = parser.parse_args()
    opts = vars(args)

    m = Microservices(aws_key=args.aws_key,aws_secret=args.aws_secret)
    app = m.create_prediction_microservice(args.pipeline,args.model_name)
    app.run(host="0.0.0.0", debug=True)


