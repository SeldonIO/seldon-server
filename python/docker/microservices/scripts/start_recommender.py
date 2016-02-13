import sys, getopt, argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='upload model')
    parser.add_argument('--src', help='arpa file', required=True)
    parser.add_argument('--aws_key', help='aws key - needed if input or output is on AWS and no IAM')
    parser.add_argument('--aws_secret', help='aws secret - needed if input or output on AWS  and no IAM')

    args = parser.parse_args()
    opts = vars(args)

    from seldon.microservice import Microservices

    if "aws_key" in opts:
        m = Microservices(aws_key=args.aws_key,aws_secret=args.aws_secret)
    else:
        m = Microservices()
    app = m.create_recommendation_microservice(args.src)
    app.run(host="0.0.0.0",port=5000,debug=True)
