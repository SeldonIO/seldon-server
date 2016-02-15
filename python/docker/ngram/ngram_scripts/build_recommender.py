import sys, getopt, argparse
from  seldon.text.ngram_recommend import NgramModel

if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='upload model')
    parser.add_argument('--arpa', help='arpa file', required=True)
    parser.add_argument('--dst', help='dst folder', required=True)
    parser.add_argument('--aws_key', help='aws key - needed if input or output is on AWS and no IAM')
    parser.add_argument('--aws_secret', help='aws secret - needed if input or output on AWS  and no IAM')

    args = parser.parse_args()
    opts = vars(args)

    recommender = NgramModel()
    recommender.fit(args.arpa)

    import seldon
    if "aws_key" in opts:
        rw = seldon.Recommender_wrapper(aws_key=args.aws_key,aws_secret=args.aws_secret)
    else:
        rw = seldon.Recommender_wrapper()
    rw.save_recommender(recommender,args.dst)




