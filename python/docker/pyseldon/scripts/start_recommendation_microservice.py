import sys, getopt, argparse
from seldon.microservice import Microservices

if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog='microservice')
    parser.add_argument('--recommender', help='location of recommender', required=True)
    parser.add_argument('--aws_key', help='aws key', required=False)
    parser.add_argument('--aws_secret', help='aws secret', required=False)
    parser.add_argument('--memcache_servers', help='memcache servers', required=False)
    parser.add_argument('--memcache_pool_size', help='memcache servers pool size', required=False, default=2, type=int)

    args = parser.parse_args()
    opts = vars(args)

    m = Microservices(aws_key=args.aws_key,aws_secret=args.aws_secret)
    app = m.create_recommendation_microservice(args.recommender,memcache_servers=args.memcache_servers,memcache_pool_size=args.memcache_pool_size):)
    app.run(host="0.0.0.0", debug=True)


