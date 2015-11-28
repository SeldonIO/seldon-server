from flask import Flask
from seldon.microservice.predict import predict_blueprint
from seldon.microservice.recommend import recommend_blueprint
import seldon
from sklearn.pipeline import Pipeline
import seldon.pipeline.util as sutl
import random
import pylibmc

class Microservices(object):

    def __init__(self,aws_key=None,aws_secret=None):
        self.aws_key = aws_key
        self.aws_secret = aws_secret

    def create_prediction_microservice(self,pipeline_folder,model_name):
        app = Flask(__name__)
                   
        rint = random.randint(1,999999)
        pw = sutl.Pipeline_wrapper(work_folder='/tmp/pl_'+str(rint),aws_key=self.aws_key,aws_secret=self.aws_secret)
        pipeline = pw.load_pipeline(pipeline_folder)
        
        app.config["seldon_pipeline_wrapper"] = pw
        app.config["seldon_pipeline"] = pipeline
        app.config["seldon_model_name"] = model_name
 
        app.register_blueprint(predict_blueprint)

        # other setup tasks
        return app

    def create_recommendation_microservice(self,recommender_folder,memcache_servers=None,memcache_pool_size=2):
        app = Flask(__name__)

        if not memcache_servers is None:
            mc = pylibmc.Client(memcache_servers)
            _mc_pool = pylibmc.ClientPool(mc, memcache_pool_size)
            app.config["seldon_memcache"] = _mc_pool
            
        rw = seldon.Recommender_wrapper()
        recommender = rw.load_recommender(recommender_folder)
        app.config["seldon_recommender"] = recommender
 
        app.register_blueprint(recommend_blueprint)

        # other setup tasks
        return app



