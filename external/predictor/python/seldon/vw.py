import sys
from fileutil import *
from kazoo.client import KazooClient
import json
from wabbit_wappa import *
from subprocess import call
import seldon.pipeline.pipelines as pl

class VWSeldon:
    """Wrapper to train a Vowpal Wabbit model for Seldon data

    """

    def __init__(self, client=None,awsKey=None,awsSecret=None,zkHosts=None,vwArgs="",features={}, namespaces={},include=[],exclude=None,target=None,weights=None,target_readable=None,dataType="json",explicitPaths=None,inputPath=None,day=1,outputPath=None,activate=False,train_filename=None,**kwds):
        """Seldon VW Wrapper

        Args:
            client (str): Name of client. Used to construct input and output full paths if not explicitPaths
            awsKey (Optional[str]): AWS key. Needed if using S3 paths and no IAM
            awsSecret (Optional[str]): AWS secret. Needed if using S3 paths and no IAM
            zkHosts (Optional[str]): zookeeper hosts. Used to get configuation if supplied.
            vwArgs (Optional[str]): training args for vowpal wabbit
            features (Optional[dict]): dictionary of features and how to translate them to VW
            namespaces (Optional[dict]): dictionary of features to the VW namespace to place them
            include (Optional[list(str)]: features to include
            exclude (Optional[list(str)]: features to exclude
            target (Optional[str]): name of target feature.
            target_readable (Optional[str]): name of the feature containing human readable target
            dataTtpe (str): json or csv
            explicitPaths (Optional[str]): whether to use the input and output paths as given
            inputPath (Optional[str]): input path for features to train
            outputPath (Optional[str]): output path for vw model
            day (int): unix day number. Used to construct location of data
            activate (boolean): whether to update zookeeper with location of new model
            train_filename (Optional(str)): name of filename to create with vw training lines rather then use wabbit_wappa directly
        """
        self.client = client
        self.awsKey = awsKey
        self.awsSecret = awsSecret
        self.zk_hosts = zkHosts
        if self.zk_hosts:
            print "connecting to zookeeper at ",self.zk_hosts
            self.zk_client = KazooClient(hosts=self.zk_hosts)
            self.zk_client.start()
        else:
            self.zk_client = None
        self.__merge_conf(self.client)
        self.vwArgs = vwArgs
        self.create_vw(vwArgs)
        self.features = features
        self.fns = namespaces
        self.include = include
        self.target = target
        if target:
            self.features[target] = "label"
        self.exclude = exclude
        self.weights = weights
        self.target_readable = target_readable
        self.data_type = dataType
        self.explicit_paths = explicitPaths
        self.inputPath = inputPath
        self.outputPath = outputPath
        self.day = day
        self.activate = activate
        self.train_filename = train_filename

        
    def activateModel(self,client,folder):
        """activate vw model in zookeeper
        """
        node = "/all_clients/"+client+"/vw"
        print "Activating model in zookeper at node ",node," with data ",folder
        if self.zk_client.exists(node):
            self.zk_client.set(node,folder)
        else:
            self.zk_client.create(node,folder,makepath=True)


    def __merge_conf(self,client):
        """merge zookeeper configuration into class
        """
        thePath = "/all_clients/"+client+"/offline/vw"
        if self.zk_client and self.zk_client.exists(thePath):
            print "merging conf from zookeeper"
            data, stat = self.zk_client.get(thePath)
            zk_conf = json.loads(data.decode('utf-8'))
            for k in zk_conf:
                if not hasattr(self, k):
                    setattr(self, k, zk_conf[k])

    @staticmethod
    def is_number(s):
        try:
            float(s)
            return True
        except ValueError:
            return False


    def convertJsonFeature(self,conversion,ns,name,val):
        """convert feature into vw feature
           
           if number create a feature:value vw feature else create a categorical feature from the feature name and value
        """
        if conversion == "split":
            f = val.split()
            ns = ns + f
        elif isinstance(val, basestring):
            if self.is_number(val):
                ns.append((name,float(val)))
            else:
                ns.append(name+"_"+val)
        else:
            ns.append((name,float(val)))

    def jsonToVw(self,j,tag=None):
        """convert a dict of features into vw line
        
        Will include and exclude features as defined args. 
        Add weights and puts features into namespaces as directed.
        """
        ns = {}
        for k in set(self.fns.values()):
            if not k == "label":
                ns[k] = []
        ns['def'] = []
        label = None
        importance = 1.0
        for k in j:
            if self.exclude and k in self.exclude:
                continue
            if self.include and not k in self.include and not k in self.features:
                continue
            if not k in self.fns:
                self.fns[k] = 'def'
            ns_f = ns[self.fns[k]]
            if k in self.features:
                conversion = self.features[k] 
                if conversion == "split":
                    self.convertJsonFeature(conversion,ns_f,k,j[k])                    
                elif conversion == "label":
                    label = int(j[k])
                    if self.weights and j[k] in self.weights:
                        importance = self.weights[j[k]]
            else:
                if isinstance(j[k], list):
                    for v in j[k]:
                        if isinstance(v,basestring):
                            self.convertJsonFeature(None,ns_f,k,v)
                        else:
                            self.convertJsonFeature(None,ns_f,k,k+str(v))
                elif isinstance(j[k], dict):
                    for k2 in j[k]:
                        self.convertJsonFeature(None,ns_f,k2,j[k][k2])
                else:
                    self.convertJsonFeature(None,ns_f,k,j[k])
        namespaces = []
        for k in ns:
            if not k == 'def':
                namespaces.append(Namespace(name=k,features=ns[k]))
        if len(ns['def']) == 0 and len(ns) == 1:
            return None
        if len(ns['def']) == 0:
            ns['def'] = None
        print "label ",label," features ",ns['def']
        if self.weights:
            return self.vw2.make_line(response=label,importance=importance,tag=tag,features=ns['def'],namespaces=namespaces)
        else:
            return self.vw2.make_line(response=label,tag=tag,features=ns['def'],namespaces=namespaces)
        
    def create_vw(self,vwArgs):
        """Create wabbit_wappa vw instance for creating the training lines and possibly doing the training
        """
        command = "vw --save_resume --predictions /dev/stdout --quiet "+vwArgs + " --readable_model ./model.readable"
        print command
        self.vw2 =  VW(command=command)
        print self.vw2.command

    def process(self,j):
        """process the lines from the input and turn into vw lines
        """
        vwLine = self.jsonToVw(j)
        self.numLinesProcessed += 1
        if vwLine:
            if self.target_readable:
                self.target_map[j[self.target]] = j[self.target_readable]
            if self.train_file:
                self.train_file.write(vwLine+"\n")
            else:
                self.vw2.send_line(vwLine)

                
    def save_data(self,line):
        """save data to local file
        """
        self.local_input.write(line+"\n")

        
    def save_target_map(self):
        """save mapping of target ids to their descriptive meanings
        """
        v = json.dumps(self.target_map,sort_keys=True)
        f = open('./target_map.json',"w")
        f.write(v)
        f.close()


    def train(self,train_filename=None,vw_command=None):
        """train a vw model from input and save to output
        """
        self.numLinesProcessed = 0
        self.target_map = {}
        if train_filename:
            self.train_file = open(train_filename,"w")
        else:
            self.train_file = None
        #stream data into vw
        if self.explicit_paths:
            inputPath = self.inputPath
        else:
            inputPath = self.inputPath + "/" + self.client + "/features/" + str(self.day) + "/"
        print "inputPath->",inputPath
        
        # stream data to local file
        fileUtil = FileUtil(key=self.awsKey,secret=self.awsSecret)
        self.local_input = open("./data","w")
        fileUtil.stream(inputPath,self.save_data)
        self.local_input.close()

        #build vw training 
        if self.data_type == "csv":
            features = pl.CsvDataSet("./data")
        else:
            features = pl.JsonDataSet("./data")
        for d in features:
            self.process(d)

        # save vw model
        if train_filename:
            self.vw2.close()
            self.train_file.close()
            r = call(["vw","--data",train_filename,"-f","model","--cache_file","./cache_file","--readable_model","./model.readable"]+self.vwArgs.split())
            print "called and got ",r
        else:
            self.vw2.save_model("./model")
            self.vw2.close()
            print "lines processed ",self.numLinesProcessed

        self.save_target_map()
        # copy models to final location
        if self.explicit_paths:
            outputPath = self.outputPath
        else:
            outputPath = self.outputPath + "/" + self.client + "/vw/" + str(self.day)
        print "outputPath->",outputPath
        fileUtil.copy("./model",outputPath+"/model")
        fileUtil.copy("./model.readable",outputPath+"/model.readable")
        fileUtil.copy("./target_map.json",outputPath+"/target_map.json")

        #activate model in zookeeper
        if self.activate:
            self.activateModel(self.client,str(outputPath))
