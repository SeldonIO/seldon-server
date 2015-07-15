import sys
from fileutil import *
from kazoo.client import KazooClient
import json
from wabbit_wappa import *

'''
get below from zookeeper

rules:
namespace feature type

namespace : default or name
feature : name of feature
transform : 
   split
   idx_val

{"rules":{"f1":"|split","f2":"ns1|"}}

otherwise will treat as string or float as given
   
'''
class JsonToVW:

    def transform(self,json,rules):
        for k in json:
            print k,json[k]

class VWSeldon:

    def __init__(self, zk_hosts = None, awsKey = None, awsSecret = None):
        self.awsKey = awsKey
        self.awsSecret = awsSecret
        if zk_hosts:
            print "connecting to zookeeper at ",zk_hosts
            self.zk_client = KazooClient(hosts=zk_hosts)
            self.zk_client.start()
        else:
            self.zk_client = None

    def activateModel(self,client,folder):
        node = "/all_clients/"+client+"/vw"
        print "Activating model in zookeper at node ",node," with data ",folder
        if self.zk_client.exists(node):
            self.zk_client.set(node,folder)
        else:
            self.zk_client.create(node,folder,makepath=True)


    def __merge_conf(self,client,conf):
        thePath = "/all_clients/"+client+"/offline/vw"
        if self.zk_client and self.zk_client.exists(thePath):
            print "merging conf from zookeeper"
            data, stat = self.zk_client.get(thePath)
            zk_conf = json.loads(data.decode('utf-8'))
            zk_conf.update(conf)
            return zk_conf
        else:
            return conf

    def jsonToVw(self,j):
        ns = {}
        if self.fns:
            for k in set(self.fns.values()):
                if not k == "label":
                    ns[k] = []
            fs = None
        else:
            fs = []
        label = None
        for k in j:
            if k in self.features:
                conversion = self.features[k] 
                if conversion == "split":
                    f = j[k].split()
                    if k in self.fns:
                        ns[self.fns[k]] = ns[self.fns[k]] + f
                    else:
                        fs = fs + f
                elif conversion == "label":
                    label = float(j[k])
            else:
                if isinstance(j[k], basestring):
                    if k in self.fns:
                        ns[self.fns[k]].append(k+"_"+j[k])
                    else:
                        fs.append(k+"_"+j[k])
                else:
                    if k in self.fns:
                        ns[self.fns[k]].append((k,float(j[k])))
                    else:
                        fs.append((k,float(j[k])))
        namespaces = []
        for k in ns:
            namespaces.append(Namespace(name=k,features=ns[k]))
        return self.vw2.make_line(response=label,features=fs,namespaces=namespaces)
        
    def create_vw(self,conf):
        command = "vw --save_resume --predictions /dev/stdout --quiet "+conf['vwArgs'] + " --readable_model ./model.readable"
        self.vw2 =  VW(command=command)
        print self.vw2.command

    def process(self,line):
        j = json.loads(line)
        vwLine = self.jsonToVw(j)
        self.numLinesProcessed += 1
        self.vw2.send_line(vwLine)
                  
    def train(self,client,conf):
        self.numLinesProcessed = 0
        print "command line conf ",conf
        conf = self.__merge_conf(client,conf)
        print "conf after zookeeper merge ",conf
        self.create_vw(conf)
        self.features = conf.get('features',{})
        self.fns = conf.get('namespaces',{})
        inputPath = conf["inputPath"] + "/" + client + "/features/" + str(conf['day']) + "/"
        print "inputPath->",inputPath
        if inputPath.startswith("s3n://"):
            isS3 = True
            inputPath = inputPath[6:]
        elif inputPath.startswith("s3://"):
            isS3 = True
            inputPath = inputPath[5:]
        else:
            isS3 = False
        if isS3:
            fileUtil = S3FileUtil(self.awsKey,self.awsSecret)
            print "AWS S3 input path ",inputPath
            parts = inputPath.split('/')
            bucket = parts[0]
            prefix = inputPath[len(bucket)+1:]
            fileUtil.stream(bucket,prefix,self)
        else:
            fileUtil = LocalFileUtil() 
            folders = [inputPath+"*"]
            print "local input folders: ",folders
            fileUtil.stream(folders,self)
        self.vw2.save_model("./model")
        self.vw2.close()
        print "lines processed ",self.numLinesProcessed
        # push model to output path on s3 or local
        outputPath = conf["outputPath"] + "/" + client + "/vw/" + str(conf["day"])
        print "outputPath->",outputPath
        if outputPath.startswith("s3n://"):
            isS3 = True
        else:
            isS3 = False
        if isS3:
            noSchemePath = outputPath[6:]
            parts = noSchemePath.split('/')
            bucket = parts[0]
            path = noSchemePath[len(bucket)+1:]
            fileUtil = S3FileUtil(self.awsKey,self.awsSecret)
            fileUtil.copy("./model",bucket,path+"/model")
            fileUtil.copy("./model.readable",bucket,path+"/model.readable")
        else:
            fileUtil = LocalFileUtil() 
            fileUtil.copy("./model",outputPath+"/model")
            fileUtil.copy("./model.readable",outputPath+"/model.readable")
        if "activate" in conf and conf["activate"]:
            self.activateModel(client,str(outputPath))
