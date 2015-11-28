
def init(config):
    # do any initialisation needed here
    print "initialised"

def get_predictions(client,json):
    # take json, convert to format needed and return list of 3-tuples of (score,classId,confidence) along with model name
    return ([(1.0,1,0.9)],"example_model")




