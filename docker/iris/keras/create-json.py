import sys
import json

for line in sys.stdin:
    line = line.rstrip()
    try:
        (f1,f2,f3,f4,cl) = line.split(',')
        d = {}
        d["f1"] = float(f1)
        d["f2"] = float(f2)
        d["f3"] = float(f3)
        d["f4"] = float(f4)
        d["name"] = cl
        j = json.dumps(d,sort_keys=True)
        print j
    except:
        continue
