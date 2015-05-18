import sys

for line in sys.stdin:
    line = line.rstrip()
    try:
        (f1,f2,f3,f4,cl) = line.split(',')
        if cl == "Iris-setosa":
            print "1 ",
        elif cl == "Iris-versicolor":
            print "2 ",
        else:
            print "3 ",
        print "|f",
        print " f1:"+f1+" f2:"+f2+" f3:"+f3+" f4:"+f4
    except:
        continue
