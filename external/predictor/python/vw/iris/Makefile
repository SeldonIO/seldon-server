SHELL=/bin/bash

iris.data:
	wget --quiet https://archive.ics.uci.edu/ml/machine-learning-databases/iris/iris.data

train:iris.data
	cat iris.data | python create-vw.py | shuf > train

model:train
	rm -f cache.file
	vw --data train -f model --cache_file ./cache.file --passes 3 --oaa 3 

daemon:model
	vw -i model  -t --daemon --quiet --port 26542 -r raw_predictions.txt
	touch daemon

kill-daemon:
	pkill -9 -f 'vw.*--port 26542'
	rm -f daemon

test-daemon:
	echo "1  |f  f1:4.8 f2:3.0 f3:1.4 f4:0.1" | netcat localhost 26542
	echo "2  |f  f1:5.8 f2:2.6 f3:4.0 f4:1.2" | netcat localhost 26542

clean:
	rm -f train model raw_predictions.txt iris.data cache.file
