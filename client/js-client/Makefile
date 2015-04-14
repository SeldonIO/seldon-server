SHELL:=/bin/bash
all: libs build build/seldon_lib.min.js build/seldon_lib.js

libs:
	rm -rf libs
	mkdir -p libs
	wget http://dl.google.com/closure-compiler/compiler-latest.zip 
	unzip compiler-latest.zip compiler.jar -d libs
	rm compiler-latest.zip
	wget https://github.com/ender-js/Ender/archive/v1.0.5.zip
	unzip -j v1.0.5.zip Ender-1.0.5/ender.js -d libs
	rm v1.0.5.zip
	wget -P libs https://raw.githubusercontent.com/ternarylabs/porthole/master/example/abc.com/js/porthole.min.js 
build: 
	mkdir -p build

build/version:
	echo "// built against version $(shell git rev-parse HEAD)" > build/version

build/ender-app.js: rlClient.js rlSession.js libs build
	ender compile --externs rlClient.js rlSession.js --use libs/ender.js
	mv ender-app.js build/

build/seldon_lib.tmp.js: build/ender-app.js 
	cat build/ender-app.js rlClient.js libs/porthole.min.js rlSession.js > build/seldon_lib.tmp.js

build/seldon_lib.js: build/version build/seldon_lib.tmp.js rlHeader.js rlFooter.js
	cat build/version rlHeader.js build/seldon_lib.tmp.js rlFooter.js > build/seldon_lib.js

build/seldon_lib.min.js: build/version build/seldon_lib.tmp.js rlHeader.js rlFooter.js
	cat build/version rlHeader.js <(java -jar libs/compiler.jar build/seldon_lib.tmp.js) rlFooter.js > build/seldon_lib.min.js

clean:
	rm -rf build
	rm -rf libs

