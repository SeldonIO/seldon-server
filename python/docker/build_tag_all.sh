# order matters as there are dependencies in Dockerfiles
pushd pyseldon ; make build ; make tag ; popd
pushd examples/iris ; make build_pipeline ; make tag ; popd
pushd ngram ; make build ; make tag ; popd
