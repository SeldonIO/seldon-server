# order matters as there are dependencies in Dockerfiles
pushd pipelines ; make build ; make tag ; popd
pushd examples/iris ; make build_pipeline ; make tag ; popd

