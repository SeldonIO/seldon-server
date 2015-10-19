# order matters as there are dependencies in Dockerfiles
pushd pipelines ; make build ; make tag ; popd
pushd vw_train ; make build ; make tag ; popd
pushd vw_runtime ; make build ; make tag ; popd
pushd xgboost_train ; make build ; make tag ; popd
pushd xgboost_runtime ; make build ; make tag ; popd
pushd examples/iris ; make build ; make tag ; popd

