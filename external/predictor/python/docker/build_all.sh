# order matters as there are dependencies in Dockerfiles
pushd pipelines ; make build ; popd
pushd vw_train ; make build ; popd
pushd vw_runtime ; make build ; popd
pushd xgboost_train ; make build ; popd
pushd xgboost_runtime ; make build ; popd
