pushd pipelines ; make push_to_dockerhub ; popd
pushd vw_train ; make push_to_dockerhub ; popd
pushd vw_runtime ; make push_to_dockerhub ; popd
pushd xgboost_train ; make push_to_dockerhub ; popd
pushd xgboost_runtime ; make push_to_dockerhub ; popd
