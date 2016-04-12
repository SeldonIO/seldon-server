make push_to_dockerhub IMAGE_FOLDER=mysql
make push_to_dockerhub IMAGE_FOLDER=seldon-server
make push_to_dockerhub IMAGE_FOLDER=td-agent
make push_to_dockerhub IMAGE_FOLDER=td-agent-node
make push_to_dockerhub IMAGE_FOLDER=td-agent-server
make push_to_dockerhub IMAGE_FOLDER=seldon-spark-build
make push_to_dockerhub IMAGE_FOLDER=examples/reuters/data
pushd examples/reuters/microservice; make push_to_dockerhub; popd
pushd seldon-control; make push_to_dockerhub; popd
pushd examples/iris/xgboost; make push_to_dockerhub; popd
pushd examples/iris/vw; make push_to_dockerhub; popd
pushd examples/iris/keras; make push_to_dockerhub; popd
make push_to_dockerhub IMAGE_FOLDER=examples/ml100k
