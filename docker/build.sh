make build_image IMAGE_FOLDER=mysql
make build_image IMAGE_FOLDER=seldon-server
make build_image IMAGE_FOLDER=td-agent
make build_image IMAGE_FOLDER=td-agent-node
make build_image IMAGE_FOLDER=td-agent-server
make build_image IMAGE_FOLDER=seldon-spark-build
make build_image IMAGE_FOLDER=examples/reuters/data
pushd examples/reuters/microservice; make build_image; podp
pushd seldon-control; make build_image; popd
pushd examples/iris/xgboost; make build_image; popd
pushd examples/iris/vw; make build_image; popd
pushd examples/iris/keras; make build_image; popd
make build_image IMAGE_FOLDER=examples/ml100k
