# Seldon Core

Seldon Core is a machine learning platform that helps your data science team deploy models into production.

It provides an open-source data science stack that runs within a [Kubernetes](http://kubernetes.io/) Cluster. You can use Seldon to deploy machine learning and deep learning models into production on-premise or in the cloud (e.g. [GCP](http://docs.seldon.io/kubernetes-google-cloud.html), AWS, Azure).

Seldon supports models built with TensorFlow, Keras, Vowpal Wabbit, XGBoost, Gensim and any other model-building tool  — it even supports models built with commercial tools and services where the model is exportable.

It includes an API with two key endpoints:

1.  **[Predict](http://docs.seldon.io/prediction-guide.html)** - Build and deploy supervised machine learning models created in any machine learning library or framework at scale using containers and [microservices](http://docs.seldon.io/api-microservices.html).
2.  **[Recommend](http://docs.seldon.io/content-recommendation-guide.html)** - High-performance user activity and content based recommendation engine with various algorithms ready to run out of the box. 

Other features include:

- Complex dynamic [algorithm configuration and combination](http://docs.seldon.io/advanced-recommender-config.html) with no downtime: run A/B and Multivariate tests, cascade algorithms and create ensembles.
- Command Line Interface ([CLI](http://docs.seldon.io/seldon-cli.html)) for configuring and managing Seldon Core.
- Secure OAuth 2.0 REST and [gRPC](http://docs.seldon.io/grpc.html) APIs to streamline integration with your data and application.
- Grafana dashboard for [real-time analytics](http://docs.seldon.io/analytics.html) built with Kafka Streams, Fluentd and InfluxDB.

Seldon is used by some of the world's most innovative organisations — it's the perfect machine learning deployment platform for start-ups and can scale to meet the demands of large enterprises.

## Get Started

It takes a few minutes to install Seldon on a Kubernertes cluster. Visit our [install guide](http://docs.seldon.io/install.html) and read our [tech docs](http://docs.seldon.io).

## Community & Support

* Join the [Seldon Users Group](https://groups.google.com/forum/#!forum/seldon-users).
* [Register for our newsletter](http://eepurl.com/6X6n1) to be the first to receive updates about our products and events.
* Visit [our website](https://www.seldon.io/), follow [@seldon_io](https://twitter.com/seldon_io) on Twitter and like [our Facebook page](https://www.facebook.com/seldonhq/).
* If you're in London, meet us at [TensorFlow London](https://www.meetup.com/TensorFlow-London/) - a community of over 1200 data scientists that we co-organise.
* We also offer [commercial support plans and managed services](https://www.seldon.io/enterprise/).

## License
Seldon is available under [Apache Licence, Version 2.0](https://github.com/SeldonIO/seldon-server/blob/master/README.md)
