# Cluster Integration Test

Note: Currently only Linux x64 is supported. MacOS with Apple Silicon may not work.

## Run tests

```shell
./run.sh
```

## Update t and r

If you have changed `t` and want to produce a new `r`:

```shell
./record.sh
```

## Base Image

The base image `Dockerfile.base` contains some resources that we would like to cache and avoid download again and again, exposed as `hub.pingcap.net/keyspace/tidb-cse-cluster-integration-test-base:latest`.

If you changed `Dockerfile.base`, rebuild and push the image:

```shell
docker build -t hub.pingcap.net/keyspace/tidb-cse-cluster-integration-test-base:latest -f Dockerfile.base .
docker push hub.pingcap.net/keyspace/tidb-cse-cluster-integration-test-base:latest
```