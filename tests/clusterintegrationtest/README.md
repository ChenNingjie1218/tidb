# Cluster Integration Test

Currently only Linux x64 is supported. Find yourself a Linux x64 machine first.

## Guide: Run tests

```shell
# cd clusterintegrationtest
./run1.sh  # mysql-tester test
./run2.sh  # vector recall test
```

## Guide: Update `r`

After changing `t` or changing optimizer plans, `r` need to be updated.

1. Start an empty CSE cluster and expose TiDB as :4000

    ```shell
    # cd clusterintegrationtest
    ./cluster.sh
    ```

    Note: You may need to wait about 30s for TiFlash to be ready.

2. Run following commands

    ```shell
    # cd clusterintegrationtest
    GOBIN=$(realpath .)/gobin go install github.com/pingcap/mysql-tester/src@314107b26aa8fce86beb0dd48e75827fb269b365
    ./gobin/src -retry-connection-count 5 -record
    ```

## Guide: Develop python_testers

1. Prepare local environment

    ```shell
    # cd clusterintegrationtest
    python3 -m venv .venv
    source .venv/bin/activate
    pip3 install -r requirements.txt
    ```

2. Download datasets

    ```shell
    # cd clusterintegrationtest
    cd datasets
    wget https://ann-benchmarks.com/fashion-mnist-784-euclidean.hdf5
    cd ..
    ```

3. Start a CSE cluster and expose TiDB as :4000

    ```shell
    # cd clusterintegrationtest
    ./cluster.sh
    ```

    Note: You may need to wait about 30s for TiFlash to be ready.

4. Run, edit and debug tests

    ```shell
    # cd clusterintegrationtest
    source .venv/bin/activate
    python3 python_testers/vector_recall.py
    ```

5. Optional: Update `Dockerfile.base` if needed

    The base image `Dockerfile.base` contains resources that we would like to cache and avoid download repeatedly for each CI run, exposed as `hub.pingcap.net/keyspace/tidb-cse-cluster-integration-test-base:latest`.

    If you changed files below, remember rebuild and push the image:

    - `Dockerfile.base`
    - `requirements.txt`
    - `datasets/*`

    First, make sure you have datasets downloaded in `datasets/` directory (see step 2). Then run following commands:

    ```shell
    docker build -t hub.pingcap.net/keyspace/tidb-cse-cluster-integration-test-base:latest -f Dockerfile.base .
    ```

    This will build the base image and you can use it locally. The new base image is not yet available in CI. Continue following steps below.

6. Optional: Verify your updated tests or updated `Dockerfile.base`

    If you have updated the test case or `Dockerfile.base`, before committing to the repository, you could verify whether your change will work in CI as well:

    ```shell
    # cd clusterintegrationtest
    ./run1.sh
    ./run2.sh
    ```

7. Optional: Publish the updated `Dockerfile.base` to CI

    Contact @breezewish to perform this change:

    ```shell
    docker push hub.pingcap.net/keyspace/tidb-cse-cluster-integration-test-base:latest
    ```