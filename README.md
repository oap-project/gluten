# Apache Gluten (Incubating): A Middle Layer for Offloading JVM-based SQL Engines' Execution to Native Engines

[![OpenSSF Best Practices](https://www.bestpractices.dev/projects/8452/badge)](https://www.bestpractices.dev/projects/8452)

*<b>This project is still under active development now, and doesn't have a stable release. Welcome to evaluate it.</b>*

# 1 Introduction

## 1.1 Problem Statement

Apache Spark is a stable, mature project that has been developed for many years. It is one of the best frameworks to scale out for processing petabyte-scale datasets. However, the Spark community has had to address performance challenges that require various optimizations over time. As a key optimization in Spark 2.0, Whole Stage Code Generation is introduced to replace Volcano Model, which achieves 2x speedup. Henceforth, most optimizations are at query plan level. Single operator's performance almost stops growing.

<p align="center">
<img src="https://user-images.githubusercontent.com/47296334/199853029-b6d0ea19-f8e4-4f62-9562-2838f7f159a7.png" width="800">
</p>

On the other side, SQL engines have been researched for many years. There are a few libraries like Clickhouse, Arrow and Velox, etc. By using features like native implementation, columnar data format and vectorized data processing, these libraries can outperform Spark's JVM based SQL engine. However, these libraries only support single node execution.

## 1.2 Gluten's Solution

“Gluten” is Latin for glue. The main goal of Gluten project is to “glue" native libraries with SparkSQL. Thus, we can benefit from high scalability of Spark SQL framework and high performance of native libraries.

The basic rule of Gluten's design is that we would reuse spark's whole control flow and as many JVM code as possible but offload the compute-intensive data processing part to native code. Here is what Gluten does:
* Transform Spark’s whole stage physical plan to Substrait plan and send to native
* Offload performance-critical data processing to native library
* Define clear JNI interfaces for native libraries
* Switch available native backends easily
* Reuse Spark’s distributed control flow
* Manage data sharing between JVM and native
* Extensible to support more native accelerators

## 1.3 Target User

Gluten's target user is anyone who wants to accelerate SparkSQL fundamentally. As a plugin to Spark, Gluten doesn't require any change for dataframe API or SQL query, but only requires user to make correct configuration.
See Gluten configuration properties [here](https://github.com/apache/incubator-gluten/blob/main/docs/Configuration.md).

## 1.4 References

You can click below links for more related information.
- [Gluten Intro Video at Data AI Summit 2022](https://www.youtube.com/watch?v=0Q6gHT_N-1U)
- [Gluten Intro Article at Medium.com](https://medium.com/intel-analytics-software/accelerate-spark-sql-queries-with-gluten-9000b65d1b4e)
- [Gluten Intro Article at Kyligence.io(in Chinese)](https://cn.kyligence.io/blog/gluten-spark/)
- [Velox Intro from Meta](https://engineering.fb.com/2023/03/09/open-source/velox-open-source-execution-engine/)

# 2 Architecture

The overview chart is like below. Substrait provides a well-defined cross-language specification for data compute operations (see more details [here](https://substrait.io/)). Spark physical plan is transformed to Substrait plan. Then Substrait plan is passed to native through JNI call.
On native side, the native operator chain will be built out and offloaded to native engine. Gluten will return Columnar Batch to Spark and Spark Columnar API (since Spark-3.0) will be used at execution time. Gluten uses Apache Arrow data format as its basic data format, so the returned data to Spark JVM is ArrowColumnarBatch.
<p align="center">
<img src="https://user-images.githubusercontent.com/47296334/199617207-1140698a-4d53-462d-9bc7-303d14be060b.png" width="800">
</p>
Currently, Gluten only supports Clickhouse backend & Velox backend. Velox is a C++ database acceleration library which provides reusable, extensible and high-performance data processing components. More details can be found from https://github.com/facebookincubator/velox/. Gluten can also be extended to support more backends.

There are several key components in Gluten:
* **Query Plan Conversion**: converts Spark's physical plan to Substrait plan.
* **Unified Memory Management**: controls native memory allocation.
* **Columnar Shuffle**: shuffles Gluten columnar data. The shuffle service still reuses the one in Spark core. A kind of columnar exchange operator is implemented to support Gluten columnar data format.
* **Fallback Mechanism**: supports falling back to Vanilla spark for unsupported operators. Gluten ColumnarToRow (C2R) and RowToColumnar (R2C) will convert Gluten columnar data and Spark's internal row data if needed. Both C2R and R2C are implemented in native code as well
* **Metrics**: collected from Gluten native engine to help identify bugs, performance bottlenecks, etc. The metrics are displayed in Spark UI.
* **Shim Layer**: supports multiple Spark versions. We plan to only support Spark's latest 2 or 3 releases. Currently, Spark-3.2, Spark-3.3 & Spark-3.4 (experimental) are supported.

# 3 How to Use

There are two ways to use Gluten.

# 3.1 Use Released Jar

One way is to use released jar. Here is a simple example. Currently, only centos7/8 and ubuntu20.04/22.04 are well supported.

```
spark-shell \
 --master yarn --deploy-mode client \
 --conf spark.plugins=org.apache.gluten.GlutenPlugin \
 --conf spark.memory.offHeap.enabled=true \
 --conf spark.memory.offHeap.size=20g \
 --conf spark.shuffle.manager=org.apache.spark.shuffle.sort.ColumnarShuffleManager \
 --jars https://github.com/apache/incubator-gluten/releases/download/v1.0.0/gluten-velox-bundle-spark3.2_2.12-ubuntu_20.04_x86_64-1.0.0.jar
```

# 3.2 Custom Build

Alternatively, you can build gluten from source, then do some configurations to enable Gluten plugin for Spark. Here is a simple example. Please refer to the corresponding backend part below for more details.

```
export gluten_jar = /PATH/TO/GLUTEN/backends-velox/target/<gluten-jar>
spark-shell 
  --master yarn --deploy-mode client \
  --conf spark.plugins=org.apache.gluten.GlutenPlugin \
  --conf spark.memory.offHeap.enabled=true \
  --conf spark.memory.offHeap.size=20g \
  --conf spark.driver.extraClassPath=${gluten_jar} \
  --conf spark.executor.extraClassPath=${gluten_jar} \
  --conf spark.shuffle.manager=org.apache.spark.shuffle.sort.ColumnarShuffleManager
  ...
```

### 3.2.1 Build and install Gluten with Velox backend

If you want to use Gluten **Velox** backend, see [Build with Velox](./docs/get-started/Velox.md) to build and install the necessary libraries.

### 3.2.2 Build and install Gluten with ClickHouse backend

If you want to use Gluten **ClickHouse** backend, see [Build with ClickHouse Backend](./docs/get-started/ClickHouse.md). ClickHouse backend is developed by [Kyligence](https://kyligence.io/), please visit https://github.com/Kyligence/ClickHouse for more infomation.

### 3.2.3 Build options

See [Gluten build guide](./docs/get-started/build-guide.md).

# 4 Contribution

Welcome to contribute to Gluten project! See [contributing guide](CONTRIBUTING.md) about how to make contributions.

## 4.1 Community

Glunten successfully joined Apache Incubator since March'24. We welcome developers and users who are interested in Gluten project. Here are several ways to contact us:

### Gluten website
https://gluten.apache.org/

### Mailing lists
For any technical discussion, please send email to [dev@gluten.apache.org](mailto:dev@gluten.apache.org). See [archives](https://lists.apache.org/list.html?dev@gluten.apache.org).
Please click [here](mailto:dev-subscribe@gluten.apache.org) to subscribe.

### Wechat group
We also have a Wechat group (in Chinese) which maybe more firendly for PRC developers/users. Due to the limitation of wechat group, please contat with weitingchen at apache.org or zhangzc at apache.org to be invited to the group. 

### Slack channel
There's also a Spark channel in Velox Slack group (in English) for community communication for Velox backend. Please check Velox document here: https://github.com/facebookincubator/velox?tab=readme-ov-file#community

## 4.2 Issue Report

Please feel free to create Github issue for reporting bug or proposing enhancement. For contributing code, please submit an issue firstly and mention that issue in your PR.

## 4.3 Documentation

Currently, all gluten documents are held at [docs](https://github.com/apache/incubator-gluten/tree/main/docs). The documents may not reflect the latest designs. Please feel free to contact us for getting design details or sharing your design ideas.

# 5 Performance

We use Decision Support Benchmark1 (TPC-H like) to evaluate Gluten's performance.
Decision Support Benchmark1 is a query set modified from [TPC-H benchmark](http://tpc.org/tpch/default5.asp). We use Parquet file format for Velox testing & MergeTree file format for Clickhouse testing, compared to Parquet file format as baseline. See [Decision Support Benchmark1](./tools/workload/tpch).

The below test environment: single node with 2TB data; Spark-3.3.2 for both baseline and Gluten. The Decision Support Benchmark1 result (tested in Jun. 2023) shows an overall speedup of 2.71x and up to 14.53x speedup in a single query with Gluten Velox backend used.

![Performance](./docs/image/velox_decision_support_bench1_22queries_performance.png)

The below testing environment: a 8-nodes AWS cluster with 1TB data; Spark-3.1.1 for both baseline and Gluten. The Decision Support Benchmark1 result shows an average speedup of 2.12x and up to 3.48x speedup with Gluten Clickhouse backend.

![Performance](./docs/image/clickhouse_decision_support_bench1_22queries_performance.png)

# 6 License

Gluten is licensed under [Apache 2.0 license](https://www.apache.org/licenses/LICENSE-2.0).

# 7 Contact

Gluten was initiated by Intel and Kyligence in 2022. Several companies such as Intel, Kyligence, BIGO, Meituan, Alibaba Cloud, NetEase, Baidu, Microsoft and others, are actively participating in the development of Gluten. If you are interested in Gluten project, please contact below email address for further discussion.

binwei.yang@intel.com; weiting.chen@intel.com;
chang.chen@kyligence.io; zhichao.zhang@kyligence.io; neng.liu@kyligence.io;
zuochunwei@meituan.com;yangchuan.zy@alibaba-inc.com;xiyu.zk@alibaba-inc.com;joey.ljy@alibaba-inc.com


##### \* LEGAL NOTICE: Your use of this software and any required dependent software (the "Software Package") is subject to the terms and conditions of the software license agreements for the Software Package, which may also include notices, disclaimers, or license terms for third party or open source software included in or with the Software Package, and your use indicates your acceptance of all such terms. Please refer to the "TPP.txt" or other similarly-named text file included with the Software Package for additional details.

