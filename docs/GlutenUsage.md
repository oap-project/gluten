### Gluten Usage

#### How to compile Gluten jar

``` shell
git clone -b main https://github.com/oap-project/gluten.git
cd gluten
```

When compiling Gluten, a backend should be enabled for execution.
For example, add below options to enable Velox backend:

If you wish to automatically build velox from source. The default velox installation path will be in "/PATH_TO_GLUTEN/tools/build/velox-ep/velox_ep".

```shell script
-Dbuild_velox=ON -Dbuild_velox_from_source=ON
```

If you wish to enable Velox backend and you have an existing compiled Velox or Velox repo, please use velox_home to set the path.

```shell script
-Dbuild_velox=ON -Dvelox_home=${VELOX_HOME}
```

We provide a single command to help build arrow as well as velox under Ubuntu20.04 environment.
The full compiling command would be like:

```shell script
mvn clean package -Pbackends-velox -Pfull-scala-compiler -DskipTests -Dcheckstyle.skip -Dbuild_cpp=ON -Dbuild_velox=ON -Dbuild_velox_from_source=ON -Dbuild_arrow=ON
```

If Arrow has once been installed successfully on your env, and there is no change to Arrow, you can
add below config to disable Arrow compilation each time when compiling Gluten:

```shell script
-Dbuild_arrow=OFF -Darrow_root=${ARROW_LIB_PATH}
```
Compile gluten again with existed velox and arrow.
The full compiling command would be like:
```shell script
mvn package -Pbackends-velox -Pfull-scala-compiler -DskipTests -Dcheckstyle.skip -Dbuild_cpp=ON -Dbuild_velox=ON -Dbuild_arrow=ON -Dbuild_protobuf=OFF
```
while arrow_root and velox_home is the default value as following

Based on the different environment, there are some parameters can be set via -D with mvn.

| Parameters | Description | Default Value |
| ---------- | ----------- | ------------- |
| build_cpp | Enable or Disable building CPP library | OFF |
| cpp_tests | Enable or Disable CPP Tests | OFF |
| build_arrow | Build Arrow from Source | OFF |
| arrow_root | When build_arrow set to False, arrow_root will be enabled to find the location of your existing arrow library. | /PATH_TO_GLUTEN/tools/build/arrow_install |
| build_protobuf | Build Protobuf from Source. If set to False, default library path will be used to find protobuf library. |ON |
| build_velox_from_source | Enable or Disable building Velox from a specific velox github repository. A default installed path will be in velox_home | OFF |
| backends-velox | Add -Pbackends-velox in maven command to compile the JVM part of Velox backend| false |
| backends-clickhouse | Add -Pbackends-clickhouse in maven command to compile the JVM part of ClickHouse backend | false |
| build_velox | Enable or Disable building the CPP part of Velox backend | OFF |
| velox_home (only valid when build_velox is ON) | The path to the compiled Velox project. When building Gluten with Velox, if you have an existing Velox, please set it. | /PATH_TO_GLUTEN/tools/build/velox_ep |
| velox_build_type | The build type Velox was built with from source code. Gluten uses this value to locate the binary path of Velox's binary libraries. | release |
| debug_build | Whether to generate debug binary library from Gluten's C++ codes. | OFF |

When build_arrow set to True, the build_arrow.sh will be launched and compile a custom arrow library from [OAP Arrow](https://github.com/oap-project/arrow/tree/arrow-8.0.0-gluten)
If you wish to change any parameters from Arrow, you can change it from the [build_arrow.sh](../tools/build_arrow.sh) script.

#### How to submit Spark Job

##### Key Spark Configurations when using Gluten

```shell script
spark.plugins io.glutenproject.GlutenPlugin
spark.gluten.sql.columnar.backend.lib ${BACKEND}
spark.sql.sources.useV1SourceList avro
spark.memory.offHeap.size 20g
spark.driver.extraClassPath ${GLUTEN_HOME}/backends-velox/target/gluten-jvm-<version>-snapshot-jar-with-dependencies.jar
spark.executor.extraClassPath ${GLUTEN_HOME}/backends-velox/target/gluten-jvm-<version>-snapshot-jar-with-dependencies.jar
```
${BACKEND} can be velox or clickhouse, refer [Velox.md](https://github.com/oap-project/gluten/blob/main/docs/Velox.md}) and [ClickHouse.md](https://github.com/oap-project/gluten/blob/main/docs/ClickHouse.md) to get more detail.

Below is an example of the script to submit Spark SQL query.

cat query.scala
```shell script
// For ORC
val table = spark.read.format("orc").load("${ORC_PATH}")
// For Parquet
val table = spark.read.parquet("${PARQURT_PATH}")
table.createOrReplaceTempView("${TABLE_NAME}")
time{spark.sql("${QUERY}").show}
```

Submit the above script from spark-shell to trigger a Spark Job with certain configurations.

```shell script
cat query.scala | spark-shell --name query --master yarn --deploy-mode client --conf spark.plugins=io.glutenproject.GlutenPlugin --conf spark.gluten.sql.columnar.backend.lib=${BACKEND} --conf spark.driver.extraClassPath=${gluten_jvm_jar} --conf spark.executor.extraClassPath=${gluten_jvm_jar} --conf spark.memory.offHeap.size=20g --conf spark.sql.sources.useV1SourceList=avro --num-executors 6 --executor-cores 6 --driver-memory 20g --executor-memory 25g --conf spark.executor.memoryOverhead=5g --conf spark.driver.maxResultSize=32g
```
