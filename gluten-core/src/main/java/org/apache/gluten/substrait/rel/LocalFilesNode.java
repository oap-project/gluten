/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gluten.substrait.rel;

import org.apache.gluten.GlutenConfig;
import org.apache.gluten.expression.ConverterUtils;

import io.substrait.proto.NamedStruct;
import io.substrait.proto.ReadRel;
import io.substrait.proto.Type;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class LocalFilesNode implements SplitInfo {
  private final Integer index;
  private final List<String> paths = new ArrayList<>();
  private final List<Long> starts = new ArrayList<>();
  private final List<Long> lengths = new ArrayList<>();
  private final List<Map<String, String>> partitionColumns = new ArrayList<>();
  private final List<Map<String, String>> metadataColumns = new ArrayList<>();
  private final List<String> preferredLocations = new ArrayList<>();

  // The format of file to read.
  public enum ReadFileFormat {
    ParquetReadFormat(),
    ArrowReadFormat(),
    OrcReadFormat(),
    DwrfReadFormat(),
    MergeTreeReadFormat(),
    TextReadFormat(),
    JsonReadFormat(),
    UnknownFormat()
  }

  protected ReadFileFormat fileFormat = ReadFileFormat.UnknownFormat;
  private Boolean iterAsInput = false;
  private StructType fileSchema;
  private Map<String, String> fileReadProperties;

  LocalFilesNode(
      Integer index,
      List<String> paths,
      List<Long> starts,
      List<Long> lengths,
      List<Map<String, String>> partitionColumns,
      List<Map<String, String>> metadataColumns,
      ReadFileFormat fileFormat,
      List<String> preferredLocations) {
    this.index = index;
    this.paths.addAll(paths);
    this.starts.addAll(starts);
    this.lengths.addAll(lengths);
    this.fileFormat = fileFormat;
    this.partitionColumns.addAll(partitionColumns);
    this.metadataColumns.addAll(metadataColumns);
    this.preferredLocations.addAll(preferredLocations);
  }

  LocalFilesNode(String iterPath) {
    this.index = null;
    this.paths.add(iterPath);
    this.iterAsInput = true;
  }

  public List<String> getPaths() {
    return paths;
  }

  public void setFileSchema(StructType schema) {
    this.fileSchema = schema;
  }

  private NamedStruct buildNamedStruct() {
    NamedStruct.Builder namedStructBuilder = NamedStruct.newBuilder();

    if (fileSchema != null) {
      Type.Struct.Builder structBuilder = Type.Struct.newBuilder();
      for (StructField field : fileSchema.fields()) {
        structBuilder.addTypes(
            ConverterUtils.getTypeNode(field.dataType(), field.nullable()).toProtobuf());
        namedStructBuilder.addNames(field.name());
      }
      namedStructBuilder.setStruct(structBuilder.build());
    }
    return namedStructBuilder.build();
  }

  public void setFileReadProperties(Map<String, String> fileReadProperties) {
    this.fileReadProperties = fileReadProperties;
  }

  @Override
  public List<String> preferredLocations() {
    return this.preferredLocations;
  }

  /**
   * Data Lake formats require some additional processing to be done on the FileBuilder, such as
   * inserting delete files information. Different lake formats should override this method to
   * implement their corresponding logic.
   */
  protected void processFileBuilder(
      ReadRel.LocalFiles.FileOrFiles.Builder fileBuilder, int index) {}

  public ReadRel.LocalFiles toProtobuf() {
    ReadRel.LocalFiles.Builder localFilesBuilder = ReadRel.LocalFiles.newBuilder();
    // The input is iterator, and the path is in the format of: Iterator:index.
    if (iterAsInput && !paths.isEmpty()) {
      ReadRel.LocalFiles.FileOrFiles.Builder fileBuilder =
          ReadRel.LocalFiles.FileOrFiles.newBuilder();
      fileBuilder.setUriFile(paths.get(0));
      localFilesBuilder.addItems(fileBuilder.build());
      return localFilesBuilder.build();
    }
    if (paths.size() != starts.size() || paths.size() != lengths.size()) {
      throw new RuntimeException("Invalid parameters.");
    }
    for (int i = 0; i < paths.size(); i++) {
      ReadRel.LocalFiles.FileOrFiles.Builder fileBuilder =
          ReadRel.LocalFiles.FileOrFiles.newBuilder();
      fileBuilder.setUriFile(paths.get(i));
      if (index != null) {
        fileBuilder.setPartitionIndex(index);
      }
      Map<String, String> partitionColumn = partitionColumns.get(i);
      if (!partitionColumn.isEmpty()) {
        partitionColumn.forEach(
            (key, value) -> {
              ReadRel.LocalFiles.FileOrFiles.partitionColumn.Builder pcBuilder =
                  ReadRel.LocalFiles.FileOrFiles.partitionColumn.newBuilder();
              pcBuilder.setKey(key).setValue(value);
              fileBuilder.addPartitionColumns(pcBuilder.build());
            });
      }
      fileBuilder.setLength(lengths.get(i));
      fileBuilder.setStart(starts.get(i));
      if (!metadataColumns.isEmpty()) {
        Map<String, String> metadataColumn = metadataColumns.get(i);
        if (!metadataColumn.isEmpty()) {
          metadataColumn.forEach(
              (key, value) -> {
                ReadRel.LocalFiles.FileOrFiles.metadataColumn.Builder mcBuilder =
                    ReadRel.LocalFiles.FileOrFiles.metadataColumn.newBuilder();
                mcBuilder.setKey(key).setValue(value);
                fileBuilder.addMetadataColumns(mcBuilder.build());
              });
        }
      } else {
        ReadRel.LocalFiles.FileOrFiles.metadataColumn.Builder mcBuilder =
            ReadRel.LocalFiles.FileOrFiles.metadataColumn.newBuilder();
        fileBuilder.addMetadataColumns(mcBuilder.build());
      }
      NamedStruct namedStruct = buildNamedStruct();
      fileBuilder.setSchema(namedStruct);

      switch (fileFormat) {
        case ParquetReadFormat:
          ReadRel.LocalFiles.FileOrFiles.ParquetReadOptions parquetReadOptions =
              ReadRel.LocalFiles.FileOrFiles.ParquetReadOptions.newBuilder()
                  .setEnableRowGroupMaxminIndex(
                      GlutenConfig.getConf().enableParquetRowGroupMaxMinIndex())
                  .build();
          fileBuilder.setParquet(parquetReadOptions);
          break;
        case OrcReadFormat:
          ReadRel.LocalFiles.FileOrFiles.OrcReadOptions orcReadOptions =
              ReadRel.LocalFiles.FileOrFiles.OrcReadOptions.newBuilder().build();
          fileBuilder.setOrc(orcReadOptions);
          break;
        case DwrfReadFormat:
          ReadRel.LocalFiles.FileOrFiles.DwrfReadOptions dwrfReadOptions =
              ReadRel.LocalFiles.FileOrFiles.DwrfReadOptions.newBuilder().build();
          fileBuilder.setDwrf(dwrfReadOptions);
          break;
        case TextReadFormat:
          String field_delimiter = fileReadProperties.getOrDefault("field_delimiter", ",");
          String quote = fileReadProperties.getOrDefault("quote", "");
          String header = fileReadProperties.getOrDefault("header", "0");
          String escape = fileReadProperties.getOrDefault("escape", "");
          String nullValue = fileReadProperties.getOrDefault("nullValue", "");
          ReadRel.LocalFiles.FileOrFiles.TextReadOptions textReadOptions =
              ReadRel.LocalFiles.FileOrFiles.TextReadOptions.newBuilder()
                  .setFieldDelimiter(field_delimiter)
                  .setQuote(quote)
                  .setHeader(Long.parseLong(header))
                  .setEscape(escape)
                  .setNullValue(nullValue)
                  .setMaxBlockSize(GlutenConfig.getConf().textInputMaxBlockSize())
                  .setEmptyAsDefault(GlutenConfig.getConf().textIputEmptyAsDefault())
                  .build();
          fileBuilder.setText(textReadOptions);
          break;
        case JsonReadFormat:
          ReadRel.LocalFiles.FileOrFiles.JsonReadOptions jsonReadOptions =
              ReadRel.LocalFiles.FileOrFiles.JsonReadOptions.newBuilder()
                  .setMaxBlockSize(GlutenConfig.getConf().textInputMaxBlockSize())
                  .build();
          fileBuilder.setJson(jsonReadOptions);
          break;
        default:
          break;
      }
      processFileBuilder(fileBuilder, i);
      localFilesBuilder.addItems(fileBuilder.build());
    }
    return localFilesBuilder.build();
  }
}
