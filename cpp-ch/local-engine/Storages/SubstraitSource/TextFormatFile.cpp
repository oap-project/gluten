#include "TextFormatFile.h"

#include <memory>
#include <Formats/FormatSettings.h>
<<<<<<< HEAD
#include <Processors/Formats/Impl/HiveTextRowInputFormat.h>
=======
#include <IO/SeekableReadBuffer.h>
#include <IO/PeekableReadBuffer.h>
#include <IO/CompressionMethod.h>
#include <Storages/HDFS/ReadBufferFromHDFS.h>
#include <Processors/Formats/IRowInputFormat.h>
>>>>>>> bug fix for txt format compression

namespace local_engine
{

TextFormatFile::TextFormatFile(
    DB::ContextPtr context_, const substrait::ReadRel::LocalFiles::FileOrFiles & file_info_, ReadBufferBuilderPtr read_buffer_builder_)
    : FormatFile(context_, file_info_, read_buffer_builder_)
{
}

FormatFile::InputFormatPtr TextFormatFile::createInputFormat(const DB::Block & header)
{
    auto res = std::make_shared<FormatFile::InputFormat>();
    DB::CompressionMethod compression_method = DB::CompressionMethod::None;
    if (file_info.text().compression_type() == "ZLIB") 
    {
        compression_method = DB::CompressionMethod::Zlib;
    }
    else if (file_info.text().compression_type() == "BZ2")
    {
        compression_method = DB::CompressionMethod::Bzip2;
    }
    res->read_buffer = std::move(read_buffer_builder->build(file_info, true, compression_method));

    /// Initialize format params
    size_t max_block_size = file_info.text().max_block_size();
    DB::RowInputFormatParams params = {max_block_size};

    /// Initialize format settings
    DB::FormatSettings format_settings = DB::getFormatSettings(context);
    const auto & schema = file_info.text().schema();
    for (const auto & name : schema.names())
        format_settings.hive_text.input_field_names.push_back(name);
    format_settings.hive_text.fields_delimiter = file_info.text().field_delimiter()[0];
    res->input = std::make_shared<DB::HiveTextRowInputFormat>(header, *(res->read_buffer), params, format_settings);
    return res;
}

}
