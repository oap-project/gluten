#include "JsonFormatFile.h"

#include <Formats/FormatSettings.h>
#include <Formats/FormatFactory.h>
#include <IO/CompressionMethod.h>
#include <Processors/Formats/Impl/JSONEachRowRowInputFormat.h>

namespace local_engine
{

JsonFormatFile::JsonFormatFile(DB::ContextPtr context_, const substrait::ReadRel::LocalFiles::FileOrFiles & file_info_, ReadBufferBuilderPtr read_buffer_builder_)
    :FormatFile(context_, file_info_, read_buffer_builder_) {}

FormatFile::InputFormatPtr JsonFormatFile::createInputFormat(const DB::Block & header)
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
    DB::FormatSettings format_settings = DB::getFormatSettings(context);
    size_t max_block_size = file_info.json().max_block_size();
    DB::RowInputFormatParams in_params = {max_block_size};
    std::shared_ptr<DB::JSONEachRowRowInputFormat> json_input_format =
        std::make_shared<DB::JSONEachRowRowInputFormat>(*(res->read_buffer), header, in_params, format_settings, false);
    res->input = json_input_format;
    return res;
}

}
