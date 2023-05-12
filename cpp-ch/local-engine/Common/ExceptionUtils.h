#pragma once
#include <Common/Exception.h>
#include <Common/logger_useful.h>

namespace local_engine
{
class ExceptionUtils
{
public:
    static void handleException(const DB::Exception & exception);
};
}
