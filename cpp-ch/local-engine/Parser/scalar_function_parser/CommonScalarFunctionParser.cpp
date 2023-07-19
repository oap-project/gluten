#include <Functions/SparkFunctionNextDay.h>
#include <Parser/FunctionParser.h>
#include <Common/Exception.h>

namespace local_engine
{
#define REGISTER_COMMON_SCALAR_FUNCTION_PARSER(cls_name, substrait_name, ch_name) \
    class ScalarFunctionParser##cls_name : public FunctionParser \
    { \
    public: \
        ScalarFunctionParser##cls_name(SerializedPlanParser * plan_parser_) : FunctionParser(plan_parser_) \
        { \
        } \
        ~ScalarFunctionParser##cls_name() override = default; \
        static constexpr auto name = #substrait_name; \
        String getName() const override \
        { \
            return #substrait_name; \
        } \
    protected: \
        String getCHFunctionName(const substrait::Expression_ScalarFunction & /*substrait_func*/) const override \
        { \
            return #ch_name; \
        } \
    }; \
    static const FunctionParserRegister<ScalarFunctionParser##cls_name> register_scalar_function_parser_##cls_name;

REGISTER_COMMON_SCALAR_FUNCTION_PARSER(NextDay, next_day, spark_next_day)
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(LastDay, last_day, toLastDayOfMonth)
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(Str2Map, str_to_map, spark_str_to_map)
}
