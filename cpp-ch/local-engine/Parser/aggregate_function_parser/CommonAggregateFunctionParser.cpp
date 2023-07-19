#include <Parser/AggregateFunctionParser.h>


namespace local_engine
{

#define REGISTER_COMMON_AGGREGATE_FUNCTION_PARSER(cls_name, substait_name, ch_name) \
    class AggregateFunctionParser##cls_name : public AggregateFunctionParser \
    { \
    public: \
        AggregateFunctionParser##cls_name(SerializedPlanParser * plan_parser_) : AggregateFunctionParser(plan_parser_) \
        { \
        } \
        ~AggregateFunctionParser##cls_name() override = default; \
        String getName() const override { return  #substait_name; } \
        static constexpr auto name = #substait_name; \
        String getCHFunctionName(const CommonFunctionInfo &) const override { return #ch_name; } \
        String getCHFunctionName(const DB::DataTypes &) const override { return #ch_name; } \
    }; \
    static const AggregateFunctionParserRegister<AggregateFunctionParser##cls_name> register_##cls_name = AggregateFunctionParserRegister<AggregateFunctionParser##cls_name>();

REGISTER_COMMON_AGGREGATE_FUNCTION_PARSER(Sum, sum, sum)
REGISTER_COMMON_AGGREGATE_FUNCTION_PARSER(Avg, avg, avg)
REGISTER_COMMON_AGGREGATE_FUNCTION_PARSER(Min, min, min)
REGISTER_COMMON_AGGREGATE_FUNCTION_PARSER(Max, max, max)
REGISTER_COMMON_AGGREGATE_FUNCTION_PARSER(StdDev, stddev, stddev_samp)
REGISTER_COMMON_AGGREGATE_FUNCTION_PARSER(StdDevSamp, stddev_samp, stddev_samp)
REGISTER_COMMON_AGGREGATE_FUNCTION_PARSER(StdDevPop, stddev_pop, stddev_pop)
REGISTER_COMMON_AGGREGATE_FUNCTION_PARSER(BitAnd, bit_and, groupBitAnd)
REGISTER_COMMON_AGGREGATE_FUNCTION_PARSER(BitOr, bit_or, groupBitOr)
REGISTER_COMMON_AGGREGATE_FUNCTION_PARSER(BitXor, bit_xor, groupBitXor)
REGISTER_COMMON_AGGREGATE_FUNCTION_PARSER(CovarPop, covar_pop, covarPop)
REGISTER_COMMON_AGGREGATE_FUNCTION_PARSER(CovarSamp, covar_samp, covarSamp)
REGISTER_COMMON_AGGREGATE_FUNCTION_PARSER(VarSamp, var_samp, varSamp)
REGISTER_COMMON_AGGREGATE_FUNCTION_PARSER(VarPop, var_pop, varPop)
REGISTER_COMMON_AGGREGATE_FUNCTION_PARSER(Corr, corr, corr)

REGISTER_COMMON_AGGREGATE_FUNCTION_PARSER(First, first, first_value_respect_nulls)
REGISTER_COMMON_AGGREGATE_FUNCTION_PARSER(FirstIgnoreNull, first_ignore_null, first_value)
REGISTER_COMMON_AGGREGATE_FUNCTION_PARSER(Last, last, last_value_respect_nulls)
REGISTER_COMMON_AGGREGATE_FUNCTION_PARSER(LastIgnoreNull, last_ignore_null, last_value)
REGISTER_COMMON_AGGREGATE_FUNCTION_PARSER(DenseRank, dense_rank, dense_rank)
REGISTER_COMMON_AGGREGATE_FUNCTION_PARSER(Rank, rank, rank)
REGISTER_COMMON_AGGREGATE_FUNCTION_PARSER(RowNumber, row_number, row_number)
REGISTER_COMMON_AGGREGATE_FUNCTION_PARSER(Ntile, ntile, ntile)
REGISTER_COMMON_AGGREGATE_FUNCTION_PARSER(PercentRank, percent_rank, percent_rank)
REGISTER_COMMON_AGGREGATE_FUNCTION_PARSER(CumeDist, cume_dist, cume_dist)

}
