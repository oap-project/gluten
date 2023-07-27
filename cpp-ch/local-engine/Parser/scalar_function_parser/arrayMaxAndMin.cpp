#include <Parser/FunctionParser.h>
#include <Common/CHUtil.h>
#include <Core/Field.h>
#include <DataTypes/IDataType.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}
}

namespace local_engine
{

class BaseFunctionParserArrayMaxAndMin : public FunctionParser
{
public:
    explicit BaseFunctionParserArrayMaxAndMin(SerializedPlanParser * plan_parser_) : FunctionParser(plan_parser_) { }
    ~BaseFunctionParserArrayMaxAndMin() override = default;

    const ActionsDAG::Node * parse(
        const substrait::Expression_ScalarFunction & substrait_func,
        ActionsDAGPtr & actions_dag) const override
    {
        auto parsed_args = parseFunctionArguments(substrait_func, "", actions_dag);
        if (parsed_args.size() != 1)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Function {} requires exactly one arguments", getName());

        const auto * max_const_node = addColumnToActionsDAG(actions_dag, std::make_shared<DataTypeString>(), getCHFunctionName(substrait_func));
        const auto * array_reduce_node = toFunctionNode(actions_dag, "arrayReduce", {max_const_node, parsed_args[0]});
        return convertNodeTypeIfNeeded(substrait_func, array_reduce_node, actions_dag);
    }
};

class FunctionParserArrayMax : public BaseFunctionParserArrayMaxAndMin
{
public:
    explicit FunctionParserArrayMax(SerializedPlanParser * plan_parser_) : BaseFunctionParserArrayMaxAndMin(plan_parser_) { }
    ~FunctionParserArrayMax() override = default;

    static constexpr auto name = "array_max";
    String getName() const override { return name; }
    String getCHFunctionName(const substrait::Expression_ScalarFunction &) const override { return "max"; }
};
static FunctionParserRegister<FunctionParserArrayMax> register_array_max;


class FunctionParserArrayMin : public BaseFunctionParserArrayMaxAndMin
{
public:
    explicit FunctionParserArrayMin(SerializedPlanParser * plan_parser_) : BaseFunctionParserArrayMaxAndMin(plan_parser_) { }
    ~FunctionParserArrayMin() override = default;

    static constexpr auto name = "array_min";
    String getName() const override { return name; }
    String getCHFunctionName(const substrait::Expression_ScalarFunction &) const override { return "min"; }
};
static FunctionParserRegister<FunctionParserArrayMin> register_array_min;

}
