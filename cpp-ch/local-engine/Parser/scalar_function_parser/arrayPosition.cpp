#include <Parser/FunctionParser.h>
#include <DataTypes/IDataType.h>
#include <Common/CHUtil.h>
#include <Core/Field.h>

namespace local_engine
{

class FunctionParserArrayPosition : public FunctionParser
{
public:
    explicit FunctionParserArrayPosition(SerializedPlanParser * plan_parser_) : FunctionParser(plan_parser_) { }
    ~FunctionParserArrayPosition() override = default;

    static constexpr auto name = "array_position";

    String getName() const override { return name; }

    String getCHFunctionName(const CommonFunctionInfo &) const override { return "indexOf"; }

    String getCHFunctionName(const DB::DataTypes &) const override { return "indexOf"; }

    const ActionsDAG::Node * parse(
        const substrait::Expression_ScalarFunction & substrait_func,
        ActionsDAGPtr & actions_dag) const override
    {
        /**
            parse array_position(arr, value) as
            if (isNull(arr) || isNull(value))
                null
            else
                indexOf(assertNotNull(arr), value)

            Main difference between Spark array_position and CH indexOf:
            1. Spark array_position returns null if either of the arguments are null
            2. CH indexOf function cannot accept Nullable(Array()) type as first argument
        */

        auto func_info = CommonFunctionInfo{substrait_func};
        auto parsed_args = parseFunctionArguments(func_info, "", actions_dag);
        if (parsed_args.size() != 2)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Function {} requires exactly two arguments", getName());

        auto ch_function_name = getCHFunctionName(func_info);

        const auto * arr_arg = parsed_args[0];
        const auto * value_arg = parsed_args[1];

        auto is_arr_nullable = arr_arg->result_type->isNullable();
        auto is_value_nullable = value_arg->result_type->isNullable();

        if (!is_arr_nullable && !is_value_nullable)
        {
            const auto * ch_function_node = toFunctionNode(actions_dag, ch_function_name, {arr_arg, value_arg});
            return convertNodeTypeIfNeeded(substrait_func, ch_function_node, actions_dag);
        }

        const auto * arr_is_null_node = toFunctionNode(actions_dag, "isNull", {arr_arg});
        const auto * value_is_null_node = toFunctionNode(actions_dag, "isNull", {value_arg});
        const auto * or_node = toFunctionNode(actions_dag, "or", {arr_is_null_node, value_is_null_node});

        const auto * arr_not_null_node = is_arr_nullable ? toFunctionNode(actions_dag, "assumeNotNull", {arr_arg}) : arr_arg;
        const auto * ch_function_node = toFunctionNode(actions_dag, ch_function_name, {arr_not_null_node, value_arg});
        DataTypePtr wrap_arr_nullable_type = wrapNullableType(true, ch_function_node->result_type);

        const auto * wrap_index_of_node = ActionsDAGUtil::convertNodeType(
            actions_dag, ch_function_node, wrap_arr_nullable_type->getName(), ch_function_node->result_name);
        const auto * null_const_node = addColumnToActionsDAG(actions_dag, wrap_arr_nullable_type, Field{});

        const auto * if_node = toFunctionNode(actions_dag, "if", {or_node, null_const_node, wrap_index_of_node});
        return convertNodeTypeIfNeeded(substrait_func, if_node, actions_dag);
    }
};

static FunctionParserRegister<FunctionParserArrayPosition> register_array_position;
}
