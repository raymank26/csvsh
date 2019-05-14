package com.github.raymank26

import com.github.raymank26.sql.SqlBaseVisitor
import com.github.raymank26.sql.SqlParser

/**
 * Date: 2019-05-13.
 */
class SqlPlannerVisitor(private val availableIndexes: List<IndexDescription>) : SqlBaseVisitor<PlanDescription?>() {

    override fun visitWhereExprAtom(ctx: SqlParser.WhereExprAtomContext): PlanDescription {
        val a = parseVariable(ctx.variable(0))
        val b = parseVariable(ctx.variable(1))
        val operator = Operator.TOKEN_TO_OPERATOR[ctx.BOOL_COMP().text]
                ?: throw RuntimeException("Unable to parse operator ${ctx.BOOL_COMP().text}")
        val source = when {
            a is RefValue && b is RefValue -> CsvInput
            a is RefValue -> getSource(a.name)
            b is RefValue -> getSource(b.name)
            else -> throw PlannerException("Left or right expression has to be a CSV field")
        }
        val atom = ExpressionAtom(a, operator, b)
        return PlanDescription(mutableMapOf(Pair(source, mutableListOf(atom))), atom)
    }

    override fun visitWhereExprIn(ctx: SqlParser.WhereExprInContext): PlanDescription {
        val field: RefValue = parseVariable(ctx.variable(0)) as? RefValue
                ?: throw PlannerException("Left variable has to be table field")

        val others = ctx.variable().drop(1)
        var type: Class<*>? = null
        val variables: MutableList<SqlValue> = mutableListOf()
        for (variable in others) {
            val parsedVariable = parseVariable(variable)
            if (parsedVariable is RefValue) {
                throw RuntimeException("Expression in IN clause mustn't be field")
            }
            if (type == null) {
                type = parsedVariable.javaClass
            } else if (type != parsedVariable.javaClass) {
                throw RuntimeException("IN clause has different types")
            }
            variables.add(parsedVariable)
        }
        val atom = ExpressionAtom(field, Operator.IN, ListValue(variables))
        return PlanDescription(
                mutableMapOf(Pair(getSource(field.name), mutableListOf(atom))), atom)
    }

    override fun visitWhereExprBool(ctx: SqlParser.WhereExprBoolContext): PlanDescription {
        if (ctx.childCount != 3) {
            throw RuntimeException("Child count != 3")
        }
        val left: PlanDescription? = visit(ctx.getChild(0))
        val right: PlanDescription? = visit(ctx.getChild(2))
        if (left == null || right == null) {
            throw RuntimeException("Some null")
        }
        val operator = when {
            ctx.AND() != null -> ExpressionOperator.AND
            ctx.OR() != null -> ExpressionOperator.OR
            else -> throw RuntimeException("Unable to parse operator expression")
        }
        val expression = ExpressionNode(left.expressionTree, operator, right.expressionTree)
        return PlanDescription(mergeAtomPlans(left.expressionsBySource, right.expressionsBySource), expression)
    }

    override fun aggregateResult(aggregate: PlanDescription?, nextResult: PlanDescription?): PlanDescription? {
        return when {
            aggregate != null -> aggregate
            nextResult != null -> nextResult
            else -> null
        }
    }

    private fun parseVariable(variable: SqlParser.VariableContext): SqlValue {
        return when (variable.type) {
            "float" -> FloatValue(variable.floatNumber().text.toFloat())
            "integer" -> IntValue(variable.integerNumber().text.toInt())
            "string" -> StringValue(variable.string().text)
            "reference" -> RefValue(variable.reference().text)
            else -> throw RuntimeException("Unable to parse type = ${variable.type}")
        }
    }

    private fun getSource(fieldName: String): ScanSource {
        return availableIndexes.find { it.fieldName == fieldName }?.let { IndexInput(it.name) } ?: CsvInput
    }

    private fun mergeAtomPlans(left: Map<ScanSource, List<ExpressionAtom>>,
                       right: Map<ScanSource, List<ExpressionAtom>>):
            Map<ScanSource, List<ExpressionAtom>> {

        val result = mutableMapOf<ScanSource, MutableList<ExpressionAtom>>()
        result.putAll(left.mapValues { ArrayList(it.value) })

        for (entry in right) {
            result.compute(entry.key) { _, originalExpressions ->
                return@compute if (originalExpressions == null) {
                    ArrayList(entry.value)
                } else {
                    originalExpressions.addAll(entry.value)
                    originalExpressions
                }
            }
        }
        return result
    }
}