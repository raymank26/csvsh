package com.github.raymank26

import com.github.raymank26.sql.SqlBaseVisitor
import com.github.raymank26.sql.SqlParser

/**
 * Date: 2019-05-13.
 */
class SqlPlannerVisitor(private val availableIndexes: List<IndexDescription>) : SqlBaseVisitor<MutableMap<ScanSource, MutableList<Expression>>>() {
    override fun visitWhereExprAtom(ctx: SqlParser.WhereExprAtomContext): MutableMap<ScanSource, MutableList<Expression>> {
        val a = parseVariable(ctx.variable(0))
        val b = parseVariable(ctx.variable(1))
        val operator = Operator.TOKEN_TO_OPERATOR[ctx.BOOL_COMP().text]
                ?: throw PlannerException("Unable to parse operator ${ctx.BOOL_COMP().text}")
        val source = when {
            a is RefValue && b is RefValue -> CsvInput
            a is RefValue -> getSource(a.name)
            b is RefValue -> getSource(b.name)
            else -> throw PlannerException("Left or right expression has to be a CSV field")
        }
        return mutableMapOf(Pair(source, mutableListOf(Expression(a, operator, b))))
    }

    override fun visitWhereExprIn(ctx: SqlParser.WhereExprInContext): MutableMap<ScanSource, MutableList<Expression>> {
        val field: RefValue = parseVariable(ctx.variable(0)).also {
            if (it !is RefValue) {
                throw PlannerException("Left variable has to be table field")
            }
        } as RefValue

        val others = ctx.variable().drop(1)
        var type: Class<*>? =  null
        val variables: MutableList<SqlValue> = mutableListOf()
        for (variable in others) {
            val parsedVariable = parseVariable(variable)
            if (parsedVariable is RefValue) {
                throw PlannerException("Expression in IN clause mustn't be field")
            }
            if (type == null) {
                type = parsedVariable.javaClass
            } else if (type != parsedVariable.javaClass) {
                throw PlannerException("IN clause has different types")
            }
            variables.add(parsedVariable)
        }
        val expression = Expression(field, Operator.IN, ListValue(variables))
        return mutableMapOf(Pair(getSource(field.name), mutableListOf(expression)))
    }

    private fun parseVariable(variable: SqlParser.VariableContext): SqlValue {
        return when (variable.type) {
            "float" -> FloatValue(variable.floatNumber().text.toFloat())
            "integer" -> IntValue(variable.integerNumber().text.toInt())
            "string" -> StringValue(variable.string().text)
            "reference" -> RefValue(variable.reference().text)
            else -> throw PlannerException("Unable to parse type = ${variable.type}")
        }
    }

    private fun getSource(fieldName: String): ScanSource {
        return availableIndexes.find { it.fieldName == fieldName }?.let { IndexInput(it.name) } ?: CsvInput
    }

    override fun defaultResult(): MutableMap<ScanSource, MutableList<Expression>> {
        return mutableMapOf()
    }

    override fun aggregateResult(aggregate: MutableMap<ScanSource, MutableList<Expression>>,
                                 nextResult: MutableMap<ScanSource, MutableList<Expression>>):
            MutableMap<ScanSource, MutableList<Expression>> {

        for (entry in nextResult) {
            aggregate.compute(entry.key) { _, originalExpressions ->
                return@compute if (originalExpressions == null) {
                    entry.value
                } else {
                    originalExpressions.addAll(entry.value)
                    originalExpressions
                }
            }
        }
        return aggregate
    }
}