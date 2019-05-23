package com.github.raymank26

import com.github.raymank26.sql.SqlBaseVisitor
import com.github.raymank26.sql.SqlParser
import java.nio.file.Paths

/**
 * Date: 2019-05-13.
 */
class SqlPlanner {

    fun createPlan(sqlAst: SqlParser.SelectContext, datasetReaderFactory: DatasetReaderFactory): SqlPlan {
        val tablePath = Paths.get(sqlAst.table().IDENTIFIER_Q().text.drop(1).dropLast(1))
        val reader = datasetReaderFactory.getReader(tablePath)
                ?: throw PlannerException("Unable to find input for path = $tablePath")
        val sqlWherePlan = sqlAst.whereExpr()?.let { SqlWhereVisitor(reader.availableIndexes()).visit(it) }
        val selectStatements: List<SelectStatementExpr> = if (sqlAst.selectExpr().allColumns() != null) {
            emptyList()
        } else {
            sqlAst.selectExpr().selectColumn().map { SqlSelectColumnVisitor().visit(it) }
        }
        val groupByFields: List<String> = getGroupByExpression(sqlAst, selectStatements)

        val orderBy = sqlAst.orderByExpr()?.let {
            val ref = it.reference().IDENTIFIER().text
            OrderByPlanDescription(ref, it.DESC()?.let { true } ?: false)
        }
        val limit = sqlAst.limitExpr()?.INTEGER()?.text?.toInt()?.apply {
            if (this < 1) {
                throw PlannerException("Limit statement has to be > 0")
            }
        }
        return SqlPlan(selectStatements, reader, sqlWherePlan, groupByFields, orderBy, limit)
    }

    private fun getGroupByExpression(sqlAst: SqlParser.SelectContext, selectStatements: List<SelectStatementExpr>): List<String> {
        val groupedFields: List<String> = sqlAst.groupByExpr()?.reference()?.map { it.IDENTIFIER().text } ?: emptyList()
        if (groupedFields.isEmpty()) {
            return groupedFields
        }
        if (selectStatements.isEmpty()) {
            throw PlannerException("Unable to select not grouped fields")
        }
        val selectPlainFieldNames = selectStatements.mapNotNull { (it as? SelectFieldExpr)?.fieldName }
        if (selectPlainFieldNames.toSet() != groupedFields.toSet()) {
            throw PlannerException("Unable to select not grouped fields")
        }
        return groupedFields
    }
}

private class SqlWhereVisitor(private val availableIndexes: List<IndexDescriptionAndPath>) : SqlBaseVisitor<WherePlanDescription?>() {

    override fun visitWhereExprAtom(ctx: SqlParser.WhereExprAtomContext): WherePlanDescription {
        var left = parseVariable(ctx.variable(0))
        var right = parseVariable(ctx.variable(1))
        var operator = Operator.TOKEN_TO_OPERATOR[ctx.BOOL_COMP().text]
                ?: throw RuntimeException("Unable to parse operator ${ctx.BOOL_COMP().text}")

        val source: ScanSource
        when {
            left is RefValue && right !is RefValue -> {
                source = getSource(left.name)
            }
            right is RefValue && left !is RefValue -> {
                source = getSource(right.name)
                val temp = left
                left = right
                right = temp
                operator = when (operator) {
                    Operator.LESS_THAN -> Operator.GREATER_THAN
                    Operator.GREATER_THAN -> Operator.LESS_THAN
                    Operator.LIKE -> Operator.LIKE
                    Operator.IN -> Operator.IN
                    Operator.EQ -> Operator.EQ
                    Operator.LESS_EQ_THAN -> Operator.GREATER_EQ_THAN
                    Operator.GREATER_EQ_THAN -> Operator.LESS_EQ_THAN
                }
            }
            else -> throw PlannerException("Left or right expression has to be a CSV field")
        }
        val atom = ExpressionAtom(left, operator, right)
        return WherePlanDescription(mutableMapOf(Pair(source, mutableListOf(atom))), atom)
    }

    override fun visitWhereExprIn(ctx: SqlParser.WhereExprInContext): WherePlanDescription {
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
        return WherePlanDescription(
                mutableMapOf(Pair(getSource(field.name), mutableListOf(atom))), atom)
    }

    override fun visitWhereExprBool(ctx: SqlParser.WhereExprBoolContext): WherePlanDescription {
        if (ctx.childCount != 3) {
            throw RuntimeException("Child count != 3")
        }
        val left: WherePlanDescription? = visit(ctx.getChild(0))
        val right: WherePlanDescription? = visit(ctx.getChild(2))
        if (left == null || right == null) {
            throw RuntimeException("Some null")
        }
        val operator = when {
            ctx.AND() != null -> ExpressionOperator.AND
            ctx.OR() != null -> ExpressionOperator.OR
            else -> throw RuntimeException("Unable to parse operator expression")
        }
        val expression = ExpressionNode(left.expressionTree, operator, right.expressionTree)
        return WherePlanDescription(mergeAtomPlans(left.expressionsBySource, right.expressionsBySource), expression)
    }

    override fun aggregateResult(aggregate: WherePlanDescription?, nextResult: WherePlanDescription?): WherePlanDescription? {
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
            "string" -> StringValue(variable.string().text.drop(1).dropLast(1))
            "reference" -> RefValue(variable.reference().text)
            else -> throw RuntimeException("Unable to parse type = ${variable.type}")
        }
    }

    private fun getSource(fieldName: String): ScanSource {
        return availableIndexes.find { it.description.fieldName == fieldName }?.let { IndexInput(it.description.name) }
                ?: CsvInput
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

private class SqlSelectColumnVisitor : SqlBaseVisitor<SelectStatementExpr>() {
    override fun visitSelectColumnPlain(ctx: SqlParser.SelectColumnPlainContext): SelectStatementExpr {
        return SelectFieldExpr(ctx.reference().IDENTIFIER().text)
    }

    override fun visitSelectColumnAgg(ctx: SqlParser.SelectColumnAggContext): SelectStatementExpr {
        return AggSelectExpr(ctx.AGG().text, ctx.reference().text)
    }
}