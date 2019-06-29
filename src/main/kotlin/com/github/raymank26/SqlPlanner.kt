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
        val sqlWherePlan = sqlAst.whereExpr()?.let { SqlWhereVisitor().visit(it) }
        val indexEvaluator = if (sqlWherePlan != null) {
            DatasetIndexOffsetCollector(reader.availableIndexes)
                    .visitExpression(sqlWherePlan.expressionTree)
        } else null

        val selectStatements: List<SelectStatementExpr> = if (sqlAst.selectExpr().allColumns() != null) {
            emptyList()
        } else {
            sqlAst.selectExpr().selectColumn().map { SqlSelectColumnVisitor().visit(it) }
        }
        val groupByFields = if (sqlAst.groupByExpr() != null) {
            getGroupByExpression(sqlAst, selectStatements)
        } else {
            validateSelectExpression(reader, selectStatements)
            emptyList()
        }

        val orderBy = sqlAst.orderByExpr()?.let {
            val ref = SqlSelectColumnVisitor().visit(it.selectColumn())
            OrderByPlanDescription(ref, it.DESC()?.let { true } ?: false)
        }
        val limit = sqlAst.limitExpr()?.INTEGER()?.text?.toInt()?.apply {
            if (this < 1) {
                throw PlannerException("Limit statement has to be > 0")
            }
        }
        return SqlPlan(selectStatements, reader, sqlWherePlan, groupByFields, orderBy, limit, indexEvaluator)
    }

    private fun validateSelectExpression(reader: DatasetReader, selectStatements: List<SelectStatementExpr>) {
        val fieldToInfo = reader.columnInfo.associateBy { it.fieldName }
        for (selectStatement in selectStatements) {
            when (selectStatement) {
                is AggSelectExpr -> throw PlannerException("Aggregate statements are not allowed without 'group by' statement")
                is SelectFieldExpr -> if (!fieldToInfo.containsKey(selectStatement.fullFieldName)) {
                    throw PlannerException("Select field = ${selectStatement.fieldName} is not in dataset")
                }
            }
        }
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

private class SqlWhereVisitor : SqlBaseVisitor<WherePlanDescription?>() {

    override fun visitWhereExprAtom(ctx: SqlParser.WhereExprAtomContext): WherePlanDescription {
        var left = parseVariable(ctx.variable(0))
        var right = parseVariable(ctx.variable(1))
        var operator = Operator.TOKEN_TO_OPERATOR[ctx.BOOL_COMP().text]
                ?: throw RuntimeException("Unable to parse operator ${ctx.BOOL_COMP().text}")

        if (right is RefValue && left !is RefValue) {
            val temp: SqlValue = left
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
        return WherePlanDescription(ExpressionAtom(left, operator, right))
    }

    override fun visitWhereExprIn(ctx: SqlParser.WhereExprInContext): WherePlanDescription {
        val field: RefValue = parseVariable(ctx.variable(0)) as? RefValue
                ?: throw PlannerException("Left variable has to be table field")

        val others = ctx.variable().drop(1)
        var type: FieldType? = null
        val variables: MutableList<SqlValueAtom> = mutableListOf()
        for (variable in others) {
            val parsedVariable = parseVariable(variable) as? SqlValueAtom
                    ?: throw PlannerException("Lists have to hold only non ref expressions")
            if (type == null) {
                type = parsedVariable.type
            } else if (type != parsedVariable.type) {
                throw RuntimeException("IN clause has different types")
            }
            variables.add(parsedVariable)
        }
        val atom = ExpressionAtom(field, Operator.IN, ListValue(variables))
        return WherePlanDescription(atom)
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
        return WherePlanDescription(expression)
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
            "float" -> DoubleValue(variable.floatNumber().text.toDouble())
            "integer" -> LongValue(variable.integerNumber().text.toLong())
            "string" -> StringValue(variable.string().text.drop(1).dropLast(1))
            "reference" -> RefValue(variable.reference().text)
            else -> throw RuntimeException("Unable to parse type = ${variable.type}")
        }
    }
}

private class SqlSelectColumnVisitor : SqlBaseVisitor<SelectStatementExpr>() {
    override fun visitSelectColumnPlain(ctx: SqlParser.SelectColumnPlainContext): SelectStatementExpr {
        return SelectFieldExpr(ctx.reference().IDENTIFIER().text)
    }

    override fun visitSelectColumnAgg(ctx: SqlParser.SelectColumnAggContext): SelectStatementExpr {
        return AggSelectExpr(ctx.AGG().text.toLowerCase(), ctx.reference().text)
    }
}

class DatasetIndexOffsetCollector(indexes: List<IndexDescriptionAndPath>) : BaseExpressionVisitor<IndexEvaluator?>() {

    private val indexesMap = indexes.associateBy { it.description.fieldName }

    override fun visitAtom(atom: ExpressionAtom): IndexEvaluator? {
        if (atom.leftVal !is RefValue) {
            throw IllegalStateException("Left value is expected to be reference")
        }
        val indexMeta: IndexDescriptionAndPath = indexesMap[atom.leftVal.name] ?: return null
        val index: ReadOnlyIndex = indexMeta.indexContent.value

        val right = atom.rightVal
        val op = atom.operator

        val lazyOffsets = {
            when {
                op == Operator.LESS_THAN && right is SqlValueAtom -> index.lessThan(right)
                op == Operator.LESS_EQ_THAN && right is SqlValueAtom -> index.lessThanEq(right)
                op == Operator.GREATER_THAN && right is SqlValueAtom -> index.moreThan(right)
                op == Operator.GREATER_EQ_THAN && right is SqlValueAtom -> index.moreThanEq(right)
                op == Operator.EQ && right is SqlValueAtom -> index.eq(right)
                op == Operator.IN && right is ListValue -> index.inRange(right)
                else -> throw RuntimeException("Unable to exec op = $op")
            }
        }
        return IndexEvaluator(setOf(indexMeta.description), lazyOffsets)
    }

    override fun visitNode(node: ExpressionNode): IndexEvaluator? {
        val leftResult: IndexEvaluator? = node.left.accept(this)
        val rightResult: IndexEvaluator? = node.right.accept(this)

        return when (node.operator) {
            ExpressionOperator.AND -> when {
                leftResult == null && rightResult != null -> rightResult
                leftResult != null && rightResult == null -> leftResult
                leftResult != null && rightResult != null -> {
                    val newOffsets = { leftResult.offsets.invoke().intersect(rightResult.offsets.invoke()) }
                    val newIndexes = leftResult.usedIndexes.intersect(rightResult.usedIndexes)
                    IndexEvaluator(newIndexes, newOffsets)
                }
                else -> null
            }
            ExpressionOperator.OR -> when {
                leftResult == null && rightResult != null -> null
                leftResult != null && rightResult == null -> null
                leftResult != null && rightResult != null -> {
                    val newOffsets = { leftResult.offsets.invoke().union(rightResult.offsets.invoke()) }
                    val newIndexes = leftResult.usedIndexes.union(rightResult.usedIndexes)
                    IndexEvaluator(newIndexes, newOffsets)
                }
                else -> null
            }
        }
    }
}

data class IndexEvaluator(val usedIndexes: Set<IndexDescription>, val offsets: () -> Set<Long>)
