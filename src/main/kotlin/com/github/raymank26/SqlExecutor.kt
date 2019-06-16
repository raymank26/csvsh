package com.github.raymank26

import java.util.regex.Pattern

/**
 * Date: 2019-05-15.
 */
class SqlExecutor {

    fun execute(sqlPlan: SqlPlan): DatasetResult {
        var dataset = readDataset(sqlPlan)

        if (sqlPlan.wherePlanDescription != null) {
            dataset = applyWhere(sqlPlan, dataset)
        }
        dataset = if (sqlPlan.groupByFields.isNotEmpty()) {
            applyGroupBy(sqlPlan, dataset)
        } else {
            applySelect(sqlPlan, dataset)
        }
        if (sqlPlan.orderByPlanDescription != null) {
            dataset = applyOrderBy(sqlPlan.orderByPlanDescription, dataset)
        }
        if (sqlPlan.limit != null) {
            dataset = applyLimit(sqlPlan.limit, dataset)
        }
        return dataset
    }

    private fun applyWhere(sqlPlan: SqlPlan, dataset: DatasetResult): DatasetResult {
        return dataset.copy(rows = dataset.rows.filter { row ->
            DatasetRowExpressionCheck(row).visitExpression(sqlPlan.wherePlanDescription!!.expressionTree)
        })
    }

    private fun readDataset(sqlPlan: SqlPlan): DatasetResult {
        val newSequence = if (sqlPlan.wherePlanDescription != null) {
            val result = DatasetIndexOffsetCollector(sqlPlan.datasetReader.availableIndexes).visitExpression(sqlPlan.wherePlanDescription.expressionTree)
            result?.invoke()?.let { sqlPlan.datasetReader.getIterator(it.sorted().toList()) }
                    ?: sqlPlan.datasetReader.getIterator()
        } else {
            sqlPlan.datasetReader.getIterator()
        }
        return DatasetResult(newSequence, sqlPlan.datasetReader.columnInfo)
    }

    private fun applySelect(sqlPlan: SqlPlan, dataset: DatasetResult): DatasetResult {
        val allowedFields = if (sqlPlan.selectStatements.isEmpty()) {
            dataset.columnInfo.map { it.fieldName }
        } else {
            sqlPlan.selectStatements.asSequence().map { it as SelectFieldExpr }.map { it.fieldName }.toSet()
        }

        val fieldNameToInfo = dataset.columnInfo.associateBy { it.fieldName }
        val newColumnInfo = allowedFields.map { fieldNameToInfo[it]!! }

        val newSequence = dataset.rows.map { row ->
            allowedFields.map { row.getCell(it) }
            val columns = mutableListOf<SqlValueAtom>()
            for (allowedField in allowedFields) {
                columns.add(row.getCell(allowedField))
            }
            DatasetRow(row.rowNum, columns, newColumnInfo, row.offset)
        }
        return DatasetResult(newSequence, newColumnInfo)
    }

    private fun applyGroupBy(sqlPlan: SqlPlan, dataset: DatasetResult): DatasetResult {
        val groupByBuckets = mutableMapOf<List<Pair<ColumnInfo, SqlValueAtom>>, List<AggregateFunction>>()
        val aggStatements = sqlPlan.selectStatements.mapNotNull { it as? AggSelectExpr }
        val plainStatements = sqlPlan.selectStatements.mapNotNull { it as? SelectFieldExpr }

        val aggToColumnInfo = mutableListOf<ColumnInfo>()
        var plainToColumnInfo = listOf<ColumnInfo>()

        dataset.rows.forEach { row ->
            val bucketDesc = sqlPlan.groupByFields.map { Pair(row.getColumnInfo(it)!!, row.getCell(it)) }
            if (plainToColumnInfo.isEmpty()) {
                for (plainStatement in plainStatements) {
                    plainToColumnInfo = bucketDesc.map { it.first }
                }
            }
            groupByBuckets.compute(bucketDesc) { _, prev ->
                if (prev != null) {
                    for ((i, aggStatement) in aggStatements.withIndex()) {
                        prev[i].process(row.getCell(aggStatement.fieldName))
                    }
                    prev
                } else {
                    val initAggList = mutableListOf<AggregateFunction>()
                    for (i in 0 until aggStatements.size) {
                        val aggStatement = aggStatements[i]
                        val cell = row.getCell(aggStatement.fieldName)
                        val agg = AGGREGATES_MAPPING[Pair(aggStatement.type, cell.type)]?.invoke()
                                ?: throw PlannerException("Unable to execute agg of type = ${aggStatement.type} and column type = ${cell.type}")
                        agg.process(cell)
                        initAggList.add(agg)
                    }
                    if (aggToColumnInfo.isEmpty()) {
                        for (i in 0 until aggStatements.size) {
                            val aggStatement = aggStatements[i]
                            val type = row.getCellType(aggStatement.fieldName)
                            aggToColumnInfo.add(ColumnInfo(type, aggStatement.fullFieldName))
                        }
                    }
                    initAggList
                }
            }
        }
        val newColumnInfo = {
            val res = mutableListOf<ColumnInfo>()
            res.addAll(plainToColumnInfo)
            res.addAll(aggToColumnInfo)
            res
        }()

        if (groupByBuckets.isEmpty()) {
            return DatasetResult(ClosableSequence(emptySequence()), newColumnInfo)
        }

        val newRows = mutableListOf<DatasetRow>()
        var rowNum = 0
        for ((fixedFields, aggregates) in groupByBuckets) {
            val columns = mutableListOf<SqlValueAtom>()
            for (bucketField in fixedFields) {
                columns.add(bucketField.second)
            }
            for (agg in aggregates) {
                columns.add(agg.getResult())
            }
            newRows.add(DatasetRow(rowNum++, columns, newColumnInfo, null))
        }
        return DatasetResult(ClosableSequence(newRows.asSequence()), newColumnInfo)
    }

    private fun applyOrderBy(orderByStmt: OrderByPlanDescription, dataset: DatasetResult): DatasetResult {
        val fieldName = orderByStmt.field.fullFieldName
        val f = { row: DatasetRow -> row.getCell(fieldName) }
        val newRows = if (orderByStmt.desc) {
            dataset.rows.transform { it.sortedByDescending(f) }
        } else {
            dataset.rows.transform { it.sortedBy(f) }
        }
        return dataset.copy(rows = newRows)
    }

    private fun applyLimit(limit: Int, dataset: DatasetResult): DatasetResult {
        return dataset.copy(rows = dataset.rows.take(limit))
    }
}

val AGGREGATES_MAPPING: Map<Pair<String, FieldType>, AggregateFunctionFactory> = mapOf(
        Pair(Pair("min", FieldType.LONG), Aggregates.MIN_INT),
        Pair(Pair("min", FieldType.DOUBLE), Aggregates.MIN_FLOAT),
        Pair(Pair("min", FieldType.STRING), Aggregates.MIN_STRING),
        Pair(Pair("max", FieldType.LONG), Aggregates.MAX_INT),
        Pair(Pair("max", FieldType.DOUBLE), Aggregates.MAX_FLOAT),
        Pair(Pair("max", FieldType.STRING), Aggregates.MAX_STRING),
        Pair(Pair("sum", FieldType.LONG), Aggregates.SUM_INT),
        Pair(Pair("sum", FieldType.DOUBLE), Aggregates.SUM_FLOAT),
        Pair(Pair("count", FieldType.LONG), Aggregates.COUNT_ANY),
        Pair(Pair("count", FieldType.DOUBLE), Aggregates.COUNT_ANY),
        Pair(Pair("count", FieldType.STRING), Aggregates.COUNT_ANY)
)

private class DatasetRowExpressionCheck(private val datasetRow: DatasetRow) : BaseExpressionVisitor<Boolean>() {
    override fun visitAtom(atom: ExpressionAtom): Boolean {
        val fieldName = (atom.leftVal as RefValue).name
        val columnValue = datasetRow.getCell(fieldName)
        return checkAtom(atom, columnValue, atom.rightVal)
    }

    override fun visitNode(node: ExpressionNode): Boolean {
        val leftResult = requireNotNull(node.left.accept(this), { "Left lines is null" })
        val rightResult = requireNotNull(node.right.accept(this), { "Right lines is null" })

        return when (node.operator) {
            ExpressionOperator.AND -> leftResult && rightResult
            ExpressionOperator.OR -> leftResult || rightResult
        }
    }

    private fun checkAtom(atom: ExpressionAtom, fieldValue: SqlValueAtom, sqlValue: SqlValue): Boolean {
        return when (atom.operator) {
            Operator.LESS_THAN -> fieldValue lt (sqlValue as SqlValueAtom)
            Operator.LESS_EQ_THAN -> fieldValue lte (sqlValue as SqlValueAtom)
            Operator.GREATER_THAN -> fieldValue gt (sqlValue as SqlValueAtom)
            Operator.GREATER_EQ_THAN -> fieldValue gte (sqlValue as SqlValueAtom)
            Operator.EQ -> fieldValue eq (sqlValue as SqlValueAtom)
            Operator.IN -> (sqlValue as ListValue).value.any { it eq fieldValue }
            Operator.LIKE -> {
                val stringContent = (sqlValue as StringValue).value
                        ?: return false
                // TODO: actually, SQL pattern matching is a great deal more complicated.
                val regExValue = stringContent.replace("%", ".*").replace('%', '?')
                Pattern.compile(regExValue).toRegex().matches(stringContent)
            }
        }
    }
}

typealias IndexEvaluator = () -> Set<Long>

private class DatasetIndexOffsetCollector(indexes: List<IndexDescriptionAndPath>) : BaseExpressionVisitor<IndexEvaluator?>() {

    private val indexesMap = indexes.associateBy { it.description.fieldName }

    override fun visitAtom(atom: ExpressionAtom): IndexEvaluator? {
        if (atom.leftVal !is RefValue) {
            throw IllegalStateException("Left value is expected to be reference")
        }
        val index = indexesMap[atom.leftVal.name]?.indexContent ?: return null

        val right = atom.rightVal
        val op = atom.operator

        return {
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
    }

    override fun visitNode(node: ExpressionNode): IndexEvaluator? {
        val leftResult: IndexEvaluator? = node.left.accept(this)
        val rightResult: IndexEvaluator? = node.right.accept(this)

        return when (node.operator) {
            ExpressionOperator.AND -> when {
                leftResult == null && rightResult != null -> rightResult
                leftResult != null && rightResult == null -> leftResult
                leftResult != null && rightResult != null -> {
                    { leftResult.invoke().intersect(rightResult.invoke()) }
                }
                else -> null
            }
            ExpressionOperator.OR -> when {
                leftResult == null && rightResult != null -> null
                leftResult != null && rightResult == null -> null
                leftResult != null && rightResult != null -> {
                    val a: () -> Set<Long> = { leftResult.invoke().union(rightResult.invoke()) }
                    a
                }
                else -> null
            }
        }
    }
}
