package com.github.raymank26

import java.util.regex.Pattern

/**
 * Date: 2019-05-15.
 */
class SqlExecutor {

    fun execute(sqlPlan: SqlPlan): DatasetResult {
        val lines = if (sqlPlan.wherePlanDescription != null) {
            val expressionToLines = executeAtomExpressions(sqlPlan)
            ExpressionTreeEvaluator(expressionToLines).visitExpression(sqlPlan.wherePlanDescription.expressionTree)
        } else {
            null
        }
        var (dataset, resource) = readDataset(sqlPlan, lines)
        return resource.use {
            if (sqlPlan.groupByFields.isNotEmpty()) {
                dataset = applyGroupBy(sqlPlan, dataset)
            } else {
                dataset = applySelect(sqlPlan, dataset)
            }
            if (sqlPlan.orderByPlanDescription != null) {
                dataset = applyOrderBy(sqlPlan.orderByPlanDescription, dataset)
            }
            if (sqlPlan.limit != null) {
                dataset = applyLimit(sqlPlan.limit, dataset)
            }
            dataset
        }
    }

    private fun executeAtomExpressions(sqlPlan: SqlPlan): Map<ExpressionAtom, Set<Int>> {
        if (sqlPlan.wherePlanDescription == null) {
            return emptyMap()
        }

        val result = mutableMapOf<ExpressionAtom, Set<Int>>()
        for ((scanSource, atoms) in sqlPlan.wherePlanDescription.expressionsBySource) {
            val exp = when (scanSource) {
                is CsvInput -> evalOverCsv(sqlPlan, atoms)
                is IndexInput -> {
                    evalOverIndex(sqlPlan, scanSource.name, atoms)
                }
            }
            result.putAll(exp)
        }
        return result
    }

    private fun evalOverIndex(sqlPlan: SqlPlan, indexName: String, expressions: List<ExpressionAtom>): Map<ExpressionAtom, Set<Int>> {
        val index = requireNotNull(sqlPlan.datasetReader.availableIndexes.find { it.description.name == indexName }?.indexContent) { "Unable to find index for name = $indexName" }

        val result = mutableMapOf<ExpressionAtom, Set<Int>>()
        for (expression in expressions) {
            val right = expression.rightVal
            val op = expression.operator

            result[expression] = when {
                op == Operator.LESS_THAN && right is SqlValueAtom -> index.lessThan(right)
                op == Operator.LESS_EQ_THAN && right is SqlValueAtom -> index.lessThanEq(right)
                op == Operator.GREATER_THAN && right is SqlValueAtom -> index.moreThan(right)
                op == Operator.GREATER_EQ_THAN && right is SqlValueAtom -> index.moreThanEq(right)
                op == Operator.EQ && right is SqlValueAtom -> index.eq(right)
                op == Operator.IN && right is ListValue -> index.inRange(right)
                else -> throw RuntimeException("Unable to exec op = $op")
            }
        }
        return result
    }

    private fun evalOverCsv(sqlPlan: SqlPlan, atoms: List<ExpressionAtom>): Map<ExpressionAtom, Set<Int>> {
        val result = mutableMapOf<ExpressionAtom, MutableSet<Int>>()
        sqlPlan.datasetReader.getIterator().use { iterator ->
            iterator.forEach { row: DatasetRow ->
                for (expression in atoms) {
                    if (isRowApplicable(row, expression)) {
                        result.compute(expression) { _, bs ->
                            return@compute if (bs == null) {
                                mutableSetOf(row.rowNum)
                            } else {
                                bs.add(row.rowNum)
                                bs
                            }
                        }
                    }
                }
            }
        }
        return result
    }

    private fun isRowApplicable(row: DatasetRow, atom: ExpressionAtom): Boolean {
        val fieldName = (atom.leftVal as RefValue).name
        val columnValue = row.getCell(fieldName)
        return checkAtom(atom, columnValue, atom.rightVal)
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

    private fun readDataset(sqlPlan: SqlPlan, rowIndexes: Set<Int>?): Pair<DatasetResult, AutoCloseable> {
        val newSequence = sqlPlan.datasetReader.getIterator()
                .asSequence()
                .filter { csvRow ->
                    rowIndexes == null || rowIndexes.contains(csvRow.rowNum)
                }
        return Pair(DatasetResult(newSequence, sqlPlan.datasetReader.columnInfo), sqlPlan.datasetReader.getIterator().resource
                ?: AutoCloseable { })
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
            DatasetRow(row.rowNum, columns, newColumnInfo)
        }
        return DatasetResult(newSequence, newColumnInfo)
    }

    private fun applyGroupBy(sqlPlan: SqlPlan, rows: DatasetResult): DatasetResult {
        val groupByBuckets = mutableMapOf<List<Pair<ColumnInfo, SqlValueAtom>>, List<AggregateFunction>>()
        val aggStatements = sqlPlan.selectStatements.mapNotNull { it as? AggSelectExpr }
        val plainStatements = sqlPlan.selectStatements.mapNotNull { it as? SelectFieldExpr }

        val aggToColumnInfo = mutableListOf<ColumnInfo>()
        var plainToColumnInfo = listOf<ColumnInfo>()

        for (row in rows.rows) {
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
            return DatasetResult(emptySequence(), newColumnInfo)
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
            newRows.add(DatasetRow(rowNum++, columns, newColumnInfo))
        }
        return DatasetResult(newRows.asSequence(), newColumnInfo)
    }

    private fun applyOrderBy(orderByStmt: OrderByPlanDescription, dataset: DatasetResult): DatasetResult {
        val fieldName = orderByStmt.field.fullFieldName
        val f = { row: DatasetRow -> row.getCell(fieldName) }
        val newRows = if (orderByStmt.desc) {
            dataset.rows.sortedByDescending(f)
        } else {
            dataset.rows.sortedBy(f)
        }
        return dataset.copy(rows = newRows)
    }

    private fun applyLimit(limit: Int, dataset: DatasetResult): DatasetResult {
        return dataset.copy(rows = dataset.rows.take(limit))
    }
}

val AGGREGATES_MAPPING: Map<Pair<String, FieldType>, AggregateFunctionFactory> = mapOf(
        Pair(Pair("min", FieldType.INTEGER), Aggregates.MIN_INT),
        Pair(Pair("min", FieldType.FLOAT), Aggregates.MIN_FLOAT),
        Pair(Pair("max", FieldType.INTEGER), Aggregates.MAX_INT),
        Pair(Pair("max", FieldType.FLOAT), Aggregates.MAX_FLOAT),
        Pair(Pair("sum", FieldType.INTEGER), Aggregates.SUM_INT),
        Pair(Pair("sum", FieldType.FLOAT), Aggregates.SUM_FLOAT),
        Pair(Pair("count", FieldType.INTEGER), Aggregates.COUNT_ANY),
        Pair(Pair("count", FieldType.FLOAT), Aggregates.COUNT_ANY),
        Pair(Pair("count", FieldType.STRING), Aggregates.COUNT_ANY)
)

private class ExpressionTreeEvaluator(private val atomsToBitSet: Map<ExpressionAtom, Set<Int>>) : BaseExpressionVisitor<Set<Int>>() {
    override fun visitAtom(atom: ExpressionAtom): Set<Int> {
        return atomsToBitSet.getOrDefault(atom, emptySet())
    }

    override fun visitNode(node: ExpressionNode): Set<Int> {
        val leftLines = requireNotNull(node.left.accept(this), { "Left lines is null" })
        val rightLines = requireNotNull(node.right.accept(this), { "Right lines is null" })

        return when (node.operator) {
            ExpressionOperator.AND -> leftLines.intersect(rightLines)
            ExpressionOperator.OR -> leftLines.union(rightLines)
        }
    }
}
