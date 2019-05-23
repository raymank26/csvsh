package com.github.raymank26

import java.util.regex.Pattern

/**
 * Date: 2019-05-15.
 */
class SqlExecutor {

    fun execute(sqlPlan: SqlPlan): DatasetResult {
        if (sqlPlan.wherePlanDescription != null) {
            val expressionToLines = executeAtomExpressions(sqlPlan)
            val resultLines = ExpressionTreeEvaluator(expressionToLines).visitExpression(sqlPlan.wherePlanDescription.expressionTree)
            var dataset = readDataset(sqlPlan, resultLines)
            if (sqlPlan.groupByFields.isNotEmpty()) {
                dataset = applyGroupBy(sqlPlan, dataset)
            }
            if (sqlPlan.orderByPlanDescription != null) {
                dataset = applyOrderBy(sqlPlan.orderByPlanDescription, dataset)
            }
            if (sqlPlan.limit != null) {
                dataset = applyLimit(sqlPlan.limit, dataset)
            }
            return dataset
        } else {
            TODO()
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
        val index = requireNotNull(sqlPlan.datasetReader.availableIndexes().find { it.description.name == indexName }?.indexContent) { "Unable to find index for name = $indexName" }
        val fieldType = index.getType()

        val result = mutableMapOf<ExpressionAtom, Set<Int>>()
        for (expression in expressions) {
            val rowsFound = when (fieldType) {
                FieldType.INTEGER -> {
                    val right = lazy { (expression.rightVal as IntValue).value }
                    val rightAsList = lazy { (expression.rightVal as ListValue).value.map { (it as IntValue).value } }
                    @Suppress("UNCHECKED_CAST") val integerIndex = index as ReadOnlyIndex<Int>
                    when (val op = expression.operator) {
                        Operator.LESS_THAN -> integerIndex.lessThan(right.value)
                        Operator.LESS_EQ_THAN -> integerIndex.lessThanEq(right.value)
                        Operator.GREATER_THAN -> integerIndex.moreThan(right.value)
                        Operator.GREATER_EQ_THAN -> integerIndex.moreThanEq(right.value)
                        Operator.EQ -> integerIndex.eq(right.value)
                        Operator.IN -> integerIndex.inRange(rightAsList.value)
                        else -> throw RuntimeException("Unable to exec op = $op")
                    }
                }
                FieldType.FLOAT -> {
                    val right = lazy { (expression.rightVal as FloatValue).value }
                    val rightAsList = lazy { (expression.rightVal as ListValue).value.map { (it as FloatValue).value } }
                    @Suppress("UNCHECKED_CAST") val floatIndex = index as ReadOnlyIndex<Float>
                    when (val op = expression.operator) {
                        Operator.LESS_THAN -> floatIndex.lessThan(right.value)
                        Operator.LESS_EQ_THAN -> floatIndex.lessThanEq(right.value)
                        Operator.GREATER_THAN -> floatIndex.moreThan(right.value)
                        Operator.GREATER_EQ_THAN -> floatIndex.moreThanEq(right.value)
                        Operator.EQ -> floatIndex.moreThanEq(right.value)
                        Operator.IN -> floatIndex.inRange(rightAsList.value)
                        else -> throw RuntimeException("Unable to exec op = $op")
                    }
                }
                FieldType.STRING -> {
                    val right = (expression.rightVal as StringValue).value
                    val rightAsList = lazy { (expression.rightVal as ListValue).value.map { (it as StringValue).value } }
                    @Suppress("UNCHECKED_CAST") val integerIndex = index as ReadOnlyIndex<String>
                    when (val op = expression.operator) {
                        Operator.LESS_THAN -> integerIndex.lessThan(right)
                        Operator.LESS_EQ_THAN -> integerIndex.lessThanEq(right)
                        Operator.GREATER_THAN -> integerIndex.moreThan(right)
                        Operator.GREATER_EQ_THAN -> integerIndex.moreThanEq(right)
                        Operator.EQ -> integerIndex.eq(right)
                        Operator.IN -> integerIndex.inRange(rightAsList.value)
                        else -> throw RuntimeException("Unable to exec op = $op")
                    }
                }
            }
            result[expression] = rowsFound
        }
        return result
    }

    private fun evalOverCsv(sqlPlan: SqlPlan, atoms: List<ExpressionAtom>): Map<ExpressionAtom, Set<Int>> {
        val result = mutableMapOf<ExpressionAtom, MutableSet<Int>>()
        sqlPlan.datasetReader.read({ row: DatasetRow ->
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
        }, null)
        return result
    }

    private fun isRowApplicable(row: DatasetRow, atom: ExpressionAtom): Boolean {
        val fieldName = (atom.leftVal as RefValue).name
        val columnValue = row.getCellTyped(fieldName) ?: fieldNotFound(fieldName)


        return when (requireNotNull(row.getCellType(fieldName))) {
            FieldType.INTEGER -> {
                checkAtom(atom, columnValue, atom.rightVal) { sqlValue -> (sqlValue as IntValue).value }
            }
            FieldType.FLOAT -> {
                checkAtom(atom, columnValue, atom.rightVal) { sqlValue -> (sqlValue as FloatValue).value }
            }
            FieldType.STRING -> {
                checkAtom(atom, columnValue, atom.rightVal) { sqlValue -> (sqlValue as StringValue).value }
            }
        }
    }

    private fun <T> checkAtom(atom: ExpressionAtom, fieldValue: Comparable<T>, sqlValue: SqlValue, toT: (SqlValue) -> T): Boolean {
        return when (atom.operator) {
            Operator.LESS_THAN -> fieldValue < toT(sqlValue)
            Operator.LESS_EQ_THAN -> fieldValue <= toT(sqlValue)
            Operator.GREATER_THAN -> fieldValue > toT(sqlValue)
            Operator.GREATER_EQ_THAN -> fieldValue >= toT(sqlValue)
            Operator.EQ -> fieldValue == toT(sqlValue)
            Operator.IN -> (sqlValue as ListValue).value.any { toT(it) == fieldValue }
            Operator.LIKE -> {
                // TODO: actually, SQL pattern matching is a great deal more complicated.
                val regExValue = (sqlValue as StringValue).value.replace("%", ".*").replace('%', '?')
                Pattern.compile(regExValue).toRegex().matches(fieldValue as String)
            }
        }
    }

    private fun readDataset(sqlPlan: SqlPlan, rowIndexes: Set<Int>?): DatasetResult {
        val rows = mutableListOf<DatasetRow>()
        sqlPlan.datasetReader.read({ csvRow: DatasetRow ->
            if (rowIndexes == null || rowIndexes.contains(csvRow.rowNum)) {
                rows.add(csvRow)
            }
        }, null)
        return DatasetResult(rows)
    }

    private fun applyGroupBy(sqlPlan: SqlPlan, rows: DatasetResult): DatasetResult {
        val groupByBuckets = mutableMapOf<List<String>, List<AggregateFunction<Any, Any>>>()
        val indexedAggStatements = sqlPlan.selectStatements.mapNotNull { it as? AggSelectExpr }

        for (row in rows.rows) {
            val bucketDesc = sqlPlan.groupByFields.map { row.getCell(it) ?: fieldNotFound(it) }
            groupByBuckets.compute(bucketDesc) { _, prev ->
                if (prev != null) {
                    for ((i, aggStatement) in indexedAggStatements.withIndex()) {
                        prev[i].process(row.getCell(aggStatement.fieldName)!!)
                    }
                    prev
                } else {
                    Array(indexedAggStatements.size) { i ->
                        val aggStatement = indexedAggStatements[i]
                        val cellType = row.getCellType(aggStatement.fieldName)
                        val agg = AGGREGATES_MAPPING[Pair(aggStatement.type, cellType)]
                                ?: throw PlannerException("Unable to execute agg of type = ${aggStatement.fieldName} and column type = $cellType")
                        agg.process(row.getCellTyped(aggStatement.fieldName)!!)
                        agg
                    }.toList()
                }
            }
        }
        val newRows = mutableListOf<DatasetRow>()
        for ((fixedFields, aggregates) in groupByBuckets) {
            val cells = mutableListOf<String>()
            for (fieldName in fixedFields) {
                cells.add(fieldName)
            }
            for (agg in aggregates) {
                cells.add(agg.toText())
            }
        }
        return DatasetResult(newRows)
    }

    private fun fieldNotFound(name: String): Nothing {
        throw ExecutorException("Field \"$name\" is not found")
    }

    private fun applyOrderBy(orderByStmt: OrderByPlanDescription, dataset: DatasetResult): DatasetResult {
        val f = { row: DatasetRow -> row.getCell(orderByStmt.field) ?: fieldNotFound(orderByStmt.field) }
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

@Suppress("UNCHECKED_CAST")
val AGGREGATES_MAPPING: Map<Pair<String, FieldType>, AggregateFunction<Any, Any>> = mapOf(
        Pair(Pair("MAX", FieldType.INTEGER), Aggregates.MAX_INT as AggregateFunction<Any, Any>),
        Pair(Pair("MAX", FieldType.FLOAT), Aggregates.MAX_FLOAT as AggregateFunction<Any, Any>),
        Pair(Pair("SUM", FieldType.INTEGER), Aggregates.SUM_INT as AggregateFunction<Any, Any>),
        Pair(Pair("SUM", FieldType.FLOAT), Aggregates.SUM_FLOAT as AggregateFunction<Any, Any>),
        Pair(Pair("COUNT", FieldType.INTEGER), Aggregates.COUNT_ANY as AggregateFunction<Any, Any>),
        Pair(Pair("COUNT", FieldType.FLOAT), Aggregates.COUNT_ANY as AggregateFunction<Any, Any>),
        Pair(Pair("COUNT", FieldType.STRING), Aggregates.COUNT_ANY as AggregateFunction<Any, Any>)
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
