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
            val rows = readRows(sqlPlan, resultLines)
            if (sqlPlan.groupByFields.isNotEmpty()) {
            }
            TODO()
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
                    val right = (expression.rightVal as IntValue).value
                    @Suppress("UNCHECKED_CAST") val integerIndex = index as ReadOnlyIndex<Int>
                    when (val op = expression.operator) {
                        Operator.LESS_THAN -> integerIndex.lessThan(right)
                        Operator.LESS_EQ_THAN -> integerIndex.lessThanEq(right)
                        Operator.GREATER_THAN -> integerIndex.moreThan(right)
                        Operator.GREATER_EQ_THAN -> integerIndex.moreThanEq(right)
                        Operator.EQ -> integerIndex.eq(right)
                        else -> throw RuntimeException("Unable to exec op = $op")
                    }
                }
                FieldType.FLOAT -> {
                    val right = (expression.rightVal as FloatValue).value
                    @Suppress("UNCHECKED_CAST") val floatIndex = index as ReadOnlyIndex<Float>
                    when (val op = expression.operator) {
                        Operator.LESS_THAN -> floatIndex.lessThan(right)
                        Operator.LESS_EQ_THAN -> floatIndex.lessThanEq(right)
                        Operator.GREATER_THAN -> floatIndex.moreThan(right)
                        Operator.GREATER_EQ_THAN -> floatIndex.moreThanEq(right)
                        Operator.EQ -> floatIndex.moreThanEq(right)
                        else -> throw RuntimeException("Unable to exec op = $op")
                    }
                }
                FieldType.STRING -> {
                    val right = (expression.rightVal as StringValue).value
                    @Suppress("UNCHECKED_CAST") val integerIndex = index as ReadOnlyIndex<String>
                    when (val op = expression.operator) {
                        Operator.LESS_THAN -> integerIndex.lessThan(right)
                        Operator.LESS_EQ_THAN -> integerIndex.lessThanEq(right)
                        Operator.GREATER_THAN -> integerIndex.moreThan(right)
                        Operator.GREATER_EQ_THAN -> integerIndex.moreThanEq(right)
                        Operator.EQ -> integerIndex.eq(right)
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
                if (isRowApplicable(sqlPlan, row, expression)) {
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

    private fun isRowApplicable(sqlPlan: SqlPlan, row: DatasetRow, atom: ExpressionAtom): Boolean {
        val fieldName = (atom.leftVal as RefValue).name
        val columnNum = sqlPlan.datasetReader.getColumnInfo()[fieldName]?.position
                ?: throw RuntimeException("Not found")
        val columnValue = row.columns[columnNum]

        return when (requireNotNull(sqlPlan.datasetReader.getColumnInfo()[fieldName]).type) {
            FieldType.INTEGER -> {
                val fieldValue: Int = columnValue.toInt()
                checkAtom(atom, fieldValue, atom.rightVal) { sqlValue -> (sqlValue as IntValue).value }
            }
            FieldType.FLOAT -> {
                val fieldValue = columnValue.toFloat()
                checkAtom(atom, fieldValue, atom.rightVal) { sqlValue -> (sqlValue as FloatValue).value }
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

    private fun readRows(sqlPlan: SqlPlan, rowIndexes: Set<Int>?): DatasetResult {
        val rows = mutableListOf<DatasetRow>()
        sqlPlan.datasetReader.read({ csvRow: DatasetRow ->
            if (rowIndexes == null || rowIndexes.contains(csvRow.rowNum)) {
                rows.add(csvRow)
            }
        }, null)
        return DatasetResult(sqlPlan.datasetReader.getColumnNames(), rows)
    }
}

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