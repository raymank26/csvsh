package com.github.raymank26

import java.util.BitSet
import java.util.regex.Pattern

/**
 * Date: 2019-05-15.
 */
class SqlExecutor {

    fun execute(engineContext: EngineContext, planDescription: PlanDescription?): DatasetResult {
        if (planDescription == null) {
            return readRows(engineContext, null)
        }
        val expressionToLines = executeAtomExpressions(engineContext, planDescription.expressionsBySource)
        val resultLines = BitSetEvalMerger(expressionToLines).visitExpression(planDescription.expressionTree)
        return readRows(engineContext, resultLines)
    }

    private fun executeAtomExpressions(engineContext: EngineContext, expressionsBySource: Map<ScanSource, List<ExpressionAtom>>): Map<ExpressionAtom, BitSet> {
        val result = mutableMapOf<ExpressionAtom, BitSet>()
        for ((scanSource, atoms) in expressionsBySource) {
            val exp = when (scanSource) {
                is CsvInput -> evalOverCsv(engineContext, atoms)
                is IndexInput -> {
                    evalOverIndex(engineContext, scanSource.name, atoms)
                }
            }
            result.putAll(exp)
        }
        return result
    }

    private fun evalOverIndex(engineContext: EngineContext, indexName: String, expressions: List<ExpressionAtom>): Map<ExpressionAtom, BitSet> {
        val index = requireNotNull(engineContext.fieldToIndex[indexName]) { "Unable to find index for name = $indexName" }
        val fieldType = index.getType()

        val result = mutableMapOf<ExpressionAtom, BitSet>()
        for (expression in expressions) {
            val rowsBitSet = when (fieldType) {
                FieldType.INTEGER -> {
                    val right = (expression.rightVal as IntValue).value
                    @Suppress("UNCHECKED_CAST") val integerIndex = index as ReadOnlyIndexNumber<Int>
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
                    @Suppress("UNCHECKED_CAST") val floatIndex = index as ReadOnlyIndexNumber<Float>
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
                    @Suppress("UNCHECKED_CAST") val integerIndex = index as ReadOnlyIndexNumber<String>
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
            result[expression] = rowsBitSet
        }
        return result
    }

    private fun evalOverCsv(engineContext: EngineContext, atoms: List<ExpressionAtom>): Map<ExpressionAtom, BitSet> {
        val result = mutableMapOf<ExpressionAtom, BitSet>()
        engineContext.sourceProvider.read({ row: DatasetRow ->
            for (expression in atoms) {
                if (isRowApplicable(engineContext, row, expression)) {
                    result.compute(expression) {_, bs ->
                        return@compute if (bs == null) {
                            BitSet().also { it.set(row.rowNum) }
                        } else {
                            bs.set(row.rowNum)
                            bs
                        }
                    }
                }
            }
        }, null)
        return result
    }

    private fun isRowApplicable(engineContext: EngineContext, row: DatasetRow, atom: ExpressionAtom): Boolean {
        val fieldName = (atom.leftVal as RefValue).name
        val columnNum = engineContext.sourceProvider.getColumnInfo()[fieldName]?.position
                ?: throw RuntimeException("Not found")
        val columnValue = row.columns[columnNum]

        return when (requireNotNull(engineContext.sourceProvider.getColumnInfo()[fieldName]).type) {
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

    private fun readRows(engineContext: EngineContext, rowIndexes: BitSet?): DatasetResult {
        val rows = mutableListOf<DatasetRow>()
        engineContext.sourceProvider.read({ csvRow: DatasetRow ->
            if (rowIndexes == null || rowIndexes[csvRow.rowNum]) {
                rows.add(csvRow)
            }
        }, null)
        return DatasetResult(engineContext.sourceProvider.getColumnNames(), rows)
    }
}

private class BitSetEvalMerger(private val atomsToBitSet: Map<ExpressionAtom, BitSet>) : BaseExpressionVisitor<BitSet>() {
    override fun visitAtom(atom: ExpressionAtom): BitSet {
        return atomsToBitSet.getOrDefault(atom, BitSet())
    }

    override fun visitNode(node: ExpressionNode): BitSet {
        val leftLines = requireNotNull(node.left.accept(this), { "Left lines is null" })
        val rightLines = requireNotNull(node.right.accept(this), { "Right lines is null" })

        when (node.operator) {
            ExpressionOperator.AND -> leftLines.and(rightLines)
            ExpressionOperator.OR -> leftLines.or(rightLines)
        }
        return leftLines
    }
}