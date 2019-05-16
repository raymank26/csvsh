package com.github.raymank26

import java.lang.RuntimeException
import java.util.BitSet

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
                    @Suppress("UNCHECKED_CAST") val stringIndex = index as ReadOnlyIndexString
                    val right = (expression.rightVal as StringValue).value
                    val op = expression.operator
                    require(op == Operator.IN)
                    stringIndex.eq(right)
                }
            }
            result[expression] = rowsBitSet
        }
        return result
    }

    private fun evalOverCsv(engineContext: EngineContext, atoms: List<ExpressionAtom>): Map<ExpressionAtom, BitSet> {
        val result = mutableMapOf<ExpressionAtom, BitSet>()
        engineContext.sourceProvider.read { row: DatasetRow ->
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
        }
        return result
    }

    private fun isRowApplicable(engineContext: EngineContext, row: DatasetRow, atom: ExpressionAtom): Boolean {
        val fieldName = (atom.leftVal as RefValue).name
        val columnNum = requireNotNull(engineContext.fieldToColumnNum[fieldName])
        val columnValue = row.columns[columnNum]

        return when (requireNotNull(engineContext.fieldToType[fieldName])) {
            FieldType.INTEGER -> {
                val fieldValue: Int = columnValue.toInt()
                val rightInt = (atom.rightVal as IntValue).value
                checkAtom(atom, fieldValue, rightInt)
            }
            FieldType.FLOAT -> {
                val fieldValue = columnValue.toFloat()
                val rightFloat = (atom.rightVal as FloatValue).value
                checkAtom(atom, fieldValue, rightFloat)
            }
            FieldType.STRING -> {
                require(atom.operator == Operator.EQ)
                columnValue == (atom.rightVal as StringValue).value
            }
        }
    }

    private fun <T : Number> checkAtom(atom: ExpressionAtom, fieldValue: Comparable<T>, rightInt: T): Boolean {
        return when (atom.operator) {
            Operator.LESS_THAN -> fieldValue < rightInt
            Operator.LESS_EQ_THAN -> fieldValue <= rightInt
            Operator.GREATER_THAN -> fieldValue > rightInt
            Operator.GREATER_EQ_THAN -> fieldValue >= rightInt
            Operator.EQ -> fieldValue == rightInt
            else -> throw RuntimeException("Operation is not applicable for ints, op = ${atom.operator}")
        }
    }

    private fun readRows(engineContext: EngineContext, rowIndexes: BitSet?): DatasetResult {
        val rows = mutableListOf<DatasetRow>()
        engineContext.sourceProvider.read { csvRow: DatasetRow ->
            if (rowIndexes == null || rowIndexes[csvRow.rowNum]) {
                rows.add(csvRow)
            }
        }
        return DatasetResult(engineContext.sourceProvider.getColumnNames(), rows)
    }
}

private class BitSetEvalMerger(private val atomsToBitset: Map<ExpressionAtom, BitSet>) : BaseExpressionVisitor<BitSet>() {
    override fun visitAtom(atom: ExpressionAtom): BitSet {
        return requireNotNull(atomsToBitset[atom]) {"Unable to find CSV lines for atom" }
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