package com.github.raymank26

import org.apache.commons.csv.CSVParser
import java.io.BufferedReader
import java.io.FileReader
import java.util.BitSet

/**
 * Date: 2019-05-15.
 */
class SqlExecutor {

    fun execute(engineContext: EngineContext, planDescription: PlanDescription?): CsvContent {
        if (planDescription == null) {
            return readRows(engineContext, null)
        }
        val expressionToLines = executeAtoms(planDescription.expressionsBySource)
        val resultLines = ExpressionEvaluator(expressionToLines).visitExpression(planDescription.expressionTree)
        return readRows(engineContext, resultLines)
    }

    private fun executeAtoms(expressionsBySource: Map<ScanSource, List<ExpressionAtom>>): Map<ExpressionAtom, BitSet> {
        return TODO()
    }

    private fun readRows(engineContext: EngineContext, rowIndexes: BitSet?): CsvContent {
        return CSVParser(BufferedReader(FileReader(engineContext.csvFileDescriptor)), engineContext.csvFormat).use {
            val linesIterator = it.iterator()
            linesIterator.next() // skip header

            val rows = mutableListOf<CsvRow>()
            while (linesIterator.hasNext()) {
                val csvLine = linesIterator.next()
                if (rowIndexes == null || rowIndexes[csvLine.recordNumber.toInt()]) {
                    val columns = mutableListOf<String>()
                    for (header in engineContext.csvHeader) {
                        val rowValue = csvLine.get(header)
                        columns.add(rowValue)
                    }
                    rows.add(CsvRow(columns))
                }
            }
            CsvContent(engineContext.csvHeader, rows)
        }
    }
}

class ExpressionEvaluator(private val atomsToBitset: Map<ExpressionAtom, BitSet>) : BaseExpressionVisitor<BitSet>() {
    override fun visitAtom(atom: ExpressionAtom): BitSet? {
        return atomsToBitset[atom] ?: throw RuntimeException("Unable to find CSV lines for atom")
    }

    override fun visitNode(node: ExpressionNode): BitSet? {
        val leftLines = requireNotNull(node.left.accept(this), { "Left lines is null" })
        val rightLines = requireNotNull(node.right.accept(this), { "Right lines is null" })

        when (node.operator) {
            ExpressionOperator.AND -> leftLines.and(rightLines)
            ExpressionOperator.OR -> leftLines.or(rightLines)
        }
        return leftLines
    }
}