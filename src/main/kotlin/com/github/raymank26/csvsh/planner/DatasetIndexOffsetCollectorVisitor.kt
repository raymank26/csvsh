package com.github.raymank26.csvsh.planner

import com.github.raymank26.csvsh.BaseExpressionVisitor
import com.github.raymank26.csvsh.ExpressionAtom
import com.github.raymank26.csvsh.ExpressionNode
import com.github.raymank26.csvsh.ExpressionOperator
import com.github.raymank26.csvsh.ListValue
import com.github.raymank26.csvsh.Operator
import com.github.raymank26.csvsh.RefValue
import com.github.raymank26.csvsh.SqlValueAtom
import com.github.raymank26.csvsh.index.IndexDescriptionAndPath
import com.github.raymank26.csvsh.index.ReadOnlyIndex

class DatasetIndexOffsetCollectorVisitor(indexes: List<IndexDescriptionAndPath>) : BaseExpressionVisitor<IndexEvaluator?>() {

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