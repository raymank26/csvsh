package com.github.raymank26.executor

import com.github.raymank26.BaseExpressionVisitor
import com.github.raymank26.DatasetRow
import com.github.raymank26.ExpressionAtom
import com.github.raymank26.ExpressionNode
import com.github.raymank26.ExpressionOperator
import com.github.raymank26.ListValue
import com.github.raymank26.Operator
import com.github.raymank26.RefValue
import com.github.raymank26.SqlValue
import com.github.raymank26.SqlValueAtom
import com.github.raymank26.StringValue
import com.github.raymank26.eq
import com.github.raymank26.gt
import com.github.raymank26.gte
import com.github.raymank26.lt
import com.github.raymank26.lte
import java.util.regex.Pattern

class WhereEvalVisitor(private val datasetRow: DatasetRow) : BaseExpressionVisitor<Boolean>() {
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