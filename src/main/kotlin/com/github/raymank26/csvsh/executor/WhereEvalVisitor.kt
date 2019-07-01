package com.github.raymank26.csvsh.executor

import com.github.raymank26.csvsh.BaseExpressionVisitor
import com.github.raymank26.csvsh.DatasetRow
import com.github.raymank26.csvsh.ExpressionAtom
import com.github.raymank26.csvsh.ExpressionNode
import com.github.raymank26.csvsh.ExpressionOperator
import com.github.raymank26.csvsh.ListValue
import com.github.raymank26.csvsh.Operator.EQ
import com.github.raymank26.csvsh.Operator.GREATER_EQ_THAN
import com.github.raymank26.csvsh.Operator.GREATER_THAN
import com.github.raymank26.csvsh.Operator.IN
import com.github.raymank26.csvsh.Operator.LESS_EQ_THAN
import com.github.raymank26.csvsh.Operator.LESS_THAN
import com.github.raymank26.csvsh.Operator.LIKE
import com.github.raymank26.csvsh.Operator.NOT_EQ
import com.github.raymank26.csvsh.Operator.NOT_IN
import com.github.raymank26.csvsh.Operator.NOT_LIKE
import com.github.raymank26.csvsh.RefValue
import com.github.raymank26.csvsh.SqlValue
import com.github.raymank26.csvsh.SqlValueAtom
import com.github.raymank26.csvsh.StringValue
import com.github.raymank26.csvsh.eq
import com.github.raymank26.csvsh.gt
import com.github.raymank26.csvsh.gte
import com.github.raymank26.csvsh.lt
import com.github.raymank26.csvsh.lte
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

    private fun checkAtom(atom: ExpressionAtom, dataColumn: SqlValueAtom, sqlValue: SqlValue?): Boolean {
        if (sqlValue == null) {
            return when (atom.operator) {
                EQ -> dataColumn.asValue == null
                NOT_EQ -> dataColumn.asValue != null
                else -> false
            }
        }
        return when (atom.operator) {
            LESS_THAN -> dataColumn lt (sqlValue as SqlValueAtom)
            LESS_EQ_THAN -> dataColumn lte (sqlValue as SqlValueAtom)
            GREATER_THAN -> dataColumn gt (sqlValue as SqlValueAtom)
            GREATER_EQ_THAN -> dataColumn gte (sqlValue as SqlValueAtom)
            EQ -> dataColumn eq (sqlValue as SqlValueAtom)
            IN -> checkIn(dataColumn, sqlValue)
            NOT_IN -> !checkIn(dataColumn, sqlValue)
            LIKE -> checkLike(dataColumn, sqlValue)
            NOT_LIKE -> !checkLike(dataColumn, sqlValue)
            NOT_EQ -> !(dataColumn eq (sqlValue as SqlValueAtom))
        }
    }

    private fun checkIn(dataColumn: SqlValueAtom, sqlValue: SqlValue) =
            (sqlValue as ListValue).value.any { it eq dataColumn }

    private fun checkLike(dataColumn: SqlValueAtom, sqlValue: SqlValue): Boolean {
        val stringContent = (dataColumn as StringValue).value
                ?: return false
        // TODO: actually, SQL pattern matching is a great deal more complicated.
        val regExValue = (sqlValue as StringValue).value!!.replace("%", ".*").replace('%', '?')
        return Pattern.compile(regExValue).toRegex().matches(stringContent)
    }
}