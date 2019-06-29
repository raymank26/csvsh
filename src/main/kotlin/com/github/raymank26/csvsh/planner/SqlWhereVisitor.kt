package com.github.raymank26.csvsh.planner

import com.github.raymank26.csvsh.DoubleValue
import com.github.raymank26.csvsh.ExpressionAtom
import com.github.raymank26.csvsh.ExpressionNode
import com.github.raymank26.csvsh.ExpressionOperator
import com.github.raymank26.csvsh.FieldType
import com.github.raymank26.csvsh.ListValue
import com.github.raymank26.csvsh.LongValue
import com.github.raymank26.csvsh.Operator
import com.github.raymank26.csvsh.RefValue
import com.github.raymank26.csvsh.SqlValue
import com.github.raymank26.csvsh.SqlValueAtom
import com.github.raymank26.csvsh.StringValue
import com.github.raymank26.sql.SqlBaseVisitor
import com.github.raymank26.sql.SqlParser

class SqlWhereVisitor : SqlBaseVisitor<WherePlanDescription?>() {

    override fun visitWhereExprAtom(ctx: SqlParser.WhereExprAtomContext): WherePlanDescription {
        var left = parseVariable(ctx.variable(0))
        var right = parseVariable(ctx.variable(1))
        var operator = Operator.TOKEN_TO_OPERATOR[ctx.BOOL_COMP().text]
                ?: throw RuntimeException("Unable to parse operator ${ctx.BOOL_COMP().text}")

        if (right is RefValue && left !is RefValue) {
            val temp: SqlValue = left
            left = right
            right = temp
            operator = when (operator) {
                Operator.LESS_THAN -> Operator.GREATER_THAN
                Operator.GREATER_THAN -> Operator.LESS_THAN
                Operator.LIKE -> Operator.LIKE
                Operator.IN -> Operator.IN
                Operator.EQ -> Operator.EQ
                Operator.LESS_EQ_THAN -> Operator.GREATER_EQ_THAN
                Operator.GREATER_EQ_THAN -> Operator.LESS_EQ_THAN
            }
        }
        return WherePlanDescription(ExpressionAtom(left, operator, right))
    }

    override fun visitWhereExprIn(ctx: SqlParser.WhereExprInContext): WherePlanDescription {
        val field: RefValue = parseVariable(ctx.variable(0)) as? RefValue
                ?: throw PlannerException("Left variable has to be table field")

        val others = ctx.variable().drop(1)
        var type: FieldType? = null
        val variables: MutableList<SqlValueAtom> = mutableListOf()
        for (variable in others) {
            val parsedVariable = parseVariable(variable) as? SqlValueAtom
                    ?: throw PlannerException("Lists have to hold only non ref expressions")
            if (type == null) {
                type = parsedVariable.type
            } else if (type != parsedVariable.type) {
                throw RuntimeException("IN clause has different types")
            }
            variables.add(parsedVariable)
        }
        val atom = ExpressionAtom(field, Operator.IN, ListValue(variables))
        return WherePlanDescription(atom)
    }

    override fun visitWhereExprBool(ctx: SqlParser.WhereExprBoolContext): WherePlanDescription {
        if (ctx.childCount != 3) {
            throw RuntimeException("Child count != 3")
        }
        val left: WherePlanDescription? = visit(ctx.getChild(0))
        val right: WherePlanDescription? = visit(ctx.getChild(2))
        if (left == null || right == null) {
            throw RuntimeException("Some null")
        }
        val operator = when {
            ctx.AND() != null -> ExpressionOperator.AND
            ctx.OR() != null -> ExpressionOperator.OR
            else -> throw RuntimeException("Unable to parse operator expression")
        }
        val expression = ExpressionNode(left.expressionTree, operator, right.expressionTree)
        return WherePlanDescription(expression)
    }

    override fun aggregateResult(aggregate: WherePlanDescription?, nextResult: WherePlanDescription?): WherePlanDescription? {
        return when {
            aggregate != null -> aggregate
            nextResult != null -> nextResult
            else -> null
        }
    }

    private fun parseVariable(variable: SqlParser.VariableContext): SqlValue {
        return when (variable.type) {
            "float" -> DoubleValue(variable.floatNumber().text.toDouble())
            "integer" -> LongValue(variable.integerNumber().text.toLong())
            "string" -> StringValue(variable.string().text.drop(1).dropLast(1))
            "reference" -> RefValue(variable.reference().text)
            else -> throw RuntimeException("Unable to parse type = ${variable.type}")
        }
    }
}