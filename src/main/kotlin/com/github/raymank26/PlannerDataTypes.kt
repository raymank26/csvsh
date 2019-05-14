package com.github.raymank26

import java.nio.file.Path

/**
 * Date: 2019-05-13.
 */
data class IndexDescription(val name: String, val fieldName: String)

data class IndexDescriptionAndPath(val description: IndexDescription, val path: Path)

sealed class ScanSource
object CsvInput: ScanSource()
data class IndexInput(val name: String): ScanSource()

enum class Operator(val token: String) {
    LESS_THAN("<"),
    GREATER_THAN(">"),
    LIKE("LIKE"),
    IN("IN"),
    EQ("=")
    ;

    companion object {
        val TOKEN_TO_OPERATOR: Map<String, Operator>  = values().associateBy { it.token }
    }
}

enum class ExpressionOperator {
    AND,
    OR
    ;
}

sealed class Expression {
    abstract fun <T> accept(visitor: BaseExpressionVisitor<T>): T?
}
data class ExpressionAtom(val leftVal: SqlValue, val operator: Operator, val rightVal: SqlValue) : Expression() {
    override fun <T> accept(visitor: BaseExpressionVisitor<T>): T? {
        return visitor.visitAtom(this)
    }
}
data class ExpressionNode(val left: Expression, val operator: ExpressionOperator, val right: Expression): Expression() {
    override fun <T> accept(visitor: BaseExpressionVisitor<T>): T? {
        return visitor.visitNode(this)
    }
}

class BaseExpressionVisitor<T> {

    fun visitAtom(atom: ExpressionAtom): T? {
        return default()
    }

    fun visitNode(node: ExpressionNode): T? {
        return combine(node.left.accept(this), node.right.accept(this))
    }

    internal fun default(): T? {
        return null
    }

    internal fun combine(left: T?, right: T?): T? {
        return right
    }
}

sealed class SqlValue
data class RefValue(val name: String): SqlValue()
data class IntValue(val value: Int): SqlValue()
data class FloatValue(val value: Float): SqlValue()
data class StringValue(val value: String): SqlValue()
data class ListValue(val value: List<SqlValue>): SqlValue()

class PlannerException(msg: String) : Exception(msg)
class SyntaxException(msg: String) : Exception(msg)

data class PlanDescription(
        val expressionsBySource: Map<ScanSource, List<ExpressionAtom>>,
        val expressionTree: Expression
)

data class EngineContext(val currentDirectory: String)



