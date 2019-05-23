package com.github.raymank26

/**
 * Date: 2019-05-13.
 */
data class IndexDescription(val name: String, val fieldName: String)

data class IndexDescriptionAndPath(val description: IndexDescription, val indexContent: ReadOnlyIndex<Any>)

sealed class ScanSource
object CsvInput : ScanSource()
data class IndexInput(val name: String) : ScanSource()

enum class FieldType(val mark: Byte) {
    INTEGER(1),
    FLOAT(2),
    STRING(3)
    ;

    companion object {
        val MARK_TO_FIELD_TYPE = values().associateBy { it.mark }
    }
}

enum class Operator(val token: String) {
    LESS_THAN("<"),
    LESS_EQ_THAN("<="),
    GREATER_THAN(">"),
    GREATER_EQ_THAN(">="),
    LIKE("LIKE"),
    IN("IN"),
    EQ("=")
    ;

    companion object {
        val TOKEN_TO_OPERATOR: Map<String, Operator> = values().associateBy { it.token }
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

data class ExpressionNode(val left: Expression, val operator: ExpressionOperator, val right: Expression) : Expression() {
    override fun <T> accept(visitor: BaseExpressionVisitor<T>): T? {
        return visitor.visitNode(this)
    }
}

open class BaseExpressionVisitor<T> {

    fun visitExpression(tree: Expression): T? {
        return tree.accept(this)
    }

    open fun visitAtom(atom: ExpressionAtom): T? {
        return default()
    }

    open fun visitNode(node: ExpressionNode): T? {
        return combine(node.left.accept(this), node.right.accept(this))
    }

    protected fun default(): T? {
        return null
    }

    protected fun combine(left: T?, right: T?): T? {
        return right
    }
}

sealed class SqlValue
data class RefValue(val name: String) : SqlValue()
data class IntValue(val value: Int) : SqlValue()
data class FloatValue(val value: Float) : SqlValue()
data class StringValue(val value: String) : SqlValue()
data class ListValue(val value: List<SqlValue>) : SqlValue()

class PlannerException(msg: String) : Exception(msg)
class SyntaxException(msg: String) : Exception(msg)
class ExecutorException(msg: String) : Exception(msg)

sealed class SelectStatementExpr

data class AggSelectExpr(val type: String, val fieldName: String) : SelectStatementExpr()
data class SelectFieldExpr(val fieldName: String) : SelectStatementExpr()

data class SqlPlan(
        val selectStatements: List<SelectStatementExpr>,
        val datasetReader: DatasetReader,
        val wherePlanDescription: WherePlanDescription?,
        val groupByFields: List<String>,
        val orderByPlanDescription: OrderByPlanDescription?,
        val limit: Int?
)

data class OrderByPlanDescription(
        val field: String,
        val desc: Boolean
)

data class WherePlanDescription(
        val expressionsBySource: Map<ScanSource, List<ExpressionAtom>>,
        val expressionTree: Expression
)

data class EngineContext(val fieldToIndex: Map<String, ReadOnlyIndex<*>>
)




