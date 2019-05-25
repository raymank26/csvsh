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


interface SqlValueAtom : Comparable<SqlValueAtom> {
    val type: FieldType
    val asValue: Any
    fun toText(): String
}

private fun SqlValueAtom.asInt(): Int {
    return asValue as? Int ?: throw RuntimeException("Value of type ${asValue.javaClass} is not Int")
}

private fun SqlValueAtom.asFloat(): Float {
    return asValue as? Float ?: throw RuntimeException("Value of type ${asValue.javaClass} is not Float")
}

private fun SqlValueAtom.asString(): String {
    return asValue as? String ?: throw RuntimeException("Value of type ${asValue.javaClass} is not String")
}

infix fun SqlValueAtom.eq(other: SqlValueAtom): Boolean = asValue == other.asValue

infix fun SqlValueAtom.plus(other: SqlValueAtom): SqlValueAtom {
    return when (type) {
        FieldType.INTEGER -> IntValue(asInt() + other.asInt())
        FieldType.FLOAT -> FloatValue(asFloat() + other.asFloat())
        FieldType.STRING -> StringValue(asString() + other.asString())
    }
}

infix fun SqlValueAtom.lt(other: SqlValueAtom): Boolean {
    return when (type) {
        FieldType.INTEGER -> asInt() < other.asInt()
        FieldType.FLOAT -> asFloat() < other.asFloat()
        FieldType.STRING -> asString() < other.asString()
    }
}

infix fun SqlValueAtom.lte(other: SqlValueAtom): Boolean {
    return when (type) {
        FieldType.INTEGER -> asInt() <= other.asFloat()
        FieldType.FLOAT -> asFloat() <= other.asFloat()
        FieldType.STRING -> asString() <= other.asString()
    }
}

infix fun SqlValueAtom.gt(other: SqlValueAtom): Boolean {
    return when (type) {
        FieldType.INTEGER -> asInt() > other.asInt()
        FieldType.FLOAT -> asFloat() > other.asFloat()
        FieldType.STRING -> asString() > other.asString()
    }
}

infix fun SqlValueAtom.gte(other: SqlValueAtom): Boolean {
    return when (type) {
        FieldType.INTEGER -> asInt() >= other.asInt()
        FieldType.FLOAT -> asFloat() >= other.asFloat()
        FieldType.STRING -> asString() >= other.asString()
    }
}

fun SqlValueAtom.max(other: SqlValueAtom): SqlValueAtom {
    return when (type) {
        FieldType.INTEGER -> IntValue(Math.max(asInt(), other.asInt()))
        FieldType.FLOAT -> FloatValue(Math.max(asFloat(), other.asFloat()))
        FieldType.STRING -> throw NotImplementedError("Operation 'plus' is not implemented for string type")
    }
}

fun SqlValueAtom.min(other: SqlValueAtom): SqlValueAtom {
    return when (type) {
        FieldType.INTEGER -> IntValue(Math.min(asInt(), other.asInt()))
        FieldType.FLOAT -> FloatValue(Math.min(asFloat(), other.asFloat()))
        FieldType.STRING -> throw NotImplementedError("Operation 'plus' is not implemented for string type")
    }
}

sealed class SqlValue

data class RefValue(val name: String) : SqlValue()

data class IntValue(val value: Int) : SqlValue(), SqlValueAtom {
    override val type: FieldType = FieldType.INTEGER
    override val asValue: Any = value

    override fun compareTo(other: SqlValueAtom): Int {
        return value.compareTo(other.asInt())
    }

    override fun toText(): String {
        return value.toString()
    }
}

data class FloatValue(val value: Float) : SqlValue(), SqlValueAtom {
    override val type: FieldType = FieldType.FLOAT
    override val asValue: Any = value

    override fun compareTo(other: SqlValueAtom): Int {
        return value.compareTo(other.asFloat())
    }

    override fun toText(): String {
        return value.toString()
    }
}

data class StringValue(val value: String) : SqlValue(), SqlValueAtom {
    override val type: FieldType = FieldType.STRING
    override val asValue: Any = value

    override fun compareTo(other: SqlValueAtom): Int {
        return value.compareTo(other.asString())
    }

    override fun toText(): String {
        return value
    }
}

data class ListValue(val value: List<SqlValueAtom>) : SqlValue()

class PlannerException(msg: String) : Exception(msg)
class SyntaxException(msg: String) : Exception(msg)
class ExecutorException(msg: String) : Exception(msg)

sealed class SelectStatementExpr {
    abstract val fullFieldName: String
}

data class AggSelectExpr(val type: String, val fieldName: String) : SelectStatementExpr() {
    override val fullFieldName = "$type($fieldName)"
}

data class SelectFieldExpr(val fieldName: String) : SelectStatementExpr() {
    override val fullFieldName: String = fieldName
}

data class SqlPlan(
        val selectStatements: List<SelectStatementExpr>,
        val datasetReader: DatasetReader,
        val wherePlanDescription: WherePlanDescription?,
        val groupByFields: List<String>,
        val orderByPlanDescription: OrderByPlanDescription?,
        val limit: Int?
)

data class OrderByPlanDescription(
        val field: SelectStatementExpr,
        val desc: Boolean
)

data class WherePlanDescription(
        val expressionsBySource: Map<ScanSource, List<ExpressionAtom>>,
        val expressionTree: Expression
)




