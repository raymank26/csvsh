package com.github.raymank26

/**
 * Date: 2019-05-13.
 */
data class IndexDescription(val name: String, val fieldName: String)

data class IndexDescriptionAndPath(val description: IndexDescription, val indexContent: ReadOnlyIndex)

sealed class ScanSource
object CsvInput : ScanSource()
data class IndexInput(val name: String) : ScanSource()

enum class FieldType(val mark: Byte) {
    INTEGER(1),
    FLOAT(2),
    STRING(3)
    ;

    companion object {
        val MARK_TO_FIELD_TYPE: Map<Byte, FieldType> = values().associateBy { it.mark }
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

    protected open fun default(): T? {
        return null
    }

    protected open fun combine(left: T?, right: T?): T? {
        return right
    }
}


interface SqlValueAtom : Comparable<SqlValueAtom> {
    val type: FieldType
    val asValue: Any?
    fun toText(): String
}

private fun SqlValueAtom.asInt(): Int {
    val v = asValue
    requireNotNull(v)
    return v as? Int ?: throw RuntimeException("Value of type ${v.javaClass} is not Int")
}

private fun SqlValueAtom.asFloat(): Float {
    val v = asValue
    requireNotNull(v)
    return v as? Float ?: throw RuntimeException("Value of type ${v.javaClass} is not Float")
}

private fun SqlValueAtom.asString(): String {
    val v = asValue
    requireNotNull(v)
    return v as? String ?: throw RuntimeException("Value of type ${v.javaClass} is not String")
}

infix fun SqlValueAtom.eq(other: SqlValueAtom): Boolean = asValue == other.asValue

infix fun SqlValueAtom.plus(other: SqlValueAtom): SqlValueAtom {
    return when {
        asValue != null && other.asValue == null -> this
        asValue == null -> other
        type == FieldType.INTEGER -> IntValue(asInt() + other.asInt())
        type == FieldType.FLOAT -> FloatValue(asFloat() + other.asFloat())
        type == FieldType.STRING -> StringValue(asString() + other.asString())
        else -> throw IllegalStateException("Case is not handled")
    }
}

infix fun SqlValueAtom.lt(other: SqlValueAtom): Boolean {
    return when {
        asValue == null || other.asValue == null -> false
        type == FieldType.INTEGER -> asInt() < other.asInt()
        type == FieldType.FLOAT -> asFloat() < other.asFloat()
        type == FieldType.STRING -> asString() < other.asString()
        else -> throw IllegalStateException("Case is not handled")
    }
}

infix fun SqlValueAtom.lte(other: SqlValueAtom): Boolean {
    return when {
        asValue == null || other.asValue == null -> false
        type == FieldType.INTEGER -> asInt() <= other.asFloat()
        type == FieldType.FLOAT -> asFloat() <= other.asFloat()
        type == FieldType.STRING -> asString() <= other.asString()
        else -> throw IllegalStateException("Case is not handled")
    }
}

infix fun SqlValueAtom.gt(other: SqlValueAtom): Boolean {
    return when {
        asValue == null || other.asValue == null -> false
        type == FieldType.INTEGER -> asInt() > other.asInt()
        type == FieldType.FLOAT -> asFloat() > other.asFloat()
        type == FieldType.STRING -> asString() > other.asString()
        else -> throw IllegalStateException("Case is not handled")
    }
}

infix fun SqlValueAtom.gte(other: SqlValueAtom): Boolean {
    return when {
        asValue == null || other.asValue == null -> false
        type == FieldType.INTEGER -> asInt() >= other.asInt()
        type == FieldType.FLOAT -> asFloat() >= other.asFloat()
        type == FieldType.STRING -> asString() >= other.asString()
        else -> throw IllegalStateException("Case is not handled")
    }
}

fun SqlValueAtom.max(other: SqlValueAtom): SqlValueAtom {
    return when {
        asValue != null && other.asValue == null -> this
        asValue == null -> other
        type == FieldType.INTEGER -> IntValue(Math.max(asInt(), other.asInt()))
        type == FieldType.FLOAT -> FloatValue(Math.max(asFloat(), other.asFloat()))
        type == FieldType.STRING -> if (asString() > other.asString()) this else other
        else -> throw IllegalStateException("Case is not handled")
    }
}

fun SqlValueAtom.min(other: SqlValueAtom): SqlValueAtom {
    return when {
        asValue != null && other.asValue == null -> this
        asValue == null -> other
        type == FieldType.INTEGER -> IntValue(Math.min(asInt(), other.asInt()))
        type == FieldType.FLOAT -> FloatValue(Math.min(asFloat(), other.asFloat()))
        type == FieldType.STRING -> if (asString() < other.asString()) this else other
        else -> throw IllegalStateException("Case is not handled")
    }
}

fun <T : Comparable<T>> compareNullable(first: SqlValueAtom, second: SqlValueAtom, toType: (SqlValueAtom) -> T): Int {
    return when {
        first.asValue == null -> -1
        second.asValue == null -> 1
        else -> toType(first).compareTo(toType(second))
    }
}

sealed class SqlValue

data class RefValue(val name: String) : SqlValue()

data class IntValue(val value: Int?) : SqlValue(), SqlValueAtom {
    override val type: FieldType = FieldType.INTEGER
    override val asValue: Any? = value

    override fun compareTo(other: SqlValueAtom): Int {
        return compareNullable(this, other) { it.asInt() }
    }

    override fun toText(): String {
        return value.toString()
    }
}

data class FloatValue(val value: Float?) : SqlValue(), SqlValueAtom {
    override val type: FieldType = FieldType.FLOAT
    override val asValue: Any? = value

    override fun compareTo(other: SqlValueAtom): Int {
        return compareNullable(this, other) { it.asFloat() }
    }

    override fun toText(): String {
        return value.toString()
    }
}

data class StringValue(val value: String?) : SqlValue(), SqlValueAtom {
    override val type: FieldType = FieldType.STRING
    override val asValue: Any? = value

    override fun compareTo(other: SqlValueAtom): Int {
        return compareNullable(this, other) { it.asString() }
    }

    override fun toText(): String {
        return value ?: "null"
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




