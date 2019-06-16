package com.github.raymank26

/**
 * Date: 2019-05-13.
 */
data class IndexDescription(val name: String, val fieldName: String)

data class IndexDescriptionAndPath(val description: IndexDescription, val indexContent: ReadOnlyIndex)

enum class FieldType(val mark: Byte) {
    LONG(1),
    DOUBLE(2),
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
    abstract fun <T> accept(visitor: BaseExpressionVisitor<T>): T
}

data class ExpressionAtom(val leftVal: SqlValue, val operator: Operator, val rightVal: SqlValue) : Expression() {
    override fun <T> accept(visitor: BaseExpressionVisitor<T>): T {
        return visitor.visitAtom(this)
    }
}

data class ExpressionNode(val left: Expression, val operator: ExpressionOperator, val right: Expression) : Expression() {
    override fun <T> accept(visitor: BaseExpressionVisitor<T>): T {
        return visitor.visitNode(this)
    }
}

open class BaseExpressionVisitor<T> {

    fun visitExpression(tree: Expression): T {
        return tree.accept(this)
    }

    open fun visitAtom(atom: ExpressionAtom): T {
        return atom.accept(this)
    }

    open fun visitNode(node: ExpressionNode): T {
        return combine(node.left.accept(this), node.right.accept(this))
    }

    protected open fun combine(left: T, right: T): T {
        return right
    }
}


interface SqlValueAtom : Comparable<SqlValueAtom> {
    val type: FieldType
    val asValue: Any?
    fun toText(): String
}

private fun SqlValueAtom.asLong(): Long {
    val v = asValue
    requireNotNull(v)
    return v as? Long ?: throw RuntimeException("Value of type ${v.javaClass} is not Int")
}

private fun SqlValueAtom.asDouble(): Double {
    val v = asValue
    requireNotNull(v)
    return v as? Double ?: throw RuntimeException("Value of type ${v.javaClass} is not Float")
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
        type == FieldType.LONG -> LongValue(asLong() + other.asLong())
        type == FieldType.DOUBLE -> DoubleValue(asDouble() + other.asDouble())
        type == FieldType.STRING -> StringValue(asString() + other.asString())
        else -> throw IllegalStateException("Case is not handled")
    }
}

infix fun SqlValueAtom.lt(other: SqlValueAtom): Boolean {
    return when {
        asValue == null || other.asValue == null -> false
        type == FieldType.LONG -> asLong() < other.asLong()
        type == FieldType.DOUBLE -> asDouble() < other.asDouble()
        type == FieldType.STRING -> asString() < other.asString()
        else -> throw IllegalStateException("Case is not handled")
    }
}

infix fun SqlValueAtom.lte(other: SqlValueAtom): Boolean {
    return when {
        asValue == null || other.asValue == null -> false
        type == FieldType.LONG -> asLong() <= other.asLong()
        type == FieldType.DOUBLE -> asDouble() <= other.asDouble()
        type == FieldType.STRING -> asString() <= other.asString()
        else -> throw IllegalStateException("Case is not handled")
    }
}

infix fun SqlValueAtom.gt(other: SqlValueAtom): Boolean {
    return when {
        asValue == null || other.asValue == null -> false
        type == FieldType.LONG -> asLong() > other.asLong()
        type == FieldType.DOUBLE -> asDouble() > other.asDouble()
        type == FieldType.STRING -> asString() > other.asString()
        else -> throw IllegalStateException("Case is not handled")
    }
}

infix fun SqlValueAtom.gte(other: SqlValueAtom): Boolean {
    return when {
        asValue == null || other.asValue == null -> false
        type == FieldType.LONG -> asLong() >= other.asLong()
        type == FieldType.DOUBLE -> asDouble() >= other.asDouble()
        type == FieldType.STRING -> asString() >= other.asString()
        else -> throw IllegalStateException("Case is not handled")
    }
}

fun SqlValueAtom.max(other: SqlValueAtom): SqlValueAtom {
    return when {
        asValue != null && other.asValue == null -> this
        asValue == null -> other
        type == FieldType.LONG -> LongValue(Math.max(asLong(), other.asLong()))
        type == FieldType.DOUBLE -> DoubleValue(Math.max(asDouble(), other.asDouble()))
        type == FieldType.STRING -> if (asString() > other.asString()) this else other
        else -> throw IllegalStateException("Case is not handled")
    }
}

fun SqlValueAtom.min(other: SqlValueAtom): SqlValueAtom {
    return when {
        asValue != null && other.asValue == null -> this
        asValue == null -> other
        type == FieldType.LONG -> LongValue(Math.min(asLong(), other.asLong()))
        type == FieldType.DOUBLE -> DoubleValue(Math.min(asDouble(), other.asDouble()))
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

data class LongValue(val value: Long?) : SqlValue(), SqlValueAtom {
    override val type: FieldType = FieldType.LONG
    override val asValue: Any? = value

    override fun compareTo(other: SqlValueAtom): Int {
        return compareNullable(this, other) { it.asLong() }
    }

    override fun toText(): String {
        return value.toString()
    }
}

data class DoubleValue(val value: Double?) : SqlValue(), SqlValueAtom {
    override val type: FieldType = FieldType.DOUBLE
    override val asValue: Any? = value

    override fun compareTo(other: SqlValueAtom): Int {
        return compareNullable(this, other) { it.asDouble() }
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

data class WherePlanDescription(val expressionTree: Expression)




