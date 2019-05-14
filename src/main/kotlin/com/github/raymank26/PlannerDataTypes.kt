package com.github.raymank26

/**
 * Date: 2019-05-13.
 */
data class IndexDescription(val name: String, val fieldName: String)

sealed class ScanSource
object CsvInput: ScanSource()
data class IndexInput(val name: String): ScanSource()

enum class Operator(val token: String) {
    LESS_THAN("<"),
    GREATER_THAN(">"),
    LIKE("LIKE"),
    IN("IN"),
    EQ("=");

    companion object {
        val TOKEN_TO_OPERATOR: Map<String, Operator>  = values().associateBy { it.token }
    }
}

data class Expression(val leftVal: SqlValue, val operator: Operator, val rightVal: SqlValue)

sealed class SqlValue
data class RefValue(val name: String): SqlValue()
data class IntValue(val value: Int): SqlValue()
data class FloatValue(val value: Float): SqlValue()
data class StringValue(val value: String): SqlValue()
data class ListValue(val value: List<SqlValue>): SqlValue()

class PlannerException(msg: String) : Exception(msg)
class SyntaxException(msg: String) : Exception(msg)



