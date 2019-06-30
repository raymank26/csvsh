package com.github.raymank26.csvsh.executor

import com.github.raymank26.csvsh.DoubleValue
import com.github.raymank26.csvsh.LongValue
import com.github.raymank26.csvsh.SqlValueAtom
import com.github.raymank26.csvsh.StringValue
import com.github.raymank26.csvsh.max
import com.github.raymank26.csvsh.min
import com.github.raymank26.csvsh.plus

class AggregateFunction(private var initial: SqlValueAtom, private val agg: (SqlValueAtom, SqlValueAtom) -> SqlValueAtom) {
    fun process(value: SqlValueAtom) {
        initial = agg.invoke(initial, value)
    }

    fun getResult(): SqlValueAtom {
        return initial
    }
}

typealias AggregateFunctionFactory = () -> AggregateFunction

object Aggregates {
    val SUM_INT: AggregateFunctionFactory = { AggregateFunction(LongValue(0), SqlValueAtom::plus) }
    val SUM_FLOAT: AggregateFunctionFactory = { AggregateFunction(DoubleValue(0.toDouble()), SqlValueAtom::plus) }
    val COUNT_ANY: AggregateFunctionFactory = { AggregateFunction(LongValue(0)) { a, _ -> a plus LongValue(1) } }
    val MAX_INT: AggregateFunctionFactory = { AggregateFunction(LongValue(null), SqlValueAtom::max) }
    val MIN_INT: AggregateFunctionFactory = { AggregateFunction(LongValue(null), SqlValueAtom::min) }
    val MAX_FLOAT: AggregateFunctionFactory = { AggregateFunction(DoubleValue(null), SqlValueAtom::max) }
    val MIN_FLOAT: AggregateFunctionFactory = { AggregateFunction(DoubleValue(null), SqlValueAtom::min) }
    val MAX_STRING: AggregateFunctionFactory = { AggregateFunction(StringValue(null), SqlValueAtom::max) }
    val MIN_STRING: AggregateFunctionFactory = { AggregateFunction(StringValue(null), SqlValueAtom::min) }
}