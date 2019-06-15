package com.github.raymank26

import org.junit.Test
import kotlin.test.assertEquals

/**
 * Date: 2019-06-13.
 */
class AggregationFunctionsTest {

    @Test
    fun testSumInt() {
        testAggregate(Aggregates.SUM_INT.invoke(), listOf(LongValue(null), LongValue(0), LongValue(1), LongValue(2)), 3L)
        testAggregate(Aggregates.SUM_INT.invoke(), listOf(LongValue(null)), 0L)
    }

    @Test
    fun testSumFloat() {
        testAggregate(Aggregates.SUM_FLOAT.invoke(), listOf(DoubleValue(null), DoubleValue(0.0), DoubleValue(1.2),
                DoubleValue(2.3)), 3.5)
        testAggregate(Aggregates.SUM_FLOAT.invoke(), listOf(DoubleValue(null)), 0.0)
    }

    @Test
    fun testCount() {
        testAggregate(Aggregates.COUNT_ANY.invoke(), listOf(DoubleValue(null), DoubleValue(0.0), DoubleValue(1.2),
                DoubleValue(2.3)), 4L)
    }

    @Test
    fun testMinInt() {
        testAggregate(Aggregates.MIN_INT.invoke(), listOf(LongValue(null), LongValue(0), LongValue(1), LongValue(2)), 0L)
        testAggregate(Aggregates.MIN_INT.invoke(), listOf(LongValue(null), LongValue(-1), LongValue(1), LongValue(2)), -1L)
        testAggregate(Aggregates.MIN_INT.invoke(), listOf(LongValue(null)), null)
    }

    @Test
    fun testMaxInt() {
        testAggregate(Aggregates.MAX_INT.invoke(), listOf(LongValue(null), LongValue(0), LongValue(1), LongValue(2)), 2L)
        testAggregate(Aggregates.MAX_INT.invoke(), listOf(LongValue(null), LongValue(-1), LongValue(1), LongValue(2)), 2L)
        testAggregate(Aggregates.MAX_INT.invoke(), listOf(LongValue(null)), null)
        testAggregate(Aggregates.MAX_INT.invoke(), listOf(LongValue(-2), LongValue(-1), LongValue(null)), -1L)
    }

    @Test
    fun testMinFloat() {
        testAggregate(Aggregates.MIN_FLOAT.invoke(), listOf(DoubleValue(null), DoubleValue(0.0), DoubleValue(1.2), DoubleValue(2.8)), 0.0)
        testAggregate(Aggregates.MIN_FLOAT.invoke(), listOf(DoubleValue(null), DoubleValue(-1.8), DoubleValue(-1.9), DoubleValue(-2.2)), -2.2)
        testAggregate(Aggregates.MIN_FLOAT.invoke(), listOf(DoubleValue(null)), null)
    }

    @Test
    fun testMaxFloat() {
        testAggregate(Aggregates.MAX_FLOAT.invoke(), listOf(DoubleValue(null), DoubleValue(0.0), DoubleValue(1.2), DoubleValue(2.8)), 2.8)
        testAggregate(Aggregates.MAX_FLOAT.invoke(), listOf(DoubleValue(null), DoubleValue(-1.8), DoubleValue(-1.9), DoubleValue(-2.2)), -1.8)
        testAggregate(Aggregates.MAX_FLOAT.invoke(), listOf(DoubleValue(null)), null)
        testAggregate(Aggregates.MAX_FLOAT.invoke(), listOf(DoubleValue(-2.0), DoubleValue(-1.0), DoubleValue(null)), -1.0)
    }

    @Test
    fun testMaxString() {
        testAggregate(Aggregates.MAX_STRING.invoke(), listOf(StringValue(null), StringValue("123")), "123")
        testAggregate(Aggregates.MAX_STRING.invoke(), listOf(StringValue("abc"), StringValue("abd")), "abd")
    }

    @Test
    fun testMinString() {
        testAggregate(Aggregates.MIN_STRING.invoke(), listOf(StringValue(null), StringValue("123")), "123")
        testAggregate(Aggregates.MIN_STRING.invoke(), listOf(StringValue("abc"), StringValue("abd")), "abc")
    }


    private fun testAggregate(agg: AggregateFunction, atoms: List<SqlValueAtom>, expectedResult: Any?) {
        atoms.forEach(agg::process)
        assertEquals(expectedResult, agg.getResult().asValue)
    }
}