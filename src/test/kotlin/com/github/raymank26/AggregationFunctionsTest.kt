package com.github.raymank26

import org.junit.Test
import kotlin.test.assertEquals

/**
 * Date: 2019-06-13.
 */
class AggregationFunctionsTest {

    @Test
    fun testSumInt() {
        testAggregate(Aggregates.SUM_INT.invoke(), listOf(IntValue(null), IntValue(0), IntValue(1), IntValue(2)), 3)
        testAggregate(Aggregates.SUM_INT.invoke(), listOf(IntValue(null)), 0)
    }

    @Test
    fun testSumFloat() {
        testAggregate(Aggregates.SUM_FLOAT.invoke(), listOf(FloatValue(null), FloatValue(0f), FloatValue(1.2f),
                FloatValue(2.3f)), 3.5f)
        testAggregate(Aggregates.SUM_FLOAT.invoke(), listOf(FloatValue(null)), 0f)
    }

    @Test
    fun testCount() {
        testAggregate(Aggregates.COUNT_ANY.invoke(), listOf(FloatValue(null), FloatValue(0f), FloatValue(1.2f),
                FloatValue(2.3f)), 4)
    }

    @Test
    fun testMinInt() {
        testAggregate(Aggregates.MIN_INT.invoke(), listOf(IntValue(null), IntValue(0), IntValue(1), IntValue(2)), 0)
        testAggregate(Aggregates.MIN_INT.invoke(), listOf(IntValue(null), IntValue(-1), IntValue(1), IntValue(2)), -1)
        testAggregate(Aggregates.MIN_INT.invoke(), listOf(IntValue(null)), null)
    }

    @Test
    fun testMaxInt() {
        testAggregate(Aggregates.MAX_INT.invoke(), listOf(IntValue(null), IntValue(0), IntValue(1), IntValue(2)), 2)
        testAggregate(Aggregates.MAX_INT.invoke(), listOf(IntValue(null), IntValue(-1), IntValue(1), IntValue(2)), 2)
        testAggregate(Aggregates.MAX_INT.invoke(), listOf(IntValue(null)), null)
        testAggregate(Aggregates.MAX_INT.invoke(), listOf(IntValue(-2), IntValue(-1), IntValue(null)), -1)
    }

    @Test
    fun testMinFloat() {
        testAggregate(Aggregates.MIN_FLOAT.invoke(), listOf(FloatValue(null), FloatValue(0f), FloatValue(1.2f), FloatValue(2.8f)), 0f)
        testAggregate(Aggregates.MIN_FLOAT.invoke(), listOf(FloatValue(null), FloatValue(-1.8f), FloatValue(-1.9f), FloatValue(-2.2f)), -2.2f)
        testAggregate(Aggregates.MIN_FLOAT.invoke(), listOf(FloatValue(null)), null)
    }

    @Test
    fun testMaxFloat() {
        testAggregate(Aggregates.MAX_FLOAT.invoke(), listOf(FloatValue(null), FloatValue(0f), FloatValue(1.2f), FloatValue(2.8f)), 2.8f)
        testAggregate(Aggregates.MAX_FLOAT.invoke(), listOf(FloatValue(null), FloatValue(-1.8f), FloatValue(-1.9f), FloatValue(-2.2f)), -1.8f)
        testAggregate(Aggregates.MAX_FLOAT.invoke(), listOf(FloatValue(null)), null)
        testAggregate(Aggregates.MAX_FLOAT.invoke(), listOf(FloatValue(-2f), FloatValue(-1f), FloatValue(null)), -1f)
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