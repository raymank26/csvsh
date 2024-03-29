package com.github.raymank26.csvsh

import com.github.raymank26.csvsh.planner.OrderByPlanDescription
import com.github.raymank26.csvsh.planner.PlannerException
import com.github.raymank26.csvsh.planner.SqlPlan
import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

/**
 * Date: 2019-05-13.
 */
class SqlPlannerTest : SqlTestUtils() {

    @Test
    fun testWhereEmpty() {
        val planDescription: SqlPlan = makePlan("SELECT * FROM '/test/input.csv'")
        assertNull(planDescription.wherePlanDescription)
    }

    @Test
    fun testWherePlan() {
        val planDescription: SqlPlan = makePlan(
                "SELECT * FROM '/test/input.csv' WHERE (5 > a AND (a > 2) AND b IN (1,2,3) OR c = 4)")
        val expectedExpr = ExpressionNode(
                left = ExpressionNode(
                        left = ExpressionNode(
                                left = ExpressionAtom(
                                        leftVal = RefValue("a"),
                                        operator = Operator.LESS_THAN,
                                        rightVal = LongValue(5)),
                                operator = ExpressionOperator.AND,
                                right = ExpressionAtom(
                                        leftVal = RefValue("a"),
                                        operator = Operator.GREATER_THAN,
                                        rightVal = LongValue(2))),
                        operator = ExpressionOperator.AND,
                        right = ExpressionAtom(
                                leftVal = RefValue(name = "b"),
                                operator = Operator.IN,
                                rightVal = ListValue(
                                        value = listOf(LongValue(1), LongValue(2), LongValue(3))))),
                operator = ExpressionOperator.OR,
                right = ExpressionAtom(
                        leftVal = RefValue("c"),
                        operator = Operator.EQ,
                        rightVal = LongValue(4)))
        assertEquals(expectedExpr, planDescription.wherePlanDescription?.expressionTree)
    }

    @Test
    fun testFullForm() {
        val planDescription: SqlPlan = makePlan(
                "SELECT a,b, MAX(c) FROM '/test/input.csv' GROUP BY a,b WHERE a < 5 ORDER BY b DESC LIMIT 9", getDefaultDatasetFactory())
        assertEquals(listOf("a", "b"), planDescription.groupByFields)
        assertEquals(OrderByPlanDescription(SelectFieldExpr("b"), true), planDescription.orderByPlanDescription)
        assertEquals(9, planDescription.limit)
        assertEquals(listOf(SelectFieldExpr("a"), SelectFieldExpr("b"), AggSelectExpr("max", "c")), planDescription.selectStatements)
    }

    @Test
    fun testGroupBy() {
        makePlan("SELECT b,a,MAX(c) FROM '/test/input.csv' GROUP BY a,b")
    }

    @Test(expected = PlannerException::class)
    fun testGroupByFailure1() {
        makePlan("SELECT a,b,c FROM '/test/input.csv' GROUP BY a,b")
    }

    @Test(expected = PlannerException::class)
    fun testGroupByFailure2() {
        makePlan("SELECT * FROM '/test/input.csv' GROUP BY a")
    }

    @Test(expected = PlannerException::class)
    fun testNullFailure() {
        makePlan("SELECT * FROM '/test/input.csv' WHERE null = 2")
    }

    @Test
    fun testNullSwap() {
        makePlan("SELECT * FROM '/test/input.csv' WHERE null = b")
    }

    @Test
    fun testSelectCountAll() {
        makePlan("SELECT count(*), sum(*) from '/test/input.csv'")
    }

    @Test(expected = PlannerException::class)
    fun testSelectAggFail() {
        makePlan("SELECT count(*), b from '/test/input.csv'")
    }

    @Test
    fun testIndexNotUsedIfNegative() {
        indexesManager.createIndex(dataPath, "aIndex", "a", readerFactory)
        val plan = makePlan("SELECT * from '/test/input.csv' WHERE a <> 'b'")
        assertEquals(null, plan.indexEvaluator)
    }
}
