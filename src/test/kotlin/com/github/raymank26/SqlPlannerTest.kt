package com.github.raymank26

import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

/**
 * Date: 2019-05-13.
 */
class SqlPlannerTest : SqlTestUtils {

    @Test
    fun testWhereEmpty() {
        val planDescription: SqlPlan = makePlan("SELECT * FROM 'a'")
        assertNull(planDescription.wherePlanDescription)
    }

    @Test
    fun testWherePlan() {
        val planDescription: SqlPlan = makePlan(
                "SELECT * FROM 'a' WHERE (5 > a AND (a > 2) AND b IN (1,2,3) OR c = 4)")
        val expectedExpr = ExpressionNode(
                left = ExpressionNode(
                        left = ExpressionNode(
                                left = ExpressionAtom(
                                        leftVal = RefValue("a"),
                                        operator = Operator.LESS_THAN,
                                        rightVal = IntValue(5)),
                                operator = ExpressionOperator.AND,
                                right = ExpressionAtom(
                                        leftVal = RefValue("a"),
                                        operator = Operator.GREATER_THAN,
                                        rightVal = IntValue(2))),
                        operator = ExpressionOperator.AND,
                        right = ExpressionAtom(
                                leftVal = RefValue(name = "b"),
                                operator = Operator.IN,
                                rightVal = ListValue(
                                        value = listOf(IntValue(1), IntValue(2), IntValue(3))))),
                operator = ExpressionOperator.OR,
                right = ExpressionAtom(
                        leftVal = RefValue("c"),
                        operator = Operator.EQ,
                        rightVal = IntValue(4)))
        assertEquals(expectedExpr, planDescription.wherePlanDescription?.expressionTree)
    }

    @Test
    fun testFullForm() {
        val planDescription: SqlPlan = makePlan(
                "SELECT a,b, MAX(c) FROM './a' GROUP BY a,b WHERE a < 5 ORDER BY b DESC LIMIT 9", getDefaultDatasetFactory())
        assertEquals(listOf("a", "b"), planDescription.groupByFields)
        assertEquals(OrderByPlanDescription(SelectFieldExpr("b"), true), planDescription.orderByPlanDescription)
        assertEquals(9, planDescription.limit)
        assertEquals(listOf(SelectFieldExpr("a"), SelectFieldExpr("b"), AggSelectExpr("max", "c")), planDescription.selectStatements)
    }

    @Test
    fun testGroupBy() {
        makePlan("SELECT b,a,MAX(c) FROM './a' GROUP BY a,b")
    }

    @Test
    fun testGroupByFailure1() {
        testPlannerFailure { makePlan("SELECT a,b,c FROM './a' GROUP BY a,b") }
    }

    @Test
    fun testGroupByFailure2() {
        testPlannerFailure { makePlan("SELECT * FROM './a' GROUP BY a") }
    }

    private fun testPlannerFailure(r: () -> Unit) {
        testFailure(PlannerException::class.java) { r() }
    }
}
