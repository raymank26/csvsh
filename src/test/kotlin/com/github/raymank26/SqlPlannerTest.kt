package com.github.raymank26

import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.fail

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
        val expBySource = planDescription.wherePlanDescription?.expressionsBySource ?: fail("Unable to find where plan")

        assertEquals(3, expBySource.size)
        assertEquals(2, expBySource.getValue(IndexInput("aIndex")).size)
        assertEquals(1, expBySource.getValue(IndexInput("bIndex")).size)
        assertEquals(1, expBySource.getValue(CsvInput).size)
    }

    @Test
    fun testFullForm() {
        val planDescription: SqlPlan = makePlan(
                "SELECT a,b, MAX(c) FROM './a' GROUP BY a,b WHERE a < 5 ORDER BY b DESC LIMIT 9", getDefaultDatasetFactory())
        assertEquals(listOf("a", "b"), planDescription.groupByFields)
        assertEquals(OrderByPlanDescription("b", true), planDescription.orderByPlanDescription)
        assertEquals(9, planDescription.limit)
        assertEquals(listOf(SelectFieldExpr("a"), SelectFieldExpr("b"), AggSelectExpr("MAX", "c")), planDescription.selectStatements)
    }

    @Test
    fun testGroupBy() {
        makePlan("SELECT a,b,max(c) FROM './a' GROUP BY a,b")
    }

    @Test
    fun testGroupByFailure() {
        testFailure { makePlan("SELECT a,b,c FROM './a' GROUP BY a,b") }
    }
}
