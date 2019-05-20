package com.github.raymank26

import org.junit.Test
import java.nio.file.Paths
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.fail

/**
 * Date: 2019-05-13.
 */
class SqlPlannerTest : SqlTestUtils {

    private val availableIndexes = listOf(
            IndexDescription(name = "aIndex", fieldName = "a"),
            IndexDescription(name = "bIndex", fieldName = "b")
    )

    @Test
    fun testWhereEmpty() {
        val planDescription: SqlPlan = makePlan("SELECT * FROM 'a'", emptyList())
        assertNull(planDescription.wherePlanDescription)
    }

    @Test
    fun testWherePlan() {
        val planDescription: SqlPlan = makePlan(
                "SELECT * FROM 'a' WHERE (5 > a AND (a > 2) AND b IN (1,2,3) OR c = 4)", availableIndexes)
        val expBySource = planDescription.wherePlanDescription?.expressionsBySource ?: fail("Unable to find where plan")

        assertEquals(3, expBySource.size)
        assertEquals(2, expBySource.getValue(IndexInput("aIndex")).size)
        assertEquals(1, expBySource.getValue(IndexInput("bIndex")).size)
        assertEquals(1, expBySource.getValue(CsvInput).size)
    }

    @Test
    fun testFullForm() {
        val planDescription: SqlPlan = makePlan(
                "SELECT a,b, MAX(c) FROM './a' GROUP BY a,b WHERE a < 5 ORDER BY b DESC LIMIT 9", availableIndexes)
        assertEquals(Paths.get("./a"), planDescription.tablePath)
        assertEquals(listOf("a", "b"), planDescription.groupByFields)
        assertEquals(OrderByPlanDescription("b", true), planDescription.orderByPlanDescription)
        assertEquals(9, planDescription.limit)
        assertEquals(listOf(SelectFieldExpr("a"), SelectFieldExpr("b"), AggSelectExpr("MAX", "c")), planDescription.selectStatements)
        println(planDescription)
    }
}
