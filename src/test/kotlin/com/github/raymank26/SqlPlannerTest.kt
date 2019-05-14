package com.github.raymank26

import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.fail

/**
 * Date: 2019-05-13.
 */
class SqlPlannerTest {
    private val sqlAstBuilder: SqlAstBuilder = SqlAstBuilder()
    private val sqlPlanner = SqlPlanner()

    @Test
    fun testEmpty() {
        val planDescription: PlanDescription? = sqlPlanner.makePlan(sqlAstBuilder.parse("SELECT * FROM 'a'"),
                emptyList())
        assertNull(planDescription)
    }

    @Test
    fun testPlanner() {
        val availableIndexes = listOf(
                IndexDescription(name = "aIndex", fieldName = "a"),
                IndexDescription(name = "bIndex", fieldName = "b")
        )
        val planDescription: PlanDescription = sqlPlanner.makePlan(sqlAstBuilder.parse(
                "SELECT * FROM 'a' WHERE (5 > a AND (a > 2) AND b IN (1,2,3) OR c = 4)"), availableIndexes)
                ?: fail("Unable to execute plan")
        val expBySource = planDescription.expressionsBySource

        assertEquals(3, expBySource.size)
        assertEquals(2, expBySource.getValue(IndexInput("aIndex")).size)
        assertEquals(1, expBySource.getValue(IndexInput("bIndex")).size)
        assertEquals(1, expBySource.getValue(CsvInput).size)
    }
}