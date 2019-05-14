package com.github.raymank26

import org.junit.Test
import kotlin.test.assertEquals

/**
 * Date: 2019-05-13.
 */
class SqlPlannerTest {
    private val sqlAstBuilder: SqlAstBuilder = SqlAstBuilder()

    @Test
    fun testPlanner() {
        val sqlPlanner = SqlPlanner()
        val availableIndexes = listOf(
                IndexDescription(name = "aIndex", fieldName = "a"),
                IndexDescription(name = "bIndex", fieldName = "b")
        )
        val plan: Map<ScanSource, List<Expression>> = sqlPlanner.makePlan(sqlAstBuilder.parse(
                "SELECT * FROM 'a' WHERE (5 > a AND (a > 2) AND b IN (1,2,3) OR c = 4)"), availableIndexes)
        assertEquals(3, plan.size)
        assertEquals(2, plan.getValue(IndexInput("aIndex")).size)
        assertEquals(1, plan.getValue(IndexInput("bIndex")).size)
        assertEquals(1, plan.getValue(CsvInput).size)
        println(plan)
    }
}