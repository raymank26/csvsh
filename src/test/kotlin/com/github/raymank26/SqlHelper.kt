package com.github.raymank26

import org.junit.Assert
import java.lang.Exception

/**
 * Date: 2019-05-20.
 */

open class BaseSqlTest {
    private val sqlAstBuilder = SqlAstBuilder()
    private val sqlPlanner = SqlPlanner()
    private val sqlExecutor = SqlExecutor()

    fun testParser(statement: String, failureExpected: Boolean = false) {
        try {
            sqlAstBuilder.parse(statement)
            if (failureExpected) {
                Assert.fail("Failure expected, but it passes")
            }
        } catch (e: Exception) {
            if (!failureExpected) {
                Assert.fail(e.toString())
            }
        }
    }

    fun makePlan(sql: String, indexes: List<IndexDescription>): PlanDescription? {
        return sqlPlanner.makePlan((sqlAstBuilder.parse(sql) as SelectStatement).ctx, indexes)
    }

    fun executeSelect(sql: String, dataset: DatasetReader): DatasetResult {
        val plan = makePlan(sql, emptyList())
        return sqlExecutor.execute(EngineContext(dataset, emptyMap()), plan)
    }
}

