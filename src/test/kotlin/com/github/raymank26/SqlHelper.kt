package com.github.raymank26

import org.junit.Assert

/**
 * Date: 2019-05-20.
 */
private val sqlAstBuilder = SqlAstBuilder()
private val sqlPlanner = SqlPlanner()
private val sqlExecutor = SqlExecutor()

interface SqlTestUtils {

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

    fun makePlan(sql: String, indexes: List<IndexDescription>): SqlPlan {
        return sqlPlanner.makePlan((sqlAstBuilder.parse(sql) as SelectStatement).ctx, indexes)
    }

    fun executeSelect(sql: String, dataset: DatasetReader): DatasetResult {
        val plan = makePlan(sql, emptyList())
        return sqlExecutor.execute(EngineContext(dataset, emptyMap()), plan)
    }
}

