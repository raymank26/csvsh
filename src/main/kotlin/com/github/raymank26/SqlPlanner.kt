package com.github.raymank26

import com.github.raymank26.sql.SqlParser

/**
 * Date: 2019-05-13.
 */
class SqlPlanner {

    fun makePlan(sqlAst: SqlParser.ParseContext, availableIndexes: List<IndexDescription>): PlanDescription? {
        val whereExpr = sqlAst.statement().select().whereExpr() ?: return null
        val resultPlan = SqlPlannerVisitor(availableIndexes).visit(whereExpr)
        require(resultPlan != null)
        return resultPlan
    }
}
