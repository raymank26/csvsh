package com.github.raymank26

import com.github.raymank26.sql.SqlParser

/**
 * Date: 2019-05-13.
 */
class SqlPlanner {

    fun makePlan(sqlAst: SqlParser.ParseContext, availableIndexes: List<IndexDescription>): Map<ScanSource, MutableList<Expression>> {
        return SqlPlannerVisitor(availableIndexes).visit(sqlAst)
    }
}
