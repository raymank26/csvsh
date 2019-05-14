package com.github.raymank26

import java.sql.ResultSet

/**
 * Date: 2019-05-13.
 */
class ExecutorEngine(private val sqlParser: SqlAstBuilder) {


    @Throws(SyntaxException::class)
    fun execute(sqlStatement: String): Iterator<ResultSet> {
        val parsedValue = sqlParser.parse(sqlStatement)
        return TODO()
    }
}