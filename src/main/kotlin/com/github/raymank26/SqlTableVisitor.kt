package com.github.raymank26

import com.github.raymank26.sql.SqlBaseVisitor
import com.github.raymank26.sql.SqlParser

/**
 * Date: 2019-05-14.
 */
class SqlTableVisitor : SqlBaseVisitor<String?>() {
    override fun visitTable(ctx: SqlParser.TableContext): String {
        return ctx.IDENTIFIER_Q().text
    }
}