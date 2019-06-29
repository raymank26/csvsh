package com.github.raymank26.planner

import com.github.raymank26.AggSelectExpr
import com.github.raymank26.SelectFieldExpr
import com.github.raymank26.SelectStatementExpr
import com.github.raymank26.sql.SqlBaseVisitor
import com.github.raymank26.sql.SqlParser

class SqlSelectColumnVisitor : SqlBaseVisitor<SelectStatementExpr>() {
    override fun visitSelectColumnPlain(ctx: SqlParser.SelectColumnPlainContext): SelectStatementExpr {
        return SelectFieldExpr(ctx.reference().IDENTIFIER().text)
    }

    override fun visitSelectColumnAgg(ctx: SqlParser.SelectColumnAggContext): SelectStatementExpr {
        return AggSelectExpr(ctx.AGG().text.toLowerCase(), ctx.reference().text)
    }
}