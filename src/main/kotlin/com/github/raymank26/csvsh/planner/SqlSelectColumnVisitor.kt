package com.github.raymank26.csvsh.planner

import com.github.raymank26.csvsh.AggSelectExpr
import com.github.raymank26.csvsh.SelectFieldExpr
import com.github.raymank26.csvsh.SelectStatementExpr
import com.github.raymank26.csvsh.sql.SqlBaseVisitor
import com.github.raymank26.csvsh.sql.SqlParser

class SqlSelectColumnVisitor : SqlBaseVisitor<SelectStatementExpr>() {
    override fun visitSelectColumnPlain(ctx: SqlParser.SelectColumnPlainContext): SelectStatementExpr {
        return SelectFieldExpr(ctx.reference().IDENTIFIER().text)
    }

    override fun visitSelectColumnAgg(ctx: SqlParser.SelectColumnAggContext): SelectStatementExpr {
        return AggSelectExpr(ctx.AGG().text.toLowerCase(), ctx.reference()?.text ?: ctx.allColumns().text)
    }
}