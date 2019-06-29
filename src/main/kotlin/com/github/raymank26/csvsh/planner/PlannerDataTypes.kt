package com.github.raymank26.csvsh.planner

import com.github.raymank26.csvsh.DatasetReader
import com.github.raymank26.csvsh.Expression
import com.github.raymank26.csvsh.SelectStatementExpr

data class SqlPlan(
        val selectStatements: List<SelectStatementExpr>,
        val datasetReader: DatasetReader,
        val wherePlanDescription: WherePlanDescription?,
        val groupByFields: List<String>,
        val orderByPlanDescription: OrderByPlanDescription?,
        val limit: Int?,
        val indexEvaluator: IndexEvaluator?
)

data class OrderByPlanDescription(
        val field: SelectStatementExpr,
        val desc: Boolean
)

data class WherePlanDescription(val expressionTree: Expression)