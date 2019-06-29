package com.github.raymank26.planner

import com.github.raymank26.DatasetReader
import com.github.raymank26.Expression
import com.github.raymank26.SelectStatementExpr

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