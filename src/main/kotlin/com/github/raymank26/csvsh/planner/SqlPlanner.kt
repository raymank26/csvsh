package com.github.raymank26.csvsh.planner

import com.github.raymank26.csvsh.AggSelectExpr
import com.github.raymank26.csvsh.DatasetReader
import com.github.raymank26.csvsh.DatasetReaderFactory
import com.github.raymank26.csvsh.SelectFieldExpr
import com.github.raymank26.csvsh.SelectStatementExpr
import com.github.raymank26.csvsh.file.FileSystem
import com.github.raymank26.csvsh.index.IndexDescription
import com.github.raymank26.csvsh.sql.SqlParser
import com.github.raymank26.csvsh.util.toPath
import org.slf4j.LoggerFactory
import java.util.concurrent.TimeUnit

private val LOG = LoggerFactory.getLogger(SqlPlanner::class.java)

/**
 * Date: 2019-05-13.
 */
class SqlPlanner(private val fileSystem: FileSystem) {

    fun createPlan(sqlAst: SqlParser.SelectContext, datasetReaderFactory: DatasetReaderFactory): SqlPlan {
        val startTime = System.nanoTime()
        val tablePath = fileSystem.toValidAbsolutePath(sqlAst.table().toPath())
        val reader = datasetReaderFactory.getReader(tablePath)
                ?: throw PlannerException("Unable to find input for path = $tablePath")
        val sqlWherePlan = sqlAst.whereExpr()?.let { SqlWhereVisitor().visit(it) }
        val indexEvaluator = if (sqlWherePlan != null) {
            DatasetIndexOffsetCollectorVisitor(reader.availableIndexes)
                    .visitExpression(sqlWherePlan.expressionTree)
        } else null

        val selectStatements: List<SelectStatementExpr> = if (sqlAst.selectExpr().allColumns() != null) {
            emptyList()
        } else {
            sqlAst.selectExpr().selectColumn().map { SqlSelectColumnVisitor().visit(it) }
        }
        val isAggregation = sqlAst.groupByExpr() != null || selectStatements.any { it is AggSelectExpr }
        val groupByFields = if (isAggregation) {
            getGroupByExpression(sqlAst, selectStatements)
        } else {
            validateSelectExpression(reader, selectStatements)
            emptyList()
        }

        val orderBy = sqlAst.orderByExpr()?.let {
            val ref = SqlSelectColumnVisitor().visit(it.selectColumn())
            OrderByPlanDescription(ref, it.DESC()?.let { true } ?: false)
        }
        val limit = sqlAst.limitExpr()?.INTEGER()?.text?.toInt()?.apply {
            if (this < 1) {
                throw PlannerException("Limit statement has to be > 0")
            }
        }
        LOG.debug("Planning completed in ${TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime)}ms.")
        return SqlPlan(selectStatements, reader, sqlWherePlan, groupByFields, orderBy, limit, indexEvaluator, isAggregation)
    }

    private fun validateSelectExpression(reader: DatasetReader, selectStatements: List<SelectStatementExpr>) {
        val fieldToInfo = reader.columnInfo.associateBy { it.fieldName }
        for (selectStatement in selectStatements) {
            when (selectStatement) {
                is AggSelectExpr -> throw PlannerException("Aggregate statements are not allowed without 'group by' statement")
                is SelectFieldExpr -> if (!fieldToInfo.containsKey(selectStatement.fullFieldName)) {
                    throw PlannerException("Select field = ${selectStatement.fieldName} is not in dataset")
                }
            }
        }
    }

    private fun getGroupByExpression(sqlAst: SqlParser.SelectContext, selectStatements: List<SelectStatementExpr>): List<String> {
        val groupedFields: List<String> = sqlAst.groupByExpr()?.reference()?.map { it.IDENTIFIER().text } ?: emptyList()
        val selectPlainFieldNames = selectStatements.mapNotNull { (it as? SelectFieldExpr)?.fieldName }
        if (selectPlainFieldNames.toSet() != groupedFields.toSet()) {
            throw PlannerException("Unable to select not grouped fields")
        }
        return groupedFields
    }
}

data class IndexEvaluator(val usedIndexes: Set<IndexDescription>, val offsets: () -> Set<Long>)
