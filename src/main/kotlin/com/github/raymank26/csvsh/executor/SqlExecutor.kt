package com.github.raymank26.csvsh.executor

import com.github.raymank26.csvsh.AggSelectExpr
import com.github.raymank26.csvsh.ClosableSequence
import com.github.raymank26.csvsh.ColumnInfo
import com.github.raymank26.csvsh.DatasetResult
import com.github.raymank26.csvsh.DatasetRow
import com.github.raymank26.csvsh.FieldType
import com.github.raymank26.csvsh.LongValue
import com.github.raymank26.csvsh.SelectFieldExpr
import com.github.raymank26.csvsh.SqlValueAtom
import com.github.raymank26.csvsh.planner.OrderByPlanDescription
import com.github.raymank26.csvsh.planner.PlannerException
import com.github.raymank26.csvsh.planner.SqlPlan
import org.slf4j.LoggerFactory
import java.util.concurrent.TimeUnit

private val LOG = LoggerFactory.getLogger(SqlExecutor::class.java)

/**
 * Date: 2019-05-15.
 */
class SqlExecutor {

    fun execute(sqlPlan: SqlPlan): DatasetResult {
        val startTime = System.nanoTime()
        var dataset = readDataset(sqlPlan)

        if (sqlPlan.wherePlanDescription != null) {
            dataset = applyWhere(sqlPlan, dataset)
        }
        dataset = if (sqlPlan.isAggregation) {
            applyGroupBy(sqlPlan, dataset)
        } else {
            applySelect(sqlPlan, dataset)
        }
        if (sqlPlan.orderByPlanDescription != null) {
            dataset = applyOrderBy(sqlPlan.orderByPlanDescription, dataset)
        }
        if (sqlPlan.limit != null) {
            dataset = applyLimit(sqlPlan.limit, dataset)
        }
        LOG.info("Execution completed in ${TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime)}ms.")
        return dataset
    }

    private fun applyWhere(sqlPlan: SqlPlan, dataset: DatasetResult): DatasetResult {
        return dataset.copy(rows = dataset.rows.filter { row ->
            WhereEvalVisitor(row).visitExpression(sqlPlan.wherePlanDescription!!.expressionTree)
        })
    }

    private fun readDataset(sqlPlan: SqlPlan): DatasetResult {
        val newSequence = if (sqlPlan.wherePlanDescription != null) {
            val offsets = sqlPlan.indexEvaluator?.offsets?.invoke()

            if (offsets == null) {
                sqlPlan.datasetReader.getIterator()
            } else {
                sqlPlan.datasetReader.getIterator(offsets.toList().sorted())
            }
        } else {
            sqlPlan.datasetReader.getIterator()
        }
        return DatasetResult(newSequence, sqlPlan.datasetReader.columnInfo)
    }

    private fun applySelect(sqlPlan: SqlPlan, dataset: DatasetResult): DatasetResult {
        val allowedFields = if (sqlPlan.selectStatements.isEmpty()) {
            dataset.columnInfo.map { it.fieldName }
        } else {
            sqlPlan.selectStatements.asSequence().map { it as SelectFieldExpr }.map { it.fieldName }.toSet()
        }

        val fieldNameToInfo = dataset.columnInfo.associateBy { it.fieldName }
        val newColumnInfo = allowedFields.map { fieldNameToInfo.getValue(it) }

        val newSequence = dataset.rows.map { row ->
            allowedFields.map { row.getCell(it) }
            val columns = mutableListOf<SqlValueAtom>()
            for (allowedField in allowedFields) {
                columns.add(row.getCell(allowedField))
            }
            DatasetRow(row.rowNum, columns, newColumnInfo, row.characterOffset)
        }
        return DatasetResult(newSequence, newColumnInfo)
    }

    private fun applyGroupBy(sqlPlan: SqlPlan, dataset: DatasetResult): DatasetResult {
        val groupByBuckets = mutableMapOf<List<Pair<ColumnInfo, SqlValueAtom>>, List<AggregateFunction>>()
        val aggStatements = sqlPlan.selectStatements.mapNotNull { it as? AggSelectExpr }
        val plainStatements = sqlPlan.selectStatements.mapNotNull { it as? SelectFieldExpr }

        val aggToColumnInfo = mutableListOf<ColumnInfo>()
        var plainToColumnInfo = listOf<ColumnInfo>()

        dataset.rows.forEach { row ->
            val bucketDesc = sqlPlan.groupByFields.map { Pair(row.getColumnInfo(it)!!, row.getCell(it)) }
            if (plainToColumnInfo.isEmpty()) {
                for (plainStatement in plainStatements) {
                    plainToColumnInfo = bucketDesc.map { it.first }
                }
            }
            groupByBuckets.compute(bucketDesc) { _, prev ->
                if (prev != null) {
                    for ((i, aggStatement) in aggStatements.withIndex()) {
                        applyRowToAggregate(prev[i], row, aggStatement.fieldName)
                    }
                    prev
                } else {
                    val initAggList = initAggregates(aggStatements, row)
                    if (aggToColumnInfo.isEmpty()) {
                        for ((index, aggregateFunction) in initAggList.withIndex()) {
                            val aggSelectStatement = aggStatements[index]
                            aggToColumnInfo.add(ColumnInfo(aggregateFunction.valueType, aggSelectStatement.fullFieldName))

                        }
                        for (i in 0 until aggStatements.size) {
                        }
                    }
                    initAggList
                }
            }
        }
        val newColumnInfo = {
            val res = mutableListOf<ColumnInfo>()
            res.addAll(plainToColumnInfo)
            res.addAll(aggToColumnInfo)
            res
        }()

        if (groupByBuckets.isEmpty()) {
            return DatasetResult(ClosableSequence(emptySequence()), newColumnInfo)
        }

        val newRows = processGroupByRows(groupByBuckets, newColumnInfo)
        return DatasetResult(ClosableSequence(newRows.asSequence()), newColumnInfo)
    }

    private fun initAggregates(aggStatements: List<AggSelectExpr>, row: DatasetRow): List<AggregateFunction> {
        val initAggList = mutableListOf<AggregateFunction>()
        for (i in 0 until aggStatements.size) {
            val aggStatement: AggSelectExpr = aggStatements[i]

            val agg = if (aggStatement.type == "count") {
                Aggregates.COUNT_ANY.invoke()
            } else {
                val cell = row.getCell(aggStatement.fieldName)
                AGGREGATES_MAPPING[Pair(aggStatement.type, cell.type)]?.invoke()
                        ?: throw PlannerException("Unable to execute agg of type = ${aggStatement.type} and column type = ${cell.type}")
            }
            applyRowToAggregate(agg, row, aggStatement.fieldName)

            initAggList.add(agg)
        }
        return initAggList
    }

    private fun applyRowToAggregate(aggregate: AggregateFunction, row: DatasetRow, fieldName: String) {
        when (aggregate.aggType) {
            AggregateType.COUNT ->
                if (fieldName == "*") {
                    aggregate.process(LongValue(1))
                } else {
                    val cell: SqlValueAtom = row.getCell(fieldName)
                    if (cell.asValue != null) {
                        aggregate.process(cell)
                    }
                }
            else -> aggregate.process(row.getCell(fieldName))
        }
    }

    private fun processGroupByRows(groupByBuckets: Map<List<Pair<ColumnInfo, SqlValueAtom>>, List<AggregateFunction>>,
                                   newColumnInfo: List<ColumnInfo>): MutableList<DatasetRow> {
        val newRows = mutableListOf<DatasetRow>()
        var rowNum = 0
        for ((fixedFields, aggregates) in groupByBuckets) {
            val columns = mutableListOf<SqlValueAtom>()
            for (bucketField in fixedFields) {
                columns.add(bucketField.second)
            }
            for (agg in aggregates) {
                columns.add(agg.getResult())
            }
            newRows.add(DatasetRow(rowNum++, columns, newColumnInfo, null))
        }
        return newRows
    }

    private fun applyOrderBy(orderByStmt: OrderByPlanDescription, dataset: DatasetResult): DatasetResult {
        val fieldName = orderByStmt.field.fullFieldName
        val f = { row: DatasetRow -> row.getCell(fieldName) }
        val newRows = if (orderByStmt.desc) {
            dataset.rows.transform { it.sortedByDescending(f) }
        } else {
            dataset.rows.transform { it.sortedBy(f) }
        }
        return dataset.copy(rows = newRows)
    }

    private fun applyLimit(limit: Int, dataset: DatasetResult): DatasetResult {
        return dataset.copy(rows = dataset.rows.take(limit))
    }
}

val AGGREGATES_MAPPING: Map<Pair<String, FieldType>, AggregateFunctionFactory> = mapOf(
        Pair(Pair("min", FieldType.LONG), Aggregates.MIN_INT),
        Pair(Pair("min", FieldType.DOUBLE), Aggregates.MIN_FLOAT),
        Pair(Pair("min", FieldType.STRING), Aggregates.MIN_STRING),
        Pair(Pair("max", FieldType.LONG), Aggregates.MAX_INT),
        Pair(Pair("max", FieldType.DOUBLE), Aggregates.MAX_FLOAT),
        Pair(Pair("max", FieldType.STRING), Aggregates.MAX_STRING),
        Pair(Pair("sum", FieldType.LONG), Aggregates.SUM_INT),
        Pair(Pair("sum", FieldType.DOUBLE), Aggregates.SUM_FLOAT)
)

