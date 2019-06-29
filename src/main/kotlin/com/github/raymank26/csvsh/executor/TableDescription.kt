package com.github.raymank26.csvsh.executor

import com.github.raymank26.csvsh.DatasetResult

/**
 * Date: 2019-06-22.
 */
data class TableDescription(
        val columns: DatasetResult,
        val sizeStatistics: DatasetResult,
        val indexes: DatasetResult
)
