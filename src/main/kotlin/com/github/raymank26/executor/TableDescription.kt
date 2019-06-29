package com.github.raymank26.executor

import com.github.raymank26.DatasetResult

/**
 * Date: 2019-06-22.
 */
data class TableDescription(
        val columns: DatasetResult,
        val sizeStatistics: DatasetResult,
        val indexes: DatasetResult
)
