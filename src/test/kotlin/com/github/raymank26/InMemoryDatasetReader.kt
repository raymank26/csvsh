package com.github.raymank26

/**
 * Date: 2019-05-20.
 */
class InMemoryDatasetReader(private val columnNamesField: List<String>,
                            private val columnInfoField: Map<String, ColumnInfo>,
                            private val datasetRows: List<DatasetRow>) : DatasetReader {

    override fun read(handle: (row: DatasetRow) -> Unit, limit: Int?) {
        datasetRows.let {
            if (limit != null) {
                it.slice(0 until limit)
            } else {
                it
            }
        }.forEach(handle)
    }

    override fun getColumnNames(): List<String> {
        return columnNamesField
    }

    override fun getColumnInfo(): Map<String, ColumnInfo> {
        return columnInfoField
    }
}