package com.github.raymank26

import java.nio.file.Path

class InMemoryDatasetFactory(private val datasetReader: DatasetReader) : DatasetReaderFactory {
    override fun getReader(path: Path): DatasetReader? {
        return datasetReader
    }
}

class InMemoryEmptyIndex<T>(private val fieldType: FieldType) : ReadOnlyIndex<T> {

    override fun moreThan(from: T): Set<Int> {
        return emptySet()
    }

    override fun moreThanEq(fromInclusive: T): Set<Int> {
        return emptySet()
    }

    override fun lessThan(to: T): Set<Int> {
        return emptySet()
    }

    override fun lessThanEq(toInclusive: T): Set<Int> {
        return emptySet()
    }

    override fun eq(value: T): Set<Int> {
        return emptySet()
    }

    override fun inRange(value: T, list: List<T>): Set<Int> {
        return emptySet()
    }

    override fun getType(): FieldType {
        return fieldType
    }

    override fun close() {
    }
}

/**
 * Date: 2019-05-20.
 */
class InMemoryDatasetReader(private val columnInfoField: List<ColumnInfo>,
                            private val datasetRows: List<DatasetRow>,
                            private val availableIndexes: List<IndexDescriptionAndPath>) : DatasetReader {
    override fun close() {
    }

    override fun availableIndexes(): List<IndexDescriptionAndPath> {
        return availableIndexes
    }

    override fun read(handle: (row: DatasetRow) -> Unit, limit: Int?) {
        datasetRows.let {
            if (limit != null) {
                it.slice(0 until limit)
            } else {
                it
            }
        }.forEach(handle)
    }

    override fun getColumnInfo(): List<ColumnInfo> {
        return columnInfoField
    }
}