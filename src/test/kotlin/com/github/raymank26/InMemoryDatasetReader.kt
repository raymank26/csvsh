package com.github.raymank26

import java.nio.file.Path
import java.util.NavigableMap

class InMemoryDatasetFactory(private val datasetReader: DatasetReader) : DatasetReaderFactory {
    override fun getReader(path: Path): DatasetReader? {
        return datasetReader
    }
}

class InMemoryIndex<T>(private val fieldType: FieldType, private val inMemoryIndex: NavigableMap<T, List<Int>>) : ReadOnlyIndex<T> {

    override fun moreThan(from: T): Set<Int> {
        return inMemoryIndex.tailMap(from, false).values.flatten().toSet()
    }

    override fun moreThanEq(fromInclusive: T): Set<Int> {
        return inMemoryIndex.tailMap(fromInclusive, true).values.flatten().toSet()
    }

    override fun lessThan(to: T): Set<Int> {
        return inMemoryIndex.headMap(to, false).values.flatten().toSet()
    }

    override fun lessThanEq(toInclusive: T): Set<Int> {
        return inMemoryIndex.headMap(toInclusive, true).values.flatten().toSet()
    }

    override fun eq(value: T): Set<Int> {
        return inMemoryIndex[value]?.toSet() ?: emptySet()
    }

    override fun inRange(list: List<T>): Set<Int> {
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