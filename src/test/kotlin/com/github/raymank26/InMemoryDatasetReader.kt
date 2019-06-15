package com.github.raymank26

import java.nio.file.Path
import java.util.NavigableMap

class InMemoryDatasetFactory(private val datasetReader: DatasetReader) : DatasetReaderFactory {
    override fun getReader(path: Path): DatasetReader? {
        return datasetReader
    }
}

class InMemoryIndex(override val type: FieldType, private val inMemoryIndex: NavigableMap<Any, List<Int>>) : ReadOnlyIndex {

    override fun moreThan(from: SqlValueAtom): Set<Int> {
        return inMemoryIndex.tailMap(from.asValue, false).values.flatten().toSet()
    }

    override fun moreThanEq(fromInclusive: SqlValueAtom): Set<Int> {
        return inMemoryIndex.tailMap(fromInclusive.asValue, true).values.flatten().toSet()
    }

    override fun lessThan(to: SqlValueAtom): Set<Int> {
        return inMemoryIndex.headMap(to.asValue, false).values.flatten().toSet()
    }

    override fun lessThanEq(toInclusive: SqlValueAtom): Set<Int> {
        return inMemoryIndex.headMap(toInclusive.asValue, true).values.flatten().toSet()
    }

    override fun eq(value: SqlValueAtom): Set<Int> {
        return inMemoryIndex[value.asValue]?.toSet() ?: emptySet()
    }

    override fun inRange(list: ListValue): Set<Int> {
        return emptySet()
    }

    override fun close() {
    }
}

/**
 * Date: 2019-05-20.
 */
class InMemoryDatasetReader(override val columnInfo: List<ColumnInfo>,
                            private val datasetRows: List<DatasetRow>,
                            override val availableIndexes: List<IndexDescriptionAndPath>) : DatasetReader {
    override fun getIterator(): ClosableSequence<DatasetRow> {
        return ClosableSequence(datasetRows.asSequence(), null)
    }

    override fun getIterator(offsets: List<Long>): ClosableSequence<DatasetRow> {
        val offsetsSet = offsets.toSet()
        return ClosableSequence(datasetRows.asSequence().filterIndexed { i, _ -> offsetsSet.contains(i.toLong()) }, null)
    }
}