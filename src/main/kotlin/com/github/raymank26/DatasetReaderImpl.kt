package com.github.raymank26

import java.io.Reader

class DatasetReaderImpl(private val contentDataProvider: ContentDataProvider,
                        private val readerFactory: () -> Reader,
                        override val columnInfo: List<ColumnInfo>,
                        override val availableIndexes: List<IndexDescriptionAndPath>) : DatasetReader {

    override fun getIterator(): ClosableSequence<DatasetRow> {
        if (columnInfo.isEmpty()) {
            return ClosableSequence(emptySequence(), null)
        }
        var i = 0
        return contentDataProvider.get(readerFactory()).map { columns ->
            DatasetRow(i++, columnInfo.mapIndexed { i, col -> createSqlAtom(columns[i], col.type) }, columnInfo)
        }
    }
}