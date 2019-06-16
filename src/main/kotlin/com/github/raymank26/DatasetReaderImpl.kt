package com.github.raymank26

import com.github.raymank26.file.NavigableReader

class DatasetReaderImpl(private val contentDataProvider: ContentDataProvider,
                        private val readerFactory: () -> NavigableReader,
                        override val columnInfo: List<ColumnInfo>,
                        override val availableIndexes: List<IndexDescriptionAndPath>) : DatasetReader {

    override fun getIterator(): ClosableSequence<DatasetRow> {
        if (columnInfo.isEmpty()) {
            return ClosableSequence(emptySequence(), null)
        }
        return contentDataProvider.get(readerFactory()).transform(toRows())
    }

    override fun getIterator(offsets: List<Long>): ClosableSequence<DatasetRow> {
        if (columnInfo.isEmpty()) {
            return ClosableSequence(emptySequence(), null)
        }
        return contentDataProvider.get(readerFactory(), offsets).transform(toRows())
    }

    private fun toRows(): (Sequence<ContentRow>) -> Sequence<DatasetRow> {
        return { seq ->
            seq.mapIndexed { i, contentRow ->
                DatasetRow(i, columnInfo.mapIndexed { j, col -> createSqlAtom(contentRow.columns[j], col.type) }, columnInfo, contentRow.offset)
            }
        }
    }
}