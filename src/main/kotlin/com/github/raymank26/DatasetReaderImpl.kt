package com.github.raymank26

import com.github.raymank26.executor.ContentDataProvider
import com.github.raymank26.executor.ContentRow
import com.github.raymank26.executor.DatasetMetadata
import com.github.raymank26.file.Md5Hash
import com.github.raymank26.file.NavigableReader
import com.github.raymank26.index.IndexDescriptionAndPath

class DatasetReaderImpl(private val contentDataProvider: ContentDataProvider,
                        private val readerFactory: () -> NavigableReader,
                        metadata: DatasetMetadata) : DatasetReader {

    override val columnInfo: List<ColumnInfo> = metadata.columnInfos
    override val availableIndexes: List<IndexDescriptionAndPath> = metadata.indexes
    override val contentHash: Md5Hash = metadata.csvMd5

    private val closeIndexes = AutoCloseable {
        availableIndexes.forEach {
            if (it.indexContent.isInitialized()) {
                it.indexContent.value.close()
            }
        }
    }

    override fun getIterator(): ClosableSequence<DatasetRow> {
        if (columnInfo.isEmpty()) {
            return ClosableSequence<DatasetRow>(emptySequence(), null).withClosable(closeIndexes)
        }
        return contentDataProvider.get(readerFactory()).transform(toRows()).withClosable(closeIndexes)
    }

    override fun getIterator(offsets: List<Long>): ClosableSequence<DatasetRow> {
        if (columnInfo.isEmpty()) {
            return ClosableSequence<DatasetRow>(emptySequence(), null).withClosable(closeIndexes)
        }
        return contentDataProvider.get(readerFactory(), offsets).transform(toRows()).withClosable(closeIndexes)
    }

    private fun toRows(): (Sequence<ContentRow>) -> Sequence<DatasetRow> {
        return { seq ->
            seq.mapIndexed { i, contentRow ->
                DatasetRow(i, columnInfo.mapIndexed { j, col -> createSqlAtom(contentRow.columns[j], col.type) }, columnInfo, contentRow.firstCharacterPosition)
            }
        }
    }

    override fun getNavigableReader(): NavigableReader {
        return readerFactory.invoke()
    }
}