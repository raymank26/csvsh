package com.github.raymank26

import com.github.raymank26.file.FileSystem
import com.github.raymank26.file.Md5Hash
import com.github.raymank26.file.Md5HashConverter
import com.github.raymank26.file.NavigableReader
import com.github.raymank26.file.getFilenameWithoutExtension
import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVParser
import org.apache.commons.csv.CSVRecord
import org.slf4j.LoggerFactory
import java.nio.file.Path
import java.util.Properties

private val LOG = LoggerFactory.getLogger(DatasetMetadataProvider::class.java)

/**
 * Date: 2019-06-09.
 */
class DatasetMetadataProvider(private val fileSystem: FileSystem,
                              private val dataReaderProvider: ContentDataProvider,
                              private val indexManager: IndexesManager) {

    fun getOrCreate(dataPath: Path): DatasetMetadata {
        val metadataPath = getMetadataPath(dataPath)
        val metadata = readMetadataContent(metadataPath, dataPath)
        if (metadata != null && metadata.csvMd5 == fileSystem.getMd5(dataPath)) {
            return metadata
        }
        return createAndSaveMetadata(metadataPath, dataPath)
    }

    private fun getMetadataPath(dataPath: Path): Path {
        return dataPath.parent.resolve(getFilenameWithoutExtension(dataPath) + ".meta")
    }

    private fun readMetadataContent(metadataPath: Path, dataPath: Path): DatasetMetadata? {
        if (!fileSystem.isFileExists(metadataPath)) {
            return null
        }
        return try {
            val prop = Properties()
            prop.load(fileSystem.getNavigableReader(metadataPath).asReader())
            val md5: Md5Hash = Md5HashConverter.INSTANCE.deserialize(prop.getProperty("md5"))
            val columns: List<ColumnInfo> = prop.getProperty("columns")
                    .split(";")
                    .map { pair ->
                        val (type, name) = pair.split(",")
                        ColumnInfo(FieldType.MARK_TO_FIELD_TYPE.getValue(type.toByte()), name)
                    }
            DatasetMetadata(columns, indexManager.loadIndexes(dataPath), md5)
        } catch (e: Exception) {
            LOG.warn("Unable to read metadata for path = $metadataPath", e)
            null
        }
    }

    private fun createAndSaveMetadata(metadataPath: Path, dataPath: Path): DatasetMetadata {
        val headers = fileSystem.getNavigableReader(dataPath).use { dataReaderProvider.header(it) }

        return fileSystem.getNavigableReader(dataPath).use { handle ->
            val currentResult: MutableList<FieldType?> = headers.map { null }.toMutableList()
            dataReaderProvider.get(handle).use {
                it.forEach { row ->
                    row.columns.withIndex().forEach { value ->
                        currentResult[value.index] = nextFieldType(currentResult[value.index], value.value)
                    }
                }
            }
            for ((i, value) in currentResult.withIndex()) {
                if (value == null) {
                    currentResult[i] = FieldType.STRING
                }
            }
            val prop = Properties()
            val md5 = fileSystem.getMd5(dataPath)
            prop.setProperty("md5", Md5HashConverter.INSTANCE.serialize(md5))
            val columnInfos = headers.asSequence()
                    .withIndex().map { (index, value) ->
                        val type = currentResult[index] ?: FieldType.STRING
                        ColumnInfo(type, value)
                    }
                    .toList()
            val columns: String = columnInfos.joinToString(separator = ";") { value ->
                "${value.type.mark},${value.fieldName}"
            }

            prop.setProperty("columns", columns)
            fileSystem.getOutputStream(metadataPath).use {
                prop.store(it, null)
            }
            DatasetMetadata(columnInfos, indexManager.loadIndexes(dataPath), md5)
        }
    }

    private fun nextFieldType(prevType: FieldType?, nextValue: String?): FieldType? {
        val nextType = guessColumnInfo(nextValue)
        return when {
            prevType == null -> nextType
            nextType == null -> prevType
            prevType == FieldType.STRING || nextType == FieldType.STRING -> FieldType.STRING
            prevType == FieldType.LONG && nextType == FieldType.DOUBLE -> FieldType.DOUBLE
            prevType == FieldType.DOUBLE && nextType == FieldType.LONG -> FieldType.DOUBLE
            else -> nextType
        }
    }

    private fun guessColumnInfo(value: String?): FieldType? {
        return when {
            value == null -> null
            value.contains('.') && value.toFloatOrNull() != null -> FieldType.DOUBLE
            value.toLongOrNull() != null -> FieldType.LONG
            else -> FieldType.STRING
        }
    }
}

interface ContentDataProvider {
    fun header(reader: NavigableReader): List<String>
    fun get(reader: NavigableReader): ClosableSequence<ContentRow>
    fun get(reader: NavigableReader, offsets: List<Long>): ClosableSequence<ContentRow>
}

data class ContentRow(val columns: List<String?>, val firstCharacterPosition: Long)

class CsvContentDataProvider(private val csvFormat: CSVFormat) : ContentDataProvider {
    override fun header(reader: NavigableReader): List<String> {
        val firstRecord: CSVRecord = CSVParser(reader.asReader(), csvFormat).firstOrNull() ?: return emptyList()
        val res = recordToRow(firstRecord).columns.filterNotNull()
        if (firstRecord.size() != res.size) {
            throw IllegalStateException("Header has nulls")
        }
        return res
    }

    override fun get(reader: NavigableReader): ClosableSequence<ContentRow> {
        val iterator = CSVParser(reader.asReader(), csvFormat).iterator()
        if (!iterator.hasNext()) {
            return ClosableSequence(emptySequence(), reader)
        }
        iterator.next()
        return ClosableSequence(iterator.asSequence().map { recordToRow(it) }, reader)
    }

    override fun get(reader: NavigableReader, offsets: List<Long>): ClosableSequence<ContentRow> {
        val rows = offsets.map { offset ->
            reader.seek(offset)
            val iterator = CSVParser(reader.asReader(), csvFormat).iterator()
            recordToRow(iterator.next())
        }
        return ClosableSequence(rows.asSequence(), reader)
    }

    private fun recordToRow(record: CSVRecord): ContentRow {
        return ContentRow((0 until record.size()).map { record[it] }, record.characterPosition)
    }
}

data class DatasetOffset(val charPosition: Long, val byteOffset: Long)

data class DatasetMetadata(val columnInfos: List<ColumnInfo>,
                           val indexes: List<IndexDescriptionAndPath>,
                           val csvMd5: Md5Hash)



