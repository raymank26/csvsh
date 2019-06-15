package com.github.raymank26

import com.github.raymank26.file.FileSystem
import com.github.raymank26.file.Md5Hash
import com.github.raymank26.file.getFilenameWithoutExtension
import com.google.common.io.BaseEncoding
import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVParser
import org.apache.commons.csv.CSVRecord
import org.slf4j.LoggerFactory
import java.io.Reader
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
            prop.load(fileSystem.getInputStream(metadataPath))
            val md5: ByteArray = BaseEncoding.base16().lowerCase().decode(prop.getProperty("md5"))
            val columns: List<ColumnInfo> = prop.getProperty("columns")
                    .split(";")
                    .map { pair ->
                        val (type, name) = pair.split(",")
                        ColumnInfo(FieldType.MARK_TO_FIELD_TYPE.getValue(type.toByte()), name)
                    }
            DatasetMetadata(columns, indexManager.loadIndexes(dataPath), Md5Hash(md5))
        } catch (e: Exception) {
            LOG.warn("Unable to read metadata for path = $metadataPath", e)
            null
        }
    }

    private fun createAndSaveMetadata(metadataPath: Path, dataPath: Path): DatasetMetadata {
        val headers = fileSystem.getReader(dataPath).use { dataReaderProvider.header(it) }

        return fileSystem.getReader(dataPath).use { handle ->
            val currentResult: MutableList<FieldType?> = headers.map { null }.toMutableList()
            dataReaderProvider.get(handle).use {
                it.iterator().forEach { columns ->
                    columns.withIndex().forEach { value ->
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
            prop.setProperty("md5", BaseEncoding.base16().lowerCase().encode(md5.content))
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
            prevType == FieldType.STRING || nextType == FieldType.STRING -> FieldType.STRING
            prevType == FieldType.INTEGER && nextType == FieldType.FLOAT -> FieldType.FLOAT
            else -> nextType
        }
    }

    private fun guessColumnInfo(value: String?): FieldType? {
        return when {
            value == null -> null
            value.contains('.') && value.toFloatOrNull() != null -> FieldType.FLOAT
            value.toIntOrNull() != null -> FieldType.INTEGER
            else -> FieldType.STRING
        }
    }
}

interface ContentDataProvider {
    fun header(reader: Reader): List<String>
    fun get(reader: Reader): ClosableIterator<List<String?>>
}

class CsvContentDataProvider(private val csvFormat: CSVFormat) : ContentDataProvider {
    override fun header(reader: Reader): List<String> {
        val firstRecord: CSVRecord = CSVParser(reader, csvFormat).firstOrNull() ?: return emptyList()
        val res = toList(firstRecord).filterNotNull()
        if (firstRecord.size() != res.size) {
            throw IllegalStateException("Header has nulls")
        }
        return res
    }

    private fun toList(record: CSVRecord): List<String?> {
        return (0 until record.size()).map { record[it] }
    }

    override fun get(reader: Reader): ClosableIterator<List<String?>> {
        val iterator = CSVParser(reader, csvFormat).iterator()
        if (!iterator.hasNext()) {
            return ClosableIterator(emptyList<List<String?>>().iterator(), reader)
        }
        iterator.next()
        return ClosableIterator(iterator, reader).map {
            toList(it)
        }
    }
}


data class DatasetMetadata(val columnInfos: List<ColumnInfo>,
                           val indexes: List<IndexDescriptionAndPath>,
                           val csvMd5: Md5Hash)



