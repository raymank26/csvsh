package com.github.raymank26

import com.github.raymank26.file.FileSystem
import com.github.raymank26.file.Md5Hash
import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVParser
import org.apache.commons.csv.CSVRecord
import org.mapdb.BTreeMap
import org.mapdb.Serializer
import org.slf4j.LoggerFactory
import java.io.Reader
import java.nio.file.Files
import java.nio.file.Path
import java.util.Properties

private val LOG = LoggerFactory.getLogger(DatasetMetadataProvider::class.java)

/**
 * Date: 2019-06-09.
 */
class DatasetMetadataProvider(private val fileSystem: FileSystem, private val dataReaderProvider: ContentDataProvider) {

    fun getOrCreate(dataPath: Path): DatasetMetadata {
        val metadataPath = getMetadataPath(dataPath)
        val metadata = readMetadataContent(metadataPath, dataPath)
        if (metadata != null && metadata.csvMd5 == fileSystem.getMd5(dataPath)) {
            return metadata
        }
        return createAndSaveMetadata(metadataPath, dataPath)
    }

    private fun getMetadataPath(dataPath: Path): Path {
        val filename = dataPath.fileName.toString()
        val metaFilename = (if (filename.contains('.')) {
            filename.substring(0, filename.indexOf('.'))
        } else {
            filename
        }) + ".meta"
        return dataPath.parent.resolve(metaFilename)
    }

    private fun readMetadataContent(metadataPath: Path, dataPath: Path): DatasetMetadata? {
        if (!fileSystem.isFileExists(metadataPath)) {
            return null
        }
        return try {
            val prop = Properties()
            prop.load(fileSystem.getInputStream(metadataPath))
            val md5 = prop.getProperty("md5")
            val columns: List<ColumnInfo> = prop.getProperty("columns")
                    .split(";")
                    .map { pair ->
                        val (name, type) = pair.split(",")
                        ColumnInfo(FieldType.MARK_TO_FIELD_TYPE.getValue(type.toByte()), name)
                    }
            DatasetMetadata(columns, loadIndexes(dataPath), Md5Hash(md5.toByteArray()))
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
                it.map { columns ->
                    columns.withIndex().forEach { value ->
                        currentResult[value.index] = nextFieldType(currentResult[value.index], value.value)
                    }
                }
            }
            val prop = Properties()
            val md5 = fileSystem.getMd5(dataPath)
            prop.setProperty("md5", md5.content.toString())
            val columnInfos = headers.asSequence()
                    .withIndex().map { (index, value) ->
                        val type = currentResult[index] ?: FieldType.STRING
                        ColumnInfo(type, value)
                    }
                    .toList()
            val columns: String = columnInfos
                    .withIndex().joinToString(separator = ";") { (index, value) ->
                        val type = currentResult[index] ?: FieldType.STRING
                        "${type.mark},$value"
                    }

            prop.setProperty("columns", columns)
            fileSystem.getOutputStream(metadataPath).use {
                prop.store(it, null)
            }
            DatasetMetadata(columnInfos, loadIndexes(dataPath), md5)
        }
    }

    private fun nextFieldType(prevType: FieldType?, nextValue: String?): FieldType {
        val nextType = guessColumnInfo(nextValue)
        return when {
            prevType == null -> nextType
            prevType == FieldType.STRING || nextType == FieldType.STRING -> FieldType.STRING
            prevType == FieldType.INTEGER && nextType == FieldType.FLOAT -> FieldType.FLOAT
            else -> nextType
        }
    }

    private fun guessColumnInfo(value: String?): FieldType {
        return when {
            value == null -> FieldType.STRING
            value.contains('.') && value.toFloatOrNull() != null -> FieldType.FLOAT
            value.toIntOrNull() != null -> FieldType.INTEGER
            else -> FieldType.STRING
        }
    }

    private fun loadIndexes(csvPath: Path): List<IndexDescriptionAndPath> {
        val indexFile: Path = csvPathToIndexFile(csvPath)
        if (!Files.exists(indexFile)) {
            return emptyList()
        }
        val db = fileSystem.getDB(indexFile)
        val result = mutableListOf<IndexDescriptionAndPath>()
        for (name in db.getAllNames()) {
            val (indexName, fieldName, fieldTypeMark) = name.split("|")

            val byteMark = fieldTypeMark.toByte()
            val serializer = when (FieldType.MARK_TO_FIELD_TYPE[byteMark]) {
                FieldType.INTEGER -> Serializer.INTEGER
                FieldType.FLOAT -> Serializer.FLOAT
                FieldType.STRING -> Serializer.STRING
                else -> throw RuntimeException("Unable to get field type by mark = $byteMark")
            }
            val tm = db.treeMap(name, serializer, Serializer.INT_ARRAY).open()
            @Suppress("UNCHECKED_CAST")
            val readOnlyIndex = MapDBReadonlyIndex(tm as BTreeMap<in Any, IntArray>, FieldType.INTEGER)
            result.add(IndexDescriptionAndPath(IndexDescription(indexName, fieldName), readOnlyIndex))
        }
        return result
    }

    private fun csvPathToIndexFile(csvPath: Path): Path {
        return csvPath.parent.resolve("indexContent")
    }
}

interface ContentDataProvider {
    fun header(reader: Reader): List<String>
    fun get(reader: Reader): ClosableIterator<List<String?>>
}

class CsvContentDataProvider(private val csvFormat: CSVFormat) : ContentDataProvider {
    override fun header(reader: Reader): List<String> {
        val firstRecord: CSVRecord = CSVParser(reader, csvFormat).first()
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
        iterator.next()
        return ClosableIterator(iterator, reader).map { toList(it) }
    }
}


data class DatasetMetadata(val columnInfos: List<ColumnInfo>,
                           val indexes: List<IndexDescriptionAndPath>,
                           val csvMd5: Md5Hash)


