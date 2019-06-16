package com.github.raymank26

import org.apache.commons.csv.CSVFormat
import org.junit.After
import java.nio.file.Files
import java.nio.file.Paths
import kotlin.test.assertEquals
import kotlin.test.fail

/**
 * Date: 2019-05-20.
 */
private val sqlAstBuilder = SqlAstBuilder()
private val sqlPlanner = SqlPlanner()
private val sqlExecutor = SqlExecutor()

private val testInput = """
    a,b,c
    foobar,1,3.0
    bazz,null,-1.0
    null,10,3.4
    null,-5,8.2
    baz,11,null
    bazz,2,-1
    foobarbaz,2,-1
""".trimIndent()

val dataPath = Paths.get("/test/input.csv")
val indexPath = Paths.get("/test/input.index")
val realIndexFilePath = Paths.get("/tmp/csvTmpIndex.tmp")
val fileSystem = InMemoryFileSystem(mapOf(Pair(dataPath, testInput)), mapOf(Pair(indexPath, realIndexFilePath)))

val dataProvider = CsvContentDataProvider(CSVFormat.RFC4180.withNullString("null"))
val indexesManager = IndexesManager(fileSystem)
val metadataProvider = DatasetMetadataProvider(fileSystem, dataProvider, indexesManager)
val readerFactory = FilesystemDatasetReaderFactory(metadataProvider, fileSystem, dataProvider)

abstract class SqlTestUtils {

    fun getDefaultDatasetFactory(): DatasetReaderFactory {
        return readerFactory
    }

    fun testParser(statement: String) {
        sqlAstBuilder.parse(statement)
    }

    fun makePlan(sql: String, datasetReaderFactory: DatasetReaderFactory = getDefaultDatasetFactory()): SqlPlan {
        return sqlPlanner.createPlan((sqlAstBuilder.parse(sql) as SelectStatement).ctx, datasetReaderFactory)
    }

    fun executeSelect(sql: String, datasetReaderFactory: DatasetReaderFactory): DatasetResult {
        val plan = makePlan(sql, datasetReaderFactory)
        return sqlExecutor.execute(plan)
    }

    fun testFailure(exceptionClazz: Class<*>? = null, r: () -> Unit) {
        try {
            r()
            fail("Passed function has been executed normally, but failure has been expected")
        } catch (e: Exception) {
            if (exceptionClazz != null) {
                assertEquals(exceptionClazz, e.javaClass)
            }
        }
    }

    @After
    fun deleteRealFile() {
        if (Files.exists(realIndexFilePath)) {
            Files.delete(realIndexFilePath)
        }
    }
}

