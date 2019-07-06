package com.github.raymank26.csvsh

import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.lmdbjava.CursorIterator
import org.lmdbjava.Dbi
import org.lmdbjava.DbiFlags
import org.lmdbjava.Env
import org.lmdbjava.EnvFlags
import org.lmdbjava.KeyRange
import java.nio.ByteBuffer
import java.nio.file.Paths
import kotlin.test.assertEquals

/**
 * Date: 2019-05-20.
 */
class SqlExecutorTest : SqlTestUtils() {

    @Rule
    @JvmField
    val tmp = TemporaryFolder()

    @Test
    fun simple() {
        val dataset = executeSelect("SELECT a FROM '/test/input.csv' WHERE b = 1", getDefaultDatasetFactory())
        checkDatasetResult(dataset) { rows ->
            assertEquals(1, rows.size)
        }
    }

    @Test
    fun testLike() {
        val dataset = executeSelect("SELECT * FROM '/test/input.csv' WHERE a LIKE '%ba%' AND c < 3.2 AND b <= 2",
                getDefaultDatasetFactory())
        checkDatasetResult(dataset) { rows ->
            assertEquals(3, rows.size)
        }
    }

    @Test
    fun testEqNull() {
        val dataset = executeSelect("SELECT * FROM '/test/input.csv' WHERE a = null OR b = null",
                getDefaultDatasetFactory())
        checkDatasetResult(dataset) { rows ->
            assertEquals(3, rows.size)
        }
    }

    @Test
    fun testIn() {
        val dataset = executeSelect("SELECT * FROM '/test/input.csv' WHERE a IN ('bazz', 'баз') AND c IN (-1.0) AND b NOT IN (2)",
                getDefaultDatasetFactory())
        val datasetList = dataset.rows.toList()
        assertEquals(1, datasetList.size)
        assertEquals(listOf(StringValue("bazz"), LongValue(null), DoubleValue(-1.0)), datasetList.first().columns)
        println(prettifyDataset(dataset.copy(ClosableSequence(datasetList.asSequence()))))
    }

    @Test
    fun testNotLike() {
        val dataset = executeSelect("SELECT * FROM '/test/input.csv' WHERE a NOT LIKE 'bazz' AND a NOT LIKE 'баз'",
                getDefaultDatasetFactory())
        checkDatasetResult(dataset) { rows ->
            assertEquals(4, rows.size)
        }
    }

    @Test
    fun testLikeWithIndexes() {
        indexesManager.createIndex(Paths.get("/test/input.csv"), "bIndex", "b", getDefaultDatasetFactory())
        val indexes = indexesManager.loadIndexes(Paths.get("/test/input.csv"))
        indexes[0].indexContent.value.use { index ->
            assertEquals(4, index.lessThanEq(LongValue(2)).size)
        }
        indexesManager.createIndex(Paths.get("/test/input.csv"), "cIndex", "c", getDefaultDatasetFactory())
        val dataset = executeSelect("SELECT * FROM '/test/input.csv' WHERE a LIKE '%ba%' AND c < 3.2 AND b <= 2", getDefaultDatasetFactory())

        checkDatasetResult(dataset) { rows ->
            assertEquals(3, rows.size)
        }
    }

    @Test
    fun testGroupBy() {
        val dataset = executeSelect("SELECT a, SUM(b), MIN(b), MAX(b), COUNT(b), SUM(c) FROM '/test/input.csv' GROUP BY a ORDER BY SUM(b) DESC", getDefaultDatasetFactory())
        checkDatasetResult(dataset) { rows ->
            assertEquals(5, rows.size)
        }
    }

    @Test
    fun testSelectCount() {
        val dataset = executeSelect("SELECT count(*) from '/test/input.csv'", getDefaultDatasetFactory())
        checkDatasetResult(dataset) { rows ->
            assertEquals(1, rows.size)
        }
    }

    private inline fun checkDatasetResult(dataset: DatasetResult, assert: (List<DatasetRow>) -> Unit) {
        val datasetList = dataset.rows.toList()
        assert.invoke(datasetList)
        println(prettifyDataset(dataset.copy(ClosableSequence(datasetList.asSequence()))))
    }

    @Test
    fun lmdbTest() {
        val tmpFile = tmp.newFile()

        val env = Env.create()
                .setMapSize(10_485_760)
                .setMaxDbs(100)
                .open(tmpFile, EnvFlags.MDB_NOSUBDIR)

        val db: Dbi<ByteBuffer> = env.openDbi("foobar", DbiFlags.MDB_CREATE, DbiFlags.MDB_DUPSORT)

        env.txnWrite().use { txn ->
            db.put(txn, serializeLong(-1L), serializeLong(2L))
            db.put(txn, serializeLong(2L), serializeLong(3L))
            txn.commit()
        }

        env.txnRead().use { txn ->
            db.iterate(txn, KeyRange.atMost<ByteBuffer>(serializeLong(2))).use { cursorIterator ->
                for (keyVal: CursorIterator.KeyVal<ByteBuffer> in cursorIterator.iterable()) {
                    println(deserializeLong(keyVal.`val`()))
                }
            }
        }
        env.txnWrite().use { txn ->
            db.drop(txn)
            txn.commit()
        }
    }

    private fun serializeLong(value: Long): ByteBuffer {
        val bb = ByteBuffer.allocateDirect(8)
        bb.putLong(value)
        bb.flip()
        return bb
    }

    private fun deserializeLong(value: ByteBuffer): Long {
        return value.getLong(0)
    }
}