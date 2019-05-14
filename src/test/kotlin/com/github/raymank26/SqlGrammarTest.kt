package com.github.raymank26

import org.junit.Assert
import org.junit.Test
import java.lang.Exception

/**
 * Date: 2019-05-11.
 */
class SqlGrammarTest {

    private val sqlParser = SqlAstBuilder()

    @Test
    fun testSimpleSelect() {
        testParser("SELECT a FROM 'b'")
    }

    @Test
    fun testSelectWithWhere() {
        testParser("SELECT a FROM 'b' WHERE a < 5")
    }

    @Test
    fun testSelectWithWhere2() {
        testParser("SELECT a FROM '5' WHERE a < 5 AND b > 4 OR b LIKE '5'")
    }

    @Test
    fun testSelectWithWhere3() {
        testParser("SELECT '5' FROM 'a' WHERE (a IN (1,2,3) AND 5 = 5) OR 4 = 4")
    }

    @Test
    fun testCreateIndex() {
        testParser("CREATE INDEX FOO ON 'foo' (a)")
    }

    @Test
    fun testFailure() {
        testParser("select a", true)
    }

    @Test
    fun testLowercase() {
        testParser("select a from 'b'")
    }

    private fun testParser(statement: String, failureExpected: Boolean = false) {
        try {
            sqlParser.parse(statement)
            if (failureExpected) {
                Assert.fail("Failure expected, but it passes")
            }
        } catch (e: Exception) {
            if (!failureExpected) {
                Assert.fail(e.toString())
            }
        }
    }
}