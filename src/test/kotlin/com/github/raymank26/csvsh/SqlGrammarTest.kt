package com.github.raymank26.csvsh

import org.junit.Test

/**
 * Date: 2019-05-11.
 */
class SqlGrammarTest : SqlTestUtils() {

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
        testParser("SELECT * FROM 'a' WHERE (a IN (1,2,3) AND 5 = 5) OR 4 = 4")
    }

    @Test
    fun testDescribeSelect() {
        testParser("DESCRIBE SELECT * FROM 'a' WHERE (a IN (1,2,3) AND 5 = 5) OR 4 = 4")
    }

    @Test
    fun testDescribeSelectNegative() {
        testParser("SELECT * FROM 'a' WHERE (a NOT IN (1,2,3) AND 5 <> 5) OR 4 <> 4 OR b NOT LIKE 'foo'")
    }

    @Test
    fun testCreateIndex() {
        testParser("CREATE INDEX FOO ON 'foo' (a)")
    }

    @Test
    fun testDropIndex() {
        testParser("DROP INDEX FOO ON 'foo'")
    }

    @Test
    fun testDescribe() {
        testParser("DESCRIBE TABLE 'foo'")
    }

    @Test
    fun testEmptyString() {
        testParser("select * from 'a' where b = ''")
    }

    @Test(expected = Exception::class)
    fun testFailure() {
        testParser("select a")
    }

    @Test(expected = Exception::class)
    fun testFailureUnrecognizedToken() {
        testParser("SELECT * FROM 'a' WHERE round = 'seed' AND state != 'CA'")
    }

    @Test
    fun testSelectCount() {
        testParser("SELECT count(*) FROM 'a'")
    }

    @Test
    fun testLowercase() {
        testParser("select a from 'b'")
    }

    @Test
    fun testWhitespaceString() {
        testParser("SELECT * FROM 'a' WHERE artist LIKE '% Ensemble'")
    }
}