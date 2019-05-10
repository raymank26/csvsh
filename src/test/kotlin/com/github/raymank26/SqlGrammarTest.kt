package com.github.raymank26

import com.github.raymank26.sql.SqlLexer
import com.github.raymank26.sql.SqlParser
import org.antlr.v4.runtime.BaseErrorListener
import org.antlr.v4.runtime.CharStreams
import org.antlr.v4.runtime.CommonTokenStream
import org.antlr.v4.runtime.Parser
import org.antlr.v4.runtime.RecognitionException
import org.antlr.v4.runtime.Recognizer
import org.antlr.v4.runtime.atn.ATNConfigSet
import org.antlr.v4.runtime.dfa.DFA
import org.junit.Assert
import org.junit.Test
import java.util.BitSet

/**
 * Date: 2019-05-11.
 */
class SqlGrammarTest {

    @Test
    fun testSelect() {
        testParser("select * from 'a'")
    }

    private fun testParser(input: String) {
        val parser = SqlParser(CommonTokenStream(SqlLexer(CharStreams.fromString(input))))
        var hasError = false
        parser.addErrorListener(object: BaseErrorListener() {
            override fun reportAttemptingFullContext(recognizer: Parser?, dfa: DFA?, startIndex: Int, stopIndex: Int, conflictingAlts: BitSet?, configs: ATNConfigSet?) {
                hasError = true
            }

            override fun syntaxError(recognizer: Recognizer<*, *>?, offendingSymbol: Any?, line: Int, charPositionInLine: Int, msg: String?, e: RecognitionException?) {
                hasError = true
            }

            override fun reportAmbiguity(recognizer: Parser?, dfa: DFA?, startIndex: Int, stopIndex: Int, exact: Boolean, ambigAlts: BitSet?, configs: ATNConfigSet?) {
                hasError = true
            }

            override fun reportContextSensitivity(recognizer: Parser?, dfa: DFA?, startIndex: Int, stopIndex: Int, prediction: Int, configs: ATNConfigSet?) {
                hasError = true
            }
        })
        val statement = parser.statement()
        if (hasError) {
            println("Tree equals to = ${statement.toStringTree()}")
        }
        Assert.assertFalse(hasError)
    }
}