package com.github.raymank26.csvsh.parser

import com.github.raymank26.csvsh.SyntaxException
import com.github.raymank26.csvsh.util.CaseChangingCharStream
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
import java.util.BitSet

/**
 * Date: 2019-05-13.
 */
class SqlAstBuilder {

    fun parse(sqlStatement: String): StatementType {
        val cts = CommonTokenStream(SqlLexer(CaseChangingCharStream(CharStreams.fromString(sqlStatement), true)))
        val parser = SqlParser(cts)
        var unknownExceptionOccurred = false
        parser.addErrorListener(object : BaseErrorListener() {
            override fun reportAttemptingFullContext(recognizer: Parser?, dfa: DFA?, startIndex: Int, stopIndex: Int, conflictingAlts: BitSet?, configs: ATNConfigSet?) {
                unknownExceptionOccurred = true
            }

            override fun syntaxError(recognizer: Recognizer<*, *>?, offendingSymbol: Any?, line: Int, charPositionInLine: Int, msg: String?, e: RecognitionException?) {
                throw SyntaxException("Unable to parse statement, line: $line, charPosition: $charPositionInLine")
            }

            override fun reportAmbiguity(recognizer: Parser?, dfa: DFA?, startIndex: Int, stopIndex: Int, exact: Boolean, ambigAlts: BitSet?, configs: ATNConfigSet?) {
                unknownExceptionOccurred = true
            }

            override fun reportContextSensitivity(recognizer: Parser?, dfa: DFA?, startIndex: Int, stopIndex: Int, prediction: Int, configs: ATNConfigSet?) {
                unknownExceptionOccurred = true
            }
        })
        val parse = parser.parse()
        if (unknownExceptionOccurred) {
            throw RuntimeException("Exception while executing parser")
        }
        return when {
            parse?.statement()?.select() != null -> SelectStatement(parse.statement().select())
            parse?.statement()?.createIndex() != null -> CreateIndexType(parse.statement().createIndex())
            parse?.statement()?.dropIndex() != null -> DropIndexType(parse.statement().dropIndex())
            parse?.statement()?.describeTable() != null -> DescribeTable(parse.statement().describeTable())
            parse?.statement()?.describeSelect() != null -> DescribeSelect(parse.statement().describeSelect())
            else -> throw RuntimeException("Unable to find appropriate construction")
        }
    }
}

sealed class StatementType

data class CreateIndexType(val ctx: SqlParser.CreateIndexContext) : StatementType()

data class DropIndexType(val ctx: SqlParser.DropIndexContext) : StatementType()

data class SelectStatement(val ctx: SqlParser.SelectContext) : StatementType()

data class DescribeTable(val ctx: SqlParser.DescribeTableContext) : StatementType()

data class DescribeSelect(val ctx: SqlParser.DescribeSelectContext) : StatementType()
