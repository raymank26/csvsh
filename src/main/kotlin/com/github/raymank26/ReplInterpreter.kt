package com.github.raymank26

import com.jakewharton.fliptables.FlipTable
import org.jline.reader.EndOfFileException
import org.jline.reader.LineReader
import org.jline.reader.LineReaderBuilder
import org.jline.reader.UserInterruptException
import org.jline.reader.impl.DefaultParser
import org.jline.reader.impl.completer.StringsCompleter
import org.jline.reader.impl.history.DefaultHistory
import org.jline.terminal.TerminalBuilder
import java.nio.file.Paths

/**
 * Date: 2019-05-17.
 */
class ReplInterpreter {

    fun run() {
        val engine = ExecutorEngine()
        val terminal = TerminalBuilder.builder()
                .build()
        val lineReader = LineReaderBuilder.builder()
                .terminal(terminal)
                .variable(LineReader.HISTORY_FILE, Paths.get(System.getProperty("user.home"), ".csvsh_history"))
                .history(DefaultHistory())
                .completer(StringsCompleter("SELECT", "DESCRIBE", "FROM", "WHERE", "ORDER BY", "LIMIT", "CREATE", "INDEX"))
                .parser(DefaultParser())
                .build()
        while (true) {
            val line = try {
                lineReader.readLine("csvsh>> ")
            } catch (e: EndOfFileException) {
                return
            } catch (e: UserInterruptException) {
                continue
            }?.trim()
            if (line.isNullOrBlank()) {
                continue
            }

            try {
                when (val response = engine.execute(line)) {
                    is DatasetResponse -> println(prettifyDataset(response.value))
                    is TextResponse -> println(response.value)
                    is VoidResponse -> Unit
                }
            } catch (e: PlannerException) {
                println("Unable to build plan: ${e.message}")
            } catch (e: ExecutorException) {
                println("Unable to execute statement: ${e.message}")
            } catch (e: SyntaxException) {
                println("Syntax exception: ${e.message}")
            }
        }
    }
}

fun prettifyDataset(dataset: DatasetResult): String {
    val header = dataset.columnInfo.map { it.fieldName }.toTypedArray()
    var rows = dataset.rows.map { row ->
        row.columns.map { col ->
            col.toText()
        }.toTypedArray()
    }.toList()

    val rowsLimit = 20
    var overflow = false
    if (rows.count() > rowsLimit) {
        overflow = true
        rows = rows.take(rowsLimit)
    }

    return FlipTable.of(header, rows.toList().toTypedArray()) +
            if (overflow) "Shown only first $rowsLimit rows." else ""
}