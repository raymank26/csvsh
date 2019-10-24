package com.github.raymank26.csvsh

import com.github.raymank26.csvsh.file.FileSystemException
import com.github.raymank26.csvsh.planner.PlannerException
import com.jakewharton.fliptables.FlipTable
import org.jline.reader.EndOfFileException
import org.jline.reader.LineReader
import org.jline.reader.LineReaderBuilder
import org.jline.reader.UserInterruptException
import org.jline.reader.impl.DefaultParser
import org.jline.reader.impl.completer.StringsCompleter
import org.jline.reader.impl.history.DefaultHistory
import org.jline.terminal.TerminalBuilder
import org.slf4j.LoggerFactory
import java.io.PrintWriter
import java.nio.file.Paths
import java.util.concurrent.TimeUnit

private val LOG = LoggerFactory.getLogger(ReplInterpreter::class.java)

/**
 * Date: 2019-05-17.
 */
class ReplInterpreter {

    private val engine = ExecutorEngine()

    fun runOnce(cmd: String, outputWriter: PrintWriter, tabularAsText: Boolean) {
        execute(cmd, outputWriter, tabularAsText)
    }

    fun runLoop() {
        val terminal = TerminalBuilder.builder()
                .build()
        val outputWriter = terminal.writer()
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

            execute(line, outputWriter, false)
        }
    }

    private fun execute(line: String, outputWriter: PrintWriter, tabularAsText: Boolean) {
        try {
            val startTime = System.nanoTime()
            processResponse(engine.execute(line), outputWriter, tabularAsText)
            LOG.info("Command execution completed in ${TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime)}ms.")
        } catch (e: PlannerException) {
            outputWriter.println("Unable to build plan: ${e.message}")
        } catch (e: ExecutorException) {
            outputWriter.println("Unable to execute statement: ${e.message}")
        } catch (e: SyntaxException) {
            outputWriter.println("Syntax exception: ${e.message}")
        } catch (e: FileSystemException) {
            outputWriter.println("Filesystem exception: ${e.message}")
        } catch (e: Exception) {
            outputWriter.println("Unhandled exception: $e")
            e.printStackTrace(outputWriter)
        }
    }

    private fun processResponse(response: ExecutorResponse, outputWriter: PrintWriter, tabularAsText: Boolean) {
        when (response) {
            is DatasetResponse -> {
                val preparedContent = if (tabularAsText) rawDataset(response.value) else prettifyDataset(response.value)
                outputWriter.print(preparedContent)
            }
            is TextResponse -> outputWriter.println(response.value)
            is VoidResponse -> Unit
            is CompositeResponse -> response.parts.forEach { processResponse(it, outputWriter, tabularAsText) }
        }
    }
}

fun rawDataset(dataset: DatasetResult): String {
    return dataset.rows.map {
        it.columns.joinToString(",") { col -> col.asValue.toString() }
    }.toList().joinToString("\n") + "\n"
}

fun prettifyDataset(dataset: DatasetResult): String {
    val header = dataset.columnInfo.map { it.fieldName }.toTypedArray()
    val rowsLimit = 20
    var rows = dataset.rows.map { row ->
        row.columns.map { col ->
            col.toText()
        }.toTypedArray()
    }.take(rowsLimit + 1).toList()


    val overflow = rows.size == rowsLimit + 1
    if (overflow) {
        rows = rows.dropLast(1)
    }

    return FlipTable.of(header, rows.toList().toTypedArray()) +
            if (overflow) "Shown only first $rowsLimit rows.\n" else "\n"
}