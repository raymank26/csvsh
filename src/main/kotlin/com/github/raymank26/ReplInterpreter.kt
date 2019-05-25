package com.github.raymank26

import com.jakewharton.fliptables.FlipTable
import java.io.BufferedReader
import java.io.InputStreamReader

/**
 * Date: 2019-05-17.
 */
class ReplInterpreter {

    fun run() {
        val reader = BufferedReader(InputStreamReader(System.`in`))
        val engine = ExecutorEngine()
        while (true) {
            System.out.print("csvsh>>> ")
            val cmd = reader.readLine()
            when (val response = engine.execute(cmd)) {
                is TextResponse -> println(response.value)
                is VoidResponse -> Unit
            }
        }
    }
}

fun prettifyDataset(dataset: DatasetResult): String {
    val header = dataset.columnInfo.map { it.fieldName }.toTypedArray()
    val rows = dataset.rows.asSequence().take(20).map { it.columns.toTypedArray() }.toList().toTypedArray()

    return FlipTable.of(header, rows)
}