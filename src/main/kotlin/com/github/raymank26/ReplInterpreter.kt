package com.github.raymank26

import java.io.BufferedReader
import java.io.InputStreamReader

/**
 * Date: 2019-05-17.
 */
class ReplInterpreter {

    fun run() {
        val reader = BufferedReader(InputStreamReader(System.`in`))
        val engine = ExecutorEngine()
        val currentDirectory = System.getProperty("user.dir")
        while (true) {
            System.out.println("csvsh>>> ")
            val cmd = reader.readLine()
            when (val response = engine.execute(cmd)) {
                is TextResponse -> println(response.value)
                is VoidResponse -> Unit
            }
        }
    }
}