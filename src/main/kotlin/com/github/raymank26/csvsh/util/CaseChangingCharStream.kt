package com.github.raymank26.csvsh.util

import org.antlr.v4.runtime.CharStream
import org.antlr.v4.runtime.misc.Interval

/**
 * Date: 2019-05-11.
 */
class CaseChangingCharStream(private val stream: CharStream, private val upper: Boolean) : CharStream {

    override fun getText(interval: Interval): String {
        return stream.getText(interval)
    }

    override fun consume() {
        stream.consume()
    }

    override fun LA(i: Int): Int {
        val c = stream.LA(i)
        if (c <= 0) {
            return c
        }
        return if (upper) {
            Character.toUpperCase(c)
        } else Character.toLowerCase(c)
    }

    override fun mark(): Int {
        return stream.mark()
    }

    override fun release(marker: Int) {
        stream.release(marker)
    }

    override fun index(): Int {
        return stream.index()
    }

    override fun seek(index: Int) {
        stream.seek(index)
    }

    override fun size(): Int {
        return stream.size()
    }

    override fun getSourceName(): String {
        return stream.sourceName
    }
}

