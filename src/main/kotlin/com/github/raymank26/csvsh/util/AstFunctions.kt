package com.github.raymank26.csvsh.util

import com.github.raymank26.csvsh.sql.SqlParser
import java.nio.file.Path
import java.nio.file.Paths

/**
 * Date: 2019-07-16.
 */
fun SqlParser.TableContext.toPath(): Path {
    return Paths.get(this.IDENTIFIER_Q().text.drop(1).dropLast(1))
}
