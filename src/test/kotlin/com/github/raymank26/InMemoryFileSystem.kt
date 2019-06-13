package com.github.raymank26

import com.github.raymank26.file.FileSystem
import org.mapdb.DB
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.io.OutputStream
import java.nio.file.Path

/**
 * Date: 2019-06-13.
 */
class InMemoryFileSystem(private val contentMapping: Map<Path, String>, private val indexesMapping: Map<Path, DB>) : FileSystem {

    val outputMapping: MutableMap<Path, ByteArrayOutputStream> = mutableMapOf()

    constructor(contentMapping: Map<Path, String>) : this(contentMapping, emptyMap())

    override fun isFileExists(path: Path): Boolean {
        return contentMapping.containsKey(path) || indexesMapping.containsKey(path)
    }

    override fun getDB(path: Path): DB {
        return requireNotNull(indexesMapping[path]) { "Index for path does not exist" }
    }

    override fun getInputStream(path: Path): InputStream {
        return requireNotNull(contentMapping[path]?.let { ByteArrayInputStream(it.toByteArray()) }) { "Path doesn't exist, path = $path" }
    }

    override fun getOutputStream(path: Path): OutputStream {
        return outputMapping.compute(path) { _, prev ->
            prev ?: ByteArrayOutputStream()
        }!!
    }
}