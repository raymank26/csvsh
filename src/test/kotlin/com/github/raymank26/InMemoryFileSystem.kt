package com.github.raymank26

import com.github.raymank26.file.FileSystem
import com.github.raymank26.file.NavigableReader
import org.mapdb.DB
import org.mapdb.DBMaker
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.io.InputStreamReader
import java.io.OutputStream
import java.io.Reader
import java.nio.file.Path

/**
 * Date: 2019-06-13.
 */
class InMemoryFileSystem(private val contentMapping: Map<Path, String>, private val indexesMapping: Map<Path, Path>) : FileSystem {

    val outputMapping: MutableMap<Path, ByteArrayOutputStream> = mutableMapOf()

    constructor(contentMapping: Map<Path, String>) : this(contentMapping, emptyMap())

    override fun isFileExists(path: Path): Boolean {
        return contentMapping.containsKey(path) || indexesMapping.containsKey(path)
    }

    override fun getDB(path: Path): DB {
        return requireNotNull(indexesMapping[path]?.let { DBMaker.fileDB(it.toFile()).make() }) { "Index for path does not exist" }
    }

    override fun getNavigableReader(path: Path): NavigableReader {
        return requireNotNull(getBytes(path)?.let { ByteArrayFile(it) }) { "Path doesn't exist, path = $path" }
    }

    override fun getInputStream(path: Path): InputStream {
        return requireNotNull(getBytes(path)?.let { ByteArrayInputStream(it) }) { "Path doesn't exist, path = $path" }
    }

    override fun getOutputStream(path: Path): OutputStream {
        return outputMapping.compute(path) { _, prev ->
            prev ?: ByteArrayOutputStream()
        }!!
    }

    private fun getBytes(path: Path): ByteArray? {
        return contentMapping[path]?.toByteArray() ?: outputMapping[path]?.toByteArray()
    }
}

class ByteArrayFile(private val buf: ByteArray) : NavigableReader {

    private var reader = InputStreamReader(ByteArrayInputStream(buf, 0, buf.size))

    override fun asReader(): Reader {
        return reader
    }

    override fun seek(offset: Long) {
        reader = InputStreamReader(ByteArrayInputStream(buf, offset.toInt(), buf.size))
    }

    override fun close() {
    }
}
