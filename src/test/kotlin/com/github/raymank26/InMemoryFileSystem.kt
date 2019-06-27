package com.github.raymank26

import com.github.raymank26.file.FileSystem
import com.github.raymank26.file.NavigableReader
import org.lmdbjava.Env
import org.lmdbjava.EnvFlags
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.io.InputStreamReader
import java.io.OutputStream
import java.io.Reader
import java.nio.ByteBuffer
import java.nio.file.Files
import java.nio.file.Path

/**
 * Date: 2019-06-13.
 */
class InMemoryFileSystem(private val contentMapping: Map<Path, String>, private val indexesMapping: Map<Path, Path>) : FileSystem {
    val outputMapping: MutableMap<Path, ByteArrayOutputStream> = mutableMapOf()

    constructor(contentMapping: Map<Path, String>) : this(contentMapping, emptyMap())

    override fun isFileExists(path: Path): Boolean {
        if (contentMapping.containsKey(path)) {
            return true
        }
        val indexPath = indexesMapping[path]
        return indexPath != null && Files.exists(indexPath)
    }

    override fun getDB(path: Path): Env<ByteBuffer> {
        return requireNotNull(indexesMapping[path]?.let {
            if (!Files.exists(it)) {
                Files.createFile(it)
            }
            Env.create()
                    .setMapSize(10_485_760)
                    .setMaxDbs(100)
                    .open(it.toFile(), EnvFlags.MDB_NOSUBDIR)
        }) { "Index for path does not exist" }
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

    override fun getSize(path: Path): Long {
        throw UnsupportedOperationException()
    }
}

class ByteArrayFile(private val buf: ByteArray) : NavigableReader {
    override fun probe(length: Int): String {
        throw UnsupportedOperationException()
    }

    private var inputStream = CountableInputStream(buf, 0, buf.size)

    private var reader = InputStreamReader(inputStream)
    override fun asReader(): Reader {
        return reader
    }

    override fun seek(offset: Long) {
        inputStream = CountableInputStream(buf, offset.toInt(), buf.size)
        reader = InputStreamReader(inputStream)
    }

    override fun getByteOffset(): Long {
        return inputStream.getPos().toLong()
    }

    override fun getEncoding(): String {
        return reader.encoding
    }

    override fun close() {
    }
}

private class CountableInputStream(buf: ByteArray, offset: Int, length: Int) : ByteArrayInputStream(buf, offset, length) {
    fun getPos(): Int {
        return pos
    }
}
