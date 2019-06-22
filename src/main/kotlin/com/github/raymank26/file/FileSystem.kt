package com.github.raymank26.file

import com.google.common.hash.Funnels
import com.google.common.hash.Hashing
import com.google.common.io.ByteStreams
import org.mapdb.DB
import java.io.InputStream
import java.io.OutputStream
import java.io.Reader
import java.nio.file.Path

interface FileSystem {

    fun isFileExists(path: Path): Boolean

    fun getDB(path: Path): DB

    fun getNavigableReader(path: Path): NavigableReader

    fun getInputStream(path: Path): InputStream

    fun getOutputStream(path: Path): OutputStream

    fun getSize(path: Path): Long

    fun getMd5(path: Path): Md5Hash {
        val hashing = Hashing.md5()
                .newHasher()
        return getInputStream(path).use {
            ByteStreams.copy(it, Funnels.asOutputStream(hashing))
            Md5Hash(hashing.hash().asBytes())
        }
    }
}

interface NavigableReader : AutoCloseable {
    fun asReader(): Reader
    fun seek(offset: Long)
    fun getByteOffset(): Long
    fun getEncoding(): String
}

fun getFilenameWithoutExtension(path: Path): String {
    val filename = path.fileName.toString()
    return if (filename.contains('.')) {
        filename.substring(0, filename.indexOf('.'))
    } else {
        filename
    }
}
