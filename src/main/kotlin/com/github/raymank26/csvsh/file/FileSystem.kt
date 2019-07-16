package com.github.raymank26.csvsh.file

import com.github.raymank26.csvsh.sql.SqlParser
import org.lmdbjava.Env
import java.io.InputStream
import java.io.OutputStream
import java.io.Reader
import java.nio.ByteBuffer
import java.nio.file.Path
import java.nio.file.Paths
import java.security.DigestInputStream
import java.security.MessageDigest


interface FileSystem {

    fun isFileExists(path: Path): Boolean

    fun getDB(path: Path): Env<ByteBuffer>

    fun getNavigableReader(path: Path): NavigableReader

    fun getInputStream(path: Path): InputStream

    fun getOutputStream(path: Path): OutputStream

    fun getSize(path: Path): Long

    fun getMd5(path: Path): Md5Hash {
        val md = MessageDigest.getInstance("MD5")
        getInputStream(path).use {
            val dis = DigestInputStream(it, md)
            dis.readBytes()
        }
        return Md5Hash(md.digest())
    }
}

interface NavigableReader : AutoCloseable {
    fun asReader(): Reader
    fun seek(offset: Long)
    fun getByteOffset(): Long
    fun getEncoding(): String
    fun probe(length: Int): String
}

fun getFilenameWithoutExtension(path: Path): String {
    val filename = path.fileName.toString()
    return if (filename.contains('.')) {
        filename.substring(0, filename.indexOf('.'))
    } else {
        filename
    }
}

fun resolvePath(tableContext: SqlParser.TableContext): Path {
    val path = Paths.get(tableContext.IDENTIFIER_Q().text.drop(1).dropLast(1))
    if (path.isAbsolute) {
        return path
    }
    return Paths.get("").toAbsolutePath().resolve(path)
}