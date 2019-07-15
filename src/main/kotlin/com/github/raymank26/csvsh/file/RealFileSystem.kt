package com.github.raymank26.csvsh.file

import org.lmdbjava.Env
import org.lmdbjava.EnvFlags
import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.io.BufferedReader
import java.io.FileInputStream
import java.io.FileOutputStream
import java.io.FileReader
import java.io.InputStream
import java.io.OutputStream
import java.io.RandomAccessFile
import java.io.Reader
import java.nio.ByteBuffer
import java.nio.file.Files
import java.nio.file.Path

/**
 * Date: 2019-06-10.
 */
class RealFileSystem : FileSystem {
    override fun getDB(path: Path): Env<ByteBuffer> {
        if (!Files.exists(path)) {
            Files.createFile(path)
        }
        return Env.create()
                .setMapSize(500 * (1024 * 1024))
                .setMaxDbs(100)
                .open(path.toFile(), EnvFlags.MDB_NOSUBDIR)
    }

    override fun isFileExists(path: Path): Boolean {
        return Files.exists(path) && Files.isRegularFile(path)
    }

    override fun getNavigableReader(path: Path): NavigableReader {
        return RealNavigableReader(RandomAccessFile(path.toFile(), "r"))
    }

    override fun getInputStream(path: Path): InputStream {
        return BufferedInputStream(FileInputStream(path.toFile()))
    }

    override fun getOutputStream(path: Path): OutputStream {
        return BufferedOutputStream(FileOutputStream(path.toFile()))
    }

    override fun getSize(path: Path): Long {
        return Files.size(path)
    }
}

private class RealNavigableReader(private val randomAccessFile: RandomAccessFile) : NavigableReader {

    private var reader: Reader
    private val encoding: String

    init {
        val tmpReader = FileReader(randomAccessFile.fd)
        encoding = tmpReader.encoding
        reader = BufferedReader(tmpReader)
    }

    override fun asReader(): Reader {
        return reader
    }

    override fun probe(length: Int): String {
        val buf = CharArray(length)
        val offset = getByteOffset()
        reader.read(buf)
        seek(offset)
        return String(buf)
    }

    override fun seek(offset: Long) {
        randomAccessFile.seek(offset)
        reader = BufferedReader(FileReader(randomAccessFile.fd))
    }

    override fun getByteOffset(): Long {
        return randomAccessFile.filePointer
    }

    override fun getEncoding(): String {
        return encoding
    }

    override fun close() {
        randomAccessFile.close()
    }
}