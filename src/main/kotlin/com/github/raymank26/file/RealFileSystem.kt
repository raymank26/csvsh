package com.github.raymank26.file

import org.mapdb.DB
import org.mapdb.DBMaker
import java.io.FileInputStream
import java.io.FileOutputStream
import java.io.FileReader
import java.io.InputStream
import java.io.OutputStream
import java.io.RandomAccessFile
import java.io.Reader
import java.nio.file.Files
import java.nio.file.Path

/**
 * Date: 2019-06-10.
 */
class RealFileSystem : FileSystem {
    override fun getDB(path: Path): DB {
        return DBMaker.fileDB(path.toFile()).make()
    }

    override fun isFileExists(path: Path): Boolean {
        return Files.exists(path) && Files.isRegularFile(path)
    }

    override fun getNavigableReader(path: Path): NavigableReader {
        return RealNavigableReader(RandomAccessFile(path.toFile(), "r"))
    }

    override fun getInputStream(path: Path): InputStream {
        return FileInputStream(path.toFile())
    }

    override fun getOutputStream(path: Path): OutputStream {
        return FileOutputStream(path.toFile())
    }
}

private class RealNavigableReader(private val randomAccessFile: RandomAccessFile) : NavigableReader {
    private val reader = FileReader(randomAccessFile.fd)

    override fun asReader(): Reader {
        return reader
    }

    override fun seek(offset: Long) {
        randomAccessFile.seek(offset)
    }

    override fun getByteOffset(): Long {
        return randomAccessFile.filePointer
    }

    override fun getEncoding(): String {
        return reader.encoding
    }

    override fun close() {
        randomAccessFile.close()
    }
}