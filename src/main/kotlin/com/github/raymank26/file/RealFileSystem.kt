package com.github.raymank26.file

import org.mapdb.DB
import org.mapdb.DBMaker
import java.io.FileInputStream
import java.io.FileOutputStream
import java.io.InputStream
import java.io.OutputStream
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

    override fun getInputStream(path: Path): InputStream {
        return FileInputStream(path.toFile())
    }

    override fun getOutputStream(path: Path): OutputStream {
        return FileOutputStream(path.toFile())
    }
}