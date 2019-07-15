package com.github.raymank26.csvsh.file

import org.apache.commons.codec.binary.Hex

class Md5HashConverter {

    companion object {
        val INSTANCE = Md5HashConverter()
    }

    fun serialize(md5Hash: Md5Hash): String {
        return Hex.encodeHexString(md5Hash.content)
    }

    fun deserialize(content: String): Md5Hash {
        return Md5Hash(Hex.decodeHex(content))
    }
}