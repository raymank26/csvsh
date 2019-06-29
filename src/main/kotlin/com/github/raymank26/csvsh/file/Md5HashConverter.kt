package com.github.raymank26.csvsh.file

import com.google.common.io.BaseEncoding

class Md5HashConverter {

    companion object {
        val INSTANCE = Md5HashConverter()
    }

    fun serialize(md5Hash: Md5Hash): String {
        return BaseEncoding.base16().lowerCase().encode(md5Hash.content)
    }

    fun deserialize(content: String): Md5Hash {
        return Md5Hash(BaseEncoding.base16().lowerCase().decode(content))
    }
}