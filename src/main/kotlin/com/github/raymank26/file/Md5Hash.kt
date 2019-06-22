package com.github.raymank26.file

import java.util.Arrays

data class Md5Hash(val content: ByteArray) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as Md5Hash

        if (!content.contentEquals(other.content)) return false
        return true
    }

    override fun hashCode(): Int {
        return content.contentHashCode()
    }

    override fun toString(): String {
        return "Md5Hash(content=${Arrays.toString(content)})"
    }
}