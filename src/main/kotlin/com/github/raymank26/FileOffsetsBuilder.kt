package com.github.raymank26

import com.github.raymank26.file.NavigableReader
import java.nio.charset.Charset

/**
 * Date: 2019-06-18.
 */
class FileOffsetsBuilder {

    fun buildOffsets(navigableReader: NavigableReader, characterOffsets: List<Long>): List<DatasetOffset> {
        if (characterOffsets.isEmpty()) {
            return emptyList()
        }
        val reader = navigableReader.asReader()

        val offsets = mutableListOf<DatasetOffset>()

        val charset = Charset.forName(navigableReader.getEncoding())
        var offsetsIndex = 0
        var nextCharPosition = characterOffsets[offsetsIndex]
        var byteOffset = 0L
        for (charPosition in 0..characterOffsets.last()) {
            if (charPosition == nextCharPosition) {
                offsets.add(DatasetOffset(charPosition, byteOffset))
                offsetsIndex++
                if (offsetsIndex == characterOffsets.size) {
                    break
                }
                nextCharPosition = characterOffsets[offsetsIndex]
            }
            byteOffset += reader.read().toChar().toString().toByteArray(charset = charset).size
        }
        return offsets
    }
}
