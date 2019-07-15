package com.github.raymank26.csvsh.file

import org.junit.Assert.assertEquals
import org.junit.Test
import java.util.Arrays
import kotlin.random.Random

/**
 * @author anton.ermak
 * Date: 2019-07-15.
 */
class Md5HashConverterTest {

    @Test
    fun testConverter() {
        val bytes = Random.Default.nextBytes(16)
        val serialized = Md5HashConverter.INSTANCE.serialize(Md5Hash(bytes))
        val deserialized = Md5HashConverter.INSTANCE.deserialize(serialized)
        assertEquals(Arrays.toString(bytes), Arrays.toString(deserialized.content))
    }
}
