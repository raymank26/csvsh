package com.github.raymank26

import org.lmdbjava.Dbi
import org.lmdbjava.Env
import org.lmdbjava.KeyRange
import java.nio.ByteBuffer

typealias FoundOffsets = Set<Long>

interface ReadOnlyIndex : AutoCloseable {
    val type: FieldType

    fun moreThan(from: SqlValueAtom): FoundOffsets
    fun moreThanEq(fromInclusive: SqlValueAtom): FoundOffsets
    fun lessThan(to: SqlValueAtom): FoundOffsets
    fun lessThanEq(toInclusive: SqlValueAtom): FoundOffsets
    fun eq(value: SqlValueAtom): FoundOffsets
    fun inRange(list: ListValue): FoundOffsets
}

class MapDBReadonlyIndex(private val dbi: Dbi<ByteBuffer>,
                         private val env: Env<ByteBuffer>,
                         private val serializer: FieldSerializer,
                         override val type: FieldType) : ReadOnlyIndex {

    override fun moreThan(from: SqlValueAtom): FoundOffsets {
        return collectOffsets(KeyRange.greaterThan(serializer.serialize(from)))
    }

    override fun moreThanEq(fromInclusive: SqlValueAtom): FoundOffsets {
        return collectOffsets(KeyRange.atLeast(serializer.serialize(fromInclusive)))
    }

    override fun lessThan(to: SqlValueAtom): FoundOffsets {
        return collectOffsets(KeyRange.lessThan(serializer.serialize(to)))
    }

    override fun lessThanEq(toInclusive: SqlValueAtom): FoundOffsets {
        return collectOffsets(KeyRange.atMost(serializer.serialize(toInclusive)))
    }

    override fun eq(value: SqlValueAtom): FoundOffsets {
        val a = serializer.serialize(value)
        val b = serializer.serialize(value)
        return collectOffsets(KeyRange.closed(a, b))
    }

    override fun inRange(list: ListValue): FoundOffsets {
        val bs = mutableSetOf<Long>()
        for (elem in list.value) {
            bs.addAll(eq(elem))
        }
        return bs
    }

    override fun close() {
        env.close()
    }

    private fun collectOffsets(keyRange: KeyRange<ByteBuffer>): Set<Long> {
        val res = mutableSetOf<Long>()
        env.txnRead().use { txn ->
            dbi.iterate(txn, keyRange).use { cursor ->
                for (v in cursor.iterable()) {
                    res.add(v.`val`().getLong(0))
                }
            }
        }
        return res
    }
}
