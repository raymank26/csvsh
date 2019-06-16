package com.github.raymank26

import org.mapdb.BTreeMap
import java.util.NavigableMap

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

class MapDBReadonlyIndex(
        private val bTreeMap: BTreeMap<in Any, LongArray>,
        override val type: FieldType) : ReadOnlyIndex {

    override fun moreThan(from: SqlValueAtom): FoundOffsets {
        return bTreeToBitSet(bTreeMap.tailMap(from.asValue, false))
    }

    override fun moreThanEq(fromInclusive: SqlValueAtom): FoundOffsets {
        return bTreeToBitSet(bTreeMap.tailMap(fromInclusive.asValue, true))
    }

    override fun lessThan(to: SqlValueAtom): FoundOffsets {
        return bTreeToBitSet(bTreeMap.headMap(to.asValue, false))
    }

    override fun lessThanEq(toInclusive: SqlValueAtom): FoundOffsets {
        return bTreeToBitSet(bTreeMap.headMap(toInclusive.asValue, true))
    }

    override fun eq(value: SqlValueAtom): FoundOffsets {
        return bTreeMap[value.asValue]?.toSet() ?: emptySet()
    }

    override fun inRange(list: ListValue): FoundOffsets {
        val bs = mutableSetOf<Long>()
        for (elem in list.value) {
            bTreeMap[elem.asValue]?.forEach { rowNum ->
                bs.add(rowNum)
            }
        }
        return bs
    }

    override fun close() {
        bTreeMap.close()
    }
}

private fun bTreeToBitSet(bTree: NavigableMap<*, LongArray>): FoundOffsets {
    val bs = mutableSetOf<Long>()
    for (indexInFile in bTree.values) {
        indexInFile.forEach { offset ->
            bs.add(offset)
        }
    }
    return bs
}
