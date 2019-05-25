package com.github.raymank26

import org.mapdb.BTreeMap
import java.util.NavigableMap

interface ReadOnlyIndex : AutoCloseable {
    val type: FieldType

    fun moreThan(from: SqlValueAtom): Set<Int>
    fun moreThanEq(fromInclusive: SqlValueAtom): Set<Int>
    fun lessThan(to: SqlValueAtom): Set<Int>
    fun lessThanEq(toInclusive: SqlValueAtom): Set<Int>
    fun eq(value: SqlValueAtom): Set<Int>
    fun inRange(list: ListValue): Set<Int>
}

class MapDBReadonlyIndex(
        private val bTreeMap: BTreeMap<in Any, IntArray>,
        override val type: FieldType) : ReadOnlyIndex {

    override fun moreThan(from: SqlValueAtom): Set<Int> {
        return bTreeToBitSet(bTreeMap.tailMap(from.asValue, false))
    }

    override fun moreThanEq(fromInclusive: SqlValueAtom): Set<Int> {
        return bTreeToBitSet(bTreeMap.tailMap(fromInclusive.asValue, true))
    }

    override fun lessThan(to: SqlValueAtom): Set<Int> {
        return bTreeToBitSet(bTreeMap.headMap(to.asValue, false))
    }

    override fun lessThanEq(toInclusive: SqlValueAtom): Set<Int> {
        return bTreeToBitSet(bTreeMap.headMap(toInclusive.asValue, true))
    }

    override fun eq(value: SqlValueAtom): Set<Int> {
        return bTreeMap[value.asValue]?.toSet() ?: emptySet()
    }

    override fun inRange(list: ListValue): Set<Int> {
        val bs = mutableSetOf<Int>()
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

private fun bTreeToBitSet(bTree: NavigableMap<*, IntArray>): Set<Int> {
    val bs = mutableSetOf<Int>()
    for (indexInFile: IntArray in bTree.values) {
        indexInFile.forEach { rowNum ->
            bs.add(rowNum)
        }
    }
    return bs
}
