package com.github.raymank26

import org.mapdb.BTreeMap
import java.util.NavigableMap

interface ReadOnlyIndex<T> : AutoCloseable {
    fun moreThan(from: T): Set<Int>
    fun moreThanEq(fromInclusive: T): Set<Int>
    fun lessThan(to: T): Set<Int>
    fun lessThanEq(toInclusive: T): Set<Int>
    fun eq(value: T): Set<Int>
    fun inRange(value: T, list: List<T>): Set<Int>
    fun getType(): FieldType
}

class MapDBReadonlyIndex<T>(
        private val bTreeMap: BTreeMap<T, IntArray>,
        private val fieldType: FieldType) : ReadOnlyIndex<T> {
    override fun getType(): FieldType {
        return fieldType
    }

    override fun moreThan(from: T): Set<Int> {
        return bTreeToBitSet(bTreeMap.tailMap(from, false))
    }

    override fun moreThanEq(fromInclusive: T): Set<Int> {
        return bTreeToBitSet(bTreeMap.tailMap(fromInclusive, true))
    }

    override fun lessThan(to: T): Set<Int> {
        return bTreeToBitSet(bTreeMap.headMap(to, false))
    }

    override fun lessThanEq(toInclusive: T): Set<Int> {
        return bTreeToBitSet(bTreeMap.headMap(toInclusive, true))
    }

    override fun eq(value: T): Set<Int> {
        return bTreeMap[value]?.toSet() ?: emptySet()
    }

    override fun inRange(value: T, list: List<T>): Set<Int> {
        val bs = mutableSetOf<Int>()
        for (elem in list) {
            bTreeMap[elem]?.forEach { rowNum ->
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
