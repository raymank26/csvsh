package com.github.raymank26

import org.mapdb.BTreeMap
import java.util.BitSet
import java.util.NavigableMap

interface ReadOnlyIndex : AutoCloseable {
    fun getType(): FieldType
}

interface ReadOnlyIndexNumber<T> : ReadOnlyIndex {
    fun moreThan(from: T): BitSet
    fun moreThanEq(fromInclusive: T): BitSet
    fun lessThan(to: T): BitSet
    fun lessThanEq(toInclusive: T): BitSet
    fun eq(value: T): BitSet
    fun inRange(value: T, list: List<T>): BitSet
}

class MapDBReadonlyIndex<T>(
        private val bTreeMap: BTreeMap<T, IntArray>,
        private val fieldType: FieldType) : ReadOnlyIndexNumber<T> {
    override fun getType(): FieldType {
        return fieldType
    }

    override fun moreThan(from: T): BitSet {
        return bTreeToBitSet(bTreeMap.tailMap(from, false))
    }

    override fun moreThanEq(fromInclusive: T): BitSet {
        return bTreeToBitSet(bTreeMap.tailMap(fromInclusive, true))
    }

    override fun lessThan(to: T): BitSet {
        return bTreeToBitSet(bTreeMap.headMap(to, false))
    }

    override fun lessThanEq(toInclusive: T): BitSet {
        return bTreeToBitSet(bTreeMap.headMap(toInclusive, true))
    }

    override fun eq(value: T): BitSet {
        return bTreeMap[value]?.let {
            val bs = BitSet()
            it.forEach { rowNum -> bs.set(rowNum) }
            bs
        } ?: BitSet()
    }

    override fun inRange(value: T, list: List<T>): BitSet {
        val bs = BitSet()
        for (elem in list) {
            bTreeMap[elem]?.forEach { rowNum ->
                bs.set(rowNum)
            }
        }
        return bs
    }

    override fun close() {
        bTreeMap.close()
    }
}

private fun bTreeToBitSet(bTree: NavigableMap<*, IntArray>): BitSet {
    val bs = BitSet()
    for (indexInFile: IntArray in bTree.values) {
        indexInFile.forEach { rowNum ->
            bs.set(rowNum)
        }
    }
    return bs
}
