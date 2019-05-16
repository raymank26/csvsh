package com.github.raymank26

import java.util.BitSet

interface ReadOnlyIndex {
    fun getType(): FieldType
}

interface ReadOnlyIndexNumber<T> : ReadOnlyIndex {
    fun moreThan(from: T): BitSet
    fun moreThanEq(fromInclusive: T): BitSet
    fun lessThan(to: T): BitSet
    fun lessThanEq(toInclusive: T): BitSet
    fun eq(value: T): BitSet
}

interface ReadOnlyIndexString : ReadOnlyIndex {
    fun eq(value: String): BitSet
}