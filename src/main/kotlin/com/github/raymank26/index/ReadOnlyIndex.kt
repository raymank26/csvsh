package com.github.raymank26.index

import com.github.raymank26.FieldType
import com.github.raymank26.ListValue
import com.github.raymank26.SqlValueAtom

interface ReadOnlyIndex : AutoCloseable {
    val type: FieldType

    fun moreThan(from: SqlValueAtom): FoundOffsets
    fun moreThanEq(fromInclusive: SqlValueAtom): FoundOffsets
    fun lessThan(to: SqlValueAtom): FoundOffsets
    fun lessThanEq(toInclusive: SqlValueAtom): FoundOffsets
    fun eq(value: SqlValueAtom): FoundOffsets
    fun inRange(list: ListValue): FoundOffsets
}