package com.github.raymank26.csvsh.index

/**
 * Date: 2019-06-30.
 */
/**
 * Date: 2019-05-13.
 */
data class IndexDescription(val name: String, val fieldName: String)

data class IndexDescriptionAndPath(val description: IndexDescription, val indexContent: Lazy<ReadOnlyIndex>)

typealias FoundOffsets = Set<Long>