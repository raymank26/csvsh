package com.github.raymank26.csvsh

import java.util.Properties

/**
 * Date: 2019-07-07.
 */
class BuildPropertiesParser {

    fun parse(): BuildProperties {
        javaClass.classLoader.getResourceAsStream("build.properties").use {
            val properties = Properties()
            properties.load(it)
            return BuildProperties(properties.getProperty("version"))
        }
    }
}

data class BuildProperties(val version: String)