package Debug_Linux

import Debug_Linux.buildTypes.*
import jetbrains.buildServer.configs.kotlin.*
import jetbrains.buildServer.configs.kotlin.Project

object Project : Project({
    id("Debug_Linux")
    name = "Linux"

    buildType(Debug_Linux_Composite)
    buildType(Debug_Linux_Python311)
    buildType(Debug_Linux_Python310)
    buildType(Debug_Linux_Python37)
    buildType(Debug_Linux_Python36)
    buildType(Debug_Linux_Python39)
    buildType(Debug_Linux_Python38)

    template(Debug_Linux_Build)
})
