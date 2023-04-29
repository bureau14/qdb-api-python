package Debug_Osx

import Debug_Osx.buildTypes.*
import jetbrains.buildServer.configs.kotlin.*
import jetbrains.buildServer.configs.kotlin.Project

object Project : Project({
    id("Debug_Osx")
    name = "macOS"

    buildType(Debug_Osx_Composite)
    buildType(Debug_Osx_Python310)
    buildType(Debug_Osx_Python37)
    buildType(Debug_Osx_Python36)
    buildType(Debug_Osx_Python39)
    buildType(Debug_Osx_Python311)
    buildType(Debug_Osx_Python38)

    template(Debug_Osx_Build)

    params {
        param("platform", "darwin-64bit")
    }
})
