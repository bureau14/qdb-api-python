package Debug_Windows_Win64

import Debug_Windows_Win64.buildTypes.*
import jetbrains.buildServer.configs.kotlin.*
import jetbrains.buildServer.configs.kotlin.Project

object Project : Project({
    id("Debug_Windows_Win64")
    name = "Win64"

    buildType(Debug_Windows_Win64_Composite)
    buildType(Debug_Windows_Win64_Python311)
    buildType(Debug_Windows_Win64_Python36)
    buildType(Debug_Windows_Win64_Python37)
    buildType(Debug_Windows_Win64_Python38)
    buildType(Debug_Windows_Win64_Python310)
    buildType(Debug_Windows_Win64_Python39)

    template(Debug_Windows_Win64_Build)
})
