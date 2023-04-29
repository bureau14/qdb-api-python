package Windows_Win64

import Windows_Win64.buildTypes.*
import jetbrains.buildServer.configs.kotlin.*
import jetbrains.buildServer.configs.kotlin.Project

object Project : Project({
    id("Windows_Win64")
    name = "Win64"

    buildType(Windows_Win64_Python38)
    buildType(Windows_Win64_Python39)
    buildType(Windows_Win64_Composite)
    buildType(Windows_Win64_Python310)
    buildType(Windows_Win64_Python311)
    buildType(Windows_Win64_Python36)
    buildType(Windows_Win64_Python37)

    template(Windows_Win64_Build)
})
