package Windows_Win32

import Windows_Win32.buildTypes.*
import jetbrains.buildServer.configs.kotlin.*
import jetbrains.buildServer.configs.kotlin.Project

object Project : Project({
    id("Windows_Win32")
    name = "Win32"

    buildType(Windows_Win32_Python37)
    buildType(Windows_Win32_Python36)
    buildType(Windows_Win32_Composite)
    buildType(Windows_Win32_Python311)
    buildType(Windows_Win32_Python310)
    buildType(Windows_Win32_Python37_2)
    buildType(Windows_Win32_Python39)

    template(Windows_Win32_Build)
})
