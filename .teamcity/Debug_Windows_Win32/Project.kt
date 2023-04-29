package Debug_Windows_Win32

import Debug_Windows_Win32.buildTypes.*
import jetbrains.buildServer.configs.kotlin.*
import jetbrains.buildServer.configs.kotlin.Project

object Project : Project({
    id("Debug_Windows_Win32")
    name = "Win32"

    buildType(Debug_Windows_Win32_Python372)
    buildType(Debug_Windows_Win32_Python39)
    buildType(Debug_Windows_Win32_Python310)
    buildType(Debug_Windows_Win32_Python311)
    buildType(Debug_Windows_Win32_Python37)
    buildType(Debug_Windows_Win32_Python36)
    buildType(Debug_Windows_Win32_Composite)

    template(Debug_Windows_Win32_Build)
})
