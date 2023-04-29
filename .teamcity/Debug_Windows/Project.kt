package Debug_Windows

import Debug_Windows.buildTypes.*
import jetbrains.buildServer.configs.kotlin.*
import jetbrains.buildServer.configs.kotlin.Project

object Project : Project({
    id("Debug_Windows")
    name = "Windows"

    buildType(Debug_Windows_Composite)

    subProject(Debug_Windows_Win64.Project)
    subProject(Debug_Windows_Win32.Project)
})
