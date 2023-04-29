package Windows

import Windows.buildTypes.*
import jetbrains.buildServer.configs.kotlin.*
import jetbrains.buildServer.configs.kotlin.Project

object Project : Project({
    id("Windows")
    name = "Windows"

    buildType(Windows_Composite)

    subProject(Windows_Win64.Project)
    subProject(Windows_Win32.Project)
})
