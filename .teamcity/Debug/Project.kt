package Debug

import Debug.buildTypes.*
import jetbrains.buildServer.configs.kotlin.*
import jetbrains.buildServer.configs.kotlin.Project

object Project : Project({
    id("Debug")
    name = "Debug"

    buildType(Debug_Composite)

    template(Debug_Build)

    params {
        param("env.CMAKE_BUILD_TYPE", "Debug")
    }

    subProject(Debug_Osx.Project)
    subProject(Debug_Linux.Project)
    subProject(Debug_Windows.Project)
})
