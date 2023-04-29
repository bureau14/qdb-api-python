package Release

import Release.buildTypes.*
import jetbrains.buildServer.configs.kotlin.*
import jetbrains.buildServer.configs.kotlin.Project

object Project : Project({
    id("Release")
    name = "Release"

    buildType(Release_Composite)

    template(Release_Build)

    params {
        param("env.CMAKE_BUILD_TYPE", "Release")
    }

    subProject(Release_Linux.Project)
    subProject(Windows.Project)
    subProject(Release_Osx.Project)
})
