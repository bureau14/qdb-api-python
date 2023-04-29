package Release_Osx

import Release_Osx.buildTypes.*
import jetbrains.buildServer.configs.kotlin.*
import jetbrains.buildServer.configs.kotlin.Project

object Project : Project({
    id("Release_Osx")
    name = "macOS"

    buildType(Osx_Composite)
    buildType(Release_Osx_Python311)
    buildType(Release_Osx_Python310)
    buildType(Release_Osx_Python38)
    buildType(Release_Osx_Python39)
    buildType(Release_Osx_Python37)

    params {
        param("platform", "darwin-64bit-core2")
    }
})
