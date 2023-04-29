package Release_Linux_X86

import Release_Linux_X86.buildTypes.*
import jetbrains.buildServer.configs.kotlin.*
import jetbrains.buildServer.configs.kotlin.Project

object Project : Project({
    id("Release_Linux_X86")
    name = "X86"

    buildType(Release_Linux_Composite)
    buildType(Release_Linux_Python311)
    buildType(Release_Linux_Python36)
    buildType(Release_Linux_Python37)
    buildType(Release_Linux_Python38)
    buildType(Release_Linux_Python310)
    buildType(Release_Linux_Python39)
})
