package Release_Linux_LinuxAArch64

import Release_Linux_LinuxAArch64.buildTypes.*
import jetbrains.buildServer.configs.kotlin.*
import jetbrains.buildServer.configs.kotlin.Project

object Project : Project({
    id("Release_Linux_LinuxAArch64")
    name = "AArch64"

    buildType(Release_Linux_LinuxAArch64_Python36)
    buildType(Release_Linux_LinuxAArch64_Python37)
    buildType(Release_Linux_LinuxAArch64_Python38)
    buildType(Release_Linux_LinuxAArch64_Python39)
    buildType(Release_Linux_LinuxAArch64_Composite)
    buildType(Release_Linux_LinuxAArch64_Python310)
    buildType(Release_Linux_LinuxAArch64_Python311)
})
