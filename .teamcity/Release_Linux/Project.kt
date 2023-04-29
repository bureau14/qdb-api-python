package Release_Linux

import Release_Linux.buildTypes.*
import jetbrains.buildServer.configs.kotlin.*
import jetbrains.buildServer.configs.kotlin.Project

object Project : Project({
    id("Release_Linux")
    name = "Linux"

    buildType(Release_Linux_Composite_2)

    template(Release_Linux_Build)

    subProject(Release_Linux_X86.Project)
    subProject(Release_Linux_LinuxAArch64.Project)
})
