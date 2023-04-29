package Release_Linux.buildTypes

import jetbrains.buildServer.configs.kotlin.*

object Release_Linux_Composite_2 : BuildType({
    name = "Composite"

    type = BuildTypeSettings.Type.COMPOSITE

    vcs {
        showDependenciesChanges = true
    }

    dependencies {
        dependency(Release_Linux_X86.buildTypes.Release_Linux_Composite) {
            snapshot {
            }

            artifacts {
                artifactRules = "*"
            }
        }
        dependency(Release_Linux_LinuxAArch64.buildTypes.Release_Linux_LinuxAArch64_Composite) {
            snapshot {
            }

            artifacts {
                artifactRules = "*"
            }
        }
    }
})
