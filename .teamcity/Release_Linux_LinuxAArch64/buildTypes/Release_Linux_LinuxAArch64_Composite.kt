package Release_Linux_LinuxAArch64.buildTypes

import jetbrains.buildServer.configs.kotlin.*

object Release_Linux_LinuxAArch64_Composite : BuildType({
    name = "Composite"

    type = BuildTypeSettings.Type.COMPOSITE

    vcs {
        root(DslContext.settingsRoot)

        showDependenciesChanges = true
    }

    dependencies {
        dependency(Release_Linux_LinuxAArch64_Python310) {
            snapshot {
                onDependencyFailure = FailureAction.FAIL_TO_START
            }

            artifacts {
                artifactRules = "*"
            }
        }
        dependency(Release_Linux_LinuxAArch64_Python311) {
            snapshot {
                onDependencyFailure = FailureAction.FAIL_TO_START
            }

            artifacts {
                artifactRules = "*"
            }
        }
        dependency(Release_Linux_LinuxAArch64_Python36) {
            snapshot {
                onDependencyFailure = FailureAction.FAIL_TO_START
            }

            artifacts {
                artifactRules = "*"
            }
        }
        dependency(Release_Linux_LinuxAArch64_Python37) {
            snapshot {
                onDependencyFailure = FailureAction.FAIL_TO_START
            }

            artifacts {
                artifactRules = "*"
            }
        }
        dependency(Release_Linux_LinuxAArch64_Python38) {
            snapshot {
                onDependencyFailure = FailureAction.FAIL_TO_START
            }

            artifacts {
                artifactRules = "*"
            }
        }
        dependency(Release_Linux_LinuxAArch64_Python39) {
            snapshot {
                onDependencyFailure = FailureAction.FAIL_TO_START
            }

            artifacts {
                artifactRules = "*"
            }
        }
    }
})
