package Debug_Windows_Win32.buildTypes

import jetbrains.buildServer.configs.kotlin.*

object Debug_Windows_Win32_Composite : BuildType({
    name = "Composite"

    artifactRules = "*"
    type = BuildTypeSettings.Type.COMPOSITE

    vcs {
        root(DslContext.settingsRoot)

        showDependenciesChanges = true
    }

    dependencies {
        dependency(Debug_Windows_Win32_Python310) {
            snapshot {
                onDependencyFailure = FailureAction.FAIL_TO_START
            }

            artifacts {
                artifactRules = "*"
            }
        }
        dependency(Debug_Windows_Win32_Python311) {
            snapshot {
                onDependencyFailure = FailureAction.FAIL_TO_START
            }

            artifacts {
                artifactRules = "*"
            }
        }
        dependency(Debug_Windows_Win32_Python36) {
            snapshot {
                onDependencyFailure = FailureAction.FAIL_TO_START
            }

            artifacts {
                artifactRules = "*"
            }
        }
        dependency(Debug_Windows_Win32_Python37) {
            snapshot {
                onDependencyFailure = FailureAction.FAIL_TO_START
            }

            artifacts {
                artifactRules = "*"
            }
        }
        dependency(Debug_Windows_Win32_Python372) {
            snapshot {
                onDependencyFailure = FailureAction.FAIL_TO_START
            }

            artifacts {
                artifactRules = "*"
            }
        }
        dependency(Debug_Windows_Win32_Python39) {
            snapshot {
                onDependencyFailure = FailureAction.FAIL_TO_START
            }

            artifacts {
                artifactRules = "*"
            }
        }
    }
})
