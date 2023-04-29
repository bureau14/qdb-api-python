package Debug_Linux.buildTypes

import jetbrains.buildServer.configs.kotlin.*

object Debug_Linux_Composite : BuildType({
    name = "Composite"

    type = BuildTypeSettings.Type.COMPOSITE

    vcs {
        root(DslContext.settingsRoot)

        showDependenciesChanges = true
    }

    dependencies {
        dependency(Debug_Linux_Python310) {
            snapshot {
                onDependencyFailure = FailureAction.FAIL_TO_START
            }

            artifacts {
                artifactRules = "*"
            }
        }
        dependency(Debug_Linux_Python311) {
            snapshot {
                onDependencyFailure = FailureAction.FAIL_TO_START
            }

            artifacts {
                artifactRules = "*"
            }
        }
        dependency(Debug_Linux_Python36) {
            snapshot {
                onDependencyFailure = FailureAction.FAIL_TO_START
            }

            artifacts {
                artifactRules = "*"
            }
        }
        dependency(Debug_Linux_Python37) {
            snapshot {
                onDependencyFailure = FailureAction.FAIL_TO_START
            }

            artifacts {
                artifactRules = "*"
            }
        }
        dependency(Debug_Linux_Python38) {
            snapshot {
                onDependencyFailure = FailureAction.FAIL_TO_START
            }

            artifacts {
                artifactRules = "*"
            }
        }
        dependency(Debug_Linux_Python39) {
            snapshot {
                onDependencyFailure = FailureAction.FAIL_TO_START
            }

            artifacts {
                artifactRules = "*"
            }
        }
    }
})
