package Debug_Osx.buildTypes

import jetbrains.buildServer.configs.kotlin.*

object Debug_Osx_Composite : BuildType({
    name = "Composite"

    type = BuildTypeSettings.Type.COMPOSITE

    vcs {
        showDependenciesChanges = true
    }

    dependencies {
        dependency(Debug_Osx_Python310) {
            snapshot {
                onDependencyFailure = FailureAction.FAIL_TO_START
            }

            artifacts {
                artifactRules = "**"
            }
        }
        dependency(Debug_Osx_Python311) {
            snapshot {
                onDependencyFailure = FailureAction.FAIL_TO_START
            }

            artifacts {
                artifactRules = "**"
            }
        }
        dependency(Debug_Osx_Python36) {
            snapshot {
                onDependencyFailure = FailureAction.FAIL_TO_START
            }

            artifacts {
                artifactRules = "**"
            }
        }
        dependency(Debug_Osx_Python37) {
            snapshot {
                onDependencyFailure = FailureAction.FAIL_TO_START
            }

            artifacts {
                artifactRules = "**"
            }
        }
        dependency(Debug_Osx_Python38) {
            snapshot {
                onDependencyFailure = FailureAction.FAIL_TO_START
            }

            artifacts {
                artifactRules = "**"
            }
        }
        dependency(Debug_Osx_Python39) {
            snapshot {
                onDependencyFailure = FailureAction.FAIL_TO_START
            }

            artifacts {
                artifactRules = "**"
            }
        }
    }
})
