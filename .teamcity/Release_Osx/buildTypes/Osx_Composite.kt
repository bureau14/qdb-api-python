package Release_Osx.buildTypes

import jetbrains.buildServer.configs.kotlin.*

object Osx_Composite : BuildType({
    name = "Composite"

    type = BuildTypeSettings.Type.COMPOSITE

    vcs {
        showDependenciesChanges = true
    }

    dependencies {
        dependency(Release_Osx_Python310) {
            snapshot {
                onDependencyFailure = FailureAction.FAIL_TO_START
            }

            artifacts {
                artifactRules = "**"
            }
        }
        dependency(Release_Osx_Python311) {
            snapshot {
                onDependencyFailure = FailureAction.FAIL_TO_START
            }

            artifacts {
                artifactRules = "**"
            }
        }
        dependency(Release_Osx_Python37) {
            snapshot {
                onDependencyFailure = FailureAction.FAIL_TO_START
            }

            artifacts {
                artifactRules = "**"
            }
        }
        dependency(Release_Osx_Python38) {
            snapshot {
                onDependencyFailure = FailureAction.FAIL_TO_START
            }

            artifacts {
                artifactRules = "**"
            }
        }
        dependency(Release_Osx_Python39) {
            snapshot {
                onDependencyFailure = FailureAction.FAIL_TO_START
            }

            artifacts {
                artifactRules = "**"
            }
        }
    }
})
