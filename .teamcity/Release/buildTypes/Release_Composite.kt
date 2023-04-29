package Release.buildTypes

import jetbrains.buildServer.configs.kotlin.*

object Release_Composite : BuildType({
    name = "Composite"

    type = BuildTypeSettings.Type.COMPOSITE

    vcs {
        root(DslContext.settingsRoot)

        showDependenciesChanges = true
    }

    dependencies {
        dependency(Release_Osx.buildTypes.Osx_Composite) {
            snapshot {
                onDependencyFailure = FailureAction.FAIL_TO_START
            }

            artifacts {
                artifactRules = "*"
            }
        }
        dependency(Release_Linux.buildTypes.Release_Linux_Composite_2) {
            snapshot {
                onDependencyFailure = FailureAction.FAIL_TO_START
            }

            artifacts {
                artifactRules = "*"
            }
        }
        dependency(Windows.buildTypes.Windows_Composite) {
            snapshot {
                onDependencyFailure = FailureAction.FAIL_TO_START
            }

            artifacts {
                artifactRules = "*"
            }
        }
    }
})
