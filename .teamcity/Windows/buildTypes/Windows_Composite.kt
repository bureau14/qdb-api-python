package Windows.buildTypes

import jetbrains.buildServer.configs.kotlin.*

object Windows_Composite : BuildType({
    name = "Composite"

    type = BuildTypeSettings.Type.COMPOSITE

    vcs {
        root(DslContext.settingsRoot)

        showDependenciesChanges = true
    }

    dependencies {
        dependency(Windows_Win32.buildTypes.Windows_Win32_Composite) {
            snapshot {
                onDependencyFailure = FailureAction.FAIL_TO_START
            }

            artifacts {
                artifactRules = "*"
            }
        }
        dependency(Windows_Win64.buildTypes.Windows_Win64_Composite) {
            snapshot {
                onDependencyFailure = FailureAction.FAIL_TO_START
            }

            artifacts {
                artifactRules = "*"
            }
        }
    }
})
