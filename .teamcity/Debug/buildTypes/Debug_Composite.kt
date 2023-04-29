package Debug.buildTypes

import jetbrains.buildServer.configs.kotlin.*

object Debug_Composite : BuildType({
    name = "Composite"

    type = BuildTypeSettings.Type.COMPOSITE

    vcs {
        root(DslContext.settingsRoot)

        showDependenciesChanges = true
    }

    dependencies {
        dependency(Debug_Linux.buildTypes.Debug_Linux_Composite) {
            snapshot {
            }

            artifacts {
                artifactRules = "** "
            }
        }
        dependency(Debug_Osx.buildTypes.Debug_Osx_Composite) {
            snapshot {
            }

            artifacts {
                artifactRules = "**"
            }
        }
        dependency(Debug_Windows.buildTypes.Debug_Windows_Composite) {
            snapshot {
            }

            artifacts {
                artifactRules = "**"
            }
        }
    }
})
