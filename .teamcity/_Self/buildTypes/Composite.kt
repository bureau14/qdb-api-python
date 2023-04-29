package _Self.buildTypes

import jetbrains.buildServer.configs.kotlin.*
import jetbrains.buildServer.configs.kotlin.buildFeatures.notifications
import jetbrains.buildServer.configs.kotlin.triggers.vcs

object Composite : BuildType({
    name = "Composite"

    type = BuildTypeSettings.Type.COMPOSITE

    vcs {
        root(DslContext.settingsRoot)

        showDependenciesChanges = true
    }

    triggers {
        vcs {
            branchFilter = "+:<default>"
        }
    }

    features {
        notifications {
            notifierSettings = slackNotifier {
                connection = "PROJECT_EXT_19"
                sendTo = "#build"
                messageFormat = verboseMessageFormat {
                    addBranch = true
                    addChanges = true
                    addStatusText = true
                    maximumNumberOfChanges = 10
                }
            }
            buildFailed = true
            buildFinishedSuccessfully = true
            firstSuccessAfterFailure = true
        }
    }

    dependencies {
        snapshot(Debug.buildTypes.Debug_Composite) {
        }
        dependency(Release.buildTypes.Release_Composite) {
            snapshot {
            }

            artifacts {
                artifactRules = "** => ."
            }
        }
    }
})
