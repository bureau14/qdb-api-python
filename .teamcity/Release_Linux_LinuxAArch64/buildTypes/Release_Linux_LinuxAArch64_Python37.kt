package Release_Linux_LinuxAArch64.buildTypes

import jetbrains.buildServer.configs.kotlin.*
import jetbrains.buildServer.configs.kotlin.buildSteps.DockerCommandStep
import jetbrains.buildServer.configs.kotlin.buildSteps.dockerCommand

object Release_Linux_LinuxAArch64_Python37 : BuildType({
    templates(Release_Linux.buildTypes.Release_Linux_Build)
    name = "Python 3.7"

    params {
        text("PYTHON_VERSION", "3.7.15", label = "Python version", description = "Python version to install. Files will be downloaded from s3://qdbbuilddeps/linux/python/Python-x.y.z.tgz", allowEmpty = false)
        param("platform", "linux-64bit-aarch64")
    }

    steps {
        dockerCommand {
            name = "Build container"
            id = "RUNNER_53"
            commandType = build {
                source = file {
                    path = "docker/Dockerfile"
                }
                contextDir = "docker/"
                platform = DockerCommandStep.ImagePlatform.Linux
                namesAndTags = "build:%build.vcs.number%-%PYTHON_VERSION%"
                commandArgs = """
                    --build-arg PYTHON_VERSION=%PYTHON_VERSION%
                    --build-arg ARCH=aarch64
                    --build-arg DEVTOOLSET_VERSION=10
                """.trimIndent()
            }
        }
    }

    dependencies {
        artifacts(AbsoluteId("Quasardb_Source_Composite")) {
            id = "ARTIFACT_DEPENDENCY_65"
            artifactRules = """
                *-%platform%-c-api.tar.gz!**/*=>qdb
                *-%platform%-utils.tar.gz!**/*=>qdb
                *-%platform%-server.tar.gz!**/*=>qdb
            """.trimIndent()
        }
    }

    requirements {
        contains("teamcity.agent.jvm.os.arch", "aarch64", "RQ_134")
    }
    
    disableSettings("RUNNER_280")
})
