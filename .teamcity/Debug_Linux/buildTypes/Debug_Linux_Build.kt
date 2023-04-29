package Debug_Linux.buildTypes

import jetbrains.buildServer.configs.kotlin.*
import jetbrains.buildServer.configs.kotlin.buildFeatures.dockerSupport
import jetbrains.buildServer.configs.kotlin.buildFeatures.perfmon
import jetbrains.buildServer.configs.kotlin.buildSteps.DockerCommandStep
import jetbrains.buildServer.configs.kotlin.buildSteps.ExecBuildStep
import jetbrains.buildServer.configs.kotlin.buildSteps.ScriptBuildStep
import jetbrains.buildServer.configs.kotlin.buildSteps.dockerCommand
import jetbrains.buildServer.configs.kotlin.buildSteps.exec
import jetbrains.buildServer.configs.kotlin.buildSteps.script

object Debug_Linux_Build : Template({
    name = "Build"

    artifactRules = """
        dist/quasardb-*.manylinux*.whl
        dist/quasardb*.egg
        dist/quasardb*.tar.gz
        dist/doc.tar.gz
    """.trimIndent()

    params {
        text("PYTHON_VERSION", "", label = "Python version", description = "Python version to install. Files will be downloaded from s3://qdbbuilddeps/linux/python/Python-x.y.z.tgz", allowEmpty = false)
        param("JUNIT_XML_FILE", "build/test/pytest.xml")
        text("env.PYTHON_EXECUTABLE", "/usr/bin/python3", label = "Python executable", description = "Points to the Python executable this should be used. This is important for Linux as well, because by default the Python docker images install multiple python versions concurrently.", allowEmpty = false)
        param("env.PYTHON_CMD", "python3")
    }

    vcs {
        root(DslContext.settingsRoot)

        cleanCheckout = true
        showDependenciesChanges = true
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
                commandArgs = "--build-arg PYTHON_VERSION=%PYTHON_VERSION%"
            }
        }
        exec {
            name = "Start services"
            id = "RUNNER_93"
            path = "bash"
            arguments = "scripts/tests/setup/start-services.sh"
        }
        exec {
            name = "Test"
            id = "RUNNER_101"
            path = "bash"
            arguments = "scripts/teamcity/20.test.sh"
            dockerImage = "build:%build.vcs.number%-%PYTHON_VERSION%"
            dockerImagePlatform = ExecBuildStep.ImagePlatform.Linux
            dockerRunParameters = "--network=host"
            param("script.content", """
                python -m pip install -r dev-requirements.txt
                python setup.py test --addopts "--junitxml=%JUNIT_XML_FILE%"
            """.trimIndent())
        }
        exec {
            name = "Build module"
            id = "RUNNER_62"
            path = "bash"
            arguments = "scripts/teamcity/10.build.sh"
            dockerImage = "build:%build.vcs.number%-%PYTHON_VERSION%"
            dockerImagePlatform = ExecBuildStep.ImagePlatform.Linux
            param("script.content", """
                rm -rf dist/
                mkdir dist
                python setup.py sdist -d dist/
                python setup.py bdist_wheel -d dist/
                python setup.py bdist_egg -d dist/
                find dist/
            """.trimIndent())
        }
        script {
            name = "Check static link"
            id = "RUNNER_280"
            scriptContent = """
                [[ "${'$'}(ldd build/lib.linux-x86_64-*/quasardb/quasardb.cpython-*-x86_64-linux-gnu.so | grep libstdc++ | wc -l)" == "0" ]]
                [[ "${'$'}(ldd build/lib.linux-x86_64-*/quasardb/quasardb.cpython-*-x86_64-linux-gnu.so | grep libgcc | wc -l)" == "0" ]]
            """.trimIndent()
            dockerImage = "build:%build.vcs.number%-%PYTHON_VERSION%"
            dockerImagePlatform = ScriptBuildStep.ImagePlatform.Linux
        }
        exec {
            name = "Generate documentation"
            id = "RUNNER_56"
            path = "bash"
            arguments = "scripts/teamcity/30.doc.sh"
            dockerImage = "build:%build.vcs.number%-%PYTHON_VERSION%"
            dockerImagePlatform = ExecBuildStep.ImagePlatform.Linux
            param("script.content", """
                python3 -m pip install dist/quasardb*.whl
                python3 -m pip install -r dev-requirements.txt
                mkdir doc || true
                python3 docgen.py
                tar -czvf dist/doc.tar.gz doc/*
            """.trimIndent())
        }
        script {
            name = "Smoke test"
            id = "RUNNER_57"
            scriptContent = """
                env
                python -m pip install -r dev-requirements.txt
                scripts/test/smoke-test.sh dist/quasardb*.whl
            """.trimIndent()
            dockerImage = "build:%build.vcs.number%-%PYTHON_VERSION%"
            dockerImagePlatform = ScriptBuildStep.ImagePlatform.Linux
        }
        exec {
            name = "Stop services"
            id = "RUNNER_144"
            executionMode = BuildStep.ExecutionMode.ALWAYS
            path = "bash"
            arguments = "scripts/tests/setup/stop-services.sh"
        }
    }

    failureConditions {
        executionTimeoutMin = 60
    }

    features {
        perfmon {
            id = "perfmon"
        }
        dockerSupport {
            id = "DockerSupport"
            loginToRegistry = on {
                dockerRegistryId = "PROJECT_EXT_39"
            }
        }
    }

    dependencies {
        dependency(AbsoluteId("Quasardb_Source_Composite")) {
            snapshot {
                onDependencyFailure = FailureAction.FAIL_TO_START
                synchronizeRevisions = false
            }

            artifacts {
                id = "ARTIFACT_DEPENDENCY_65"
                artifactRules = """
                    *-linux-64bit-core2-c-api.tar.gz!**/*=>qdb
                    *-linux-64bit-core2-utils.tar.gz!**/*=>qdb
                    *-linux-64bit-core2-server.tar.gz!**/*=>qdb
                """.trimIndent()
            }
        }
    }

    requirements {
        doesNotContain("teamcity.agent.jvm.os.arch", "arm", "RQ_311")
        doesNotContain("teamcity.agent.jvm.os.arch", "aarch64", "RQ_312")
    }
})
