package Debug_Windows_Win64.buildTypes

import jetbrains.buildServer.configs.kotlin.*
import jetbrains.buildServer.configs.kotlin.buildFeatures.perfmon
import jetbrains.buildServer.configs.kotlin.buildSteps.exec

object Debug_Windows_Win64_Build : Template({
    name = "Build"

    artifactRules = """
        dist/quasardb*.egg
        dist/quasardb*.whl
    """.trimIndent()

    params {
        param("JUNIT_XML_FILE", "build/test/pytest.xml")
        param("dependencies.artifact.rules", """
            *-c-api.zip!**/*.*=>qdb
            *-server.zip!**/*=>qdb
        """.trimIndent())
        param("env.JUNIT_XML_FILE", "build/test/pytest.xml")
        param("env.PYTHON_EXECUTABLE", "")
        text("env.PYTHON_CMD", "", label = "Python executable", description = """Absolutely location to Python executable to use for the build, e.g. c:\Python3.6\python.exe""", allowEmpty = false)
    }

    vcs {
        root(DslContext.settingsRoot)

        cleanCheckout = true
        showDependenciesChanges = true
    }

    steps {
        exec {
            name = "Start services"
            id = "RUNNER_102"
            path = "bash"
            arguments = "scripts/tests/setup/start-services.sh"
        }
        exec {
            name = "Test"
            id = "RUNNER_48"
            path = "bash"
            arguments = "scripts/teamcity/20.test.sh"
            param("script.content", """
                %PYTHON_CMD% -m venv .env/
                source .env/bin/activate
                %PYTHON_CMD% -m pip install --user -r dev-requirements.txt
                %PYTHON_CMD% setup.py test  --addopts "--junitxml=%JUNIT_XML_FILE%"
            """.trimIndent())
        }
        exec {
            name = "Stop services"
            id = "RUNNER_143"
            executionMode = BuildStep.ExecutionMode.ALWAYS
            path = "bash"
            arguments = "scripts/tests/setup/stop-services.sh"
        }
        exec {
            name = "Build"
            id = "RUNNER_112"
            path = "bash"
            arguments = "scripts/teamcity/10.build.sh"
            param("script.content", """
                rm -r -fo dist/
                %PYTHON_CMD% setup.py bdist_egg -d dist/
                %PYTHON_CMD% setup.py bdist_wheel -d dist/
            """.trimIndent())
        }
    }

    failureConditions {
        executionTimeoutMin = 60
    }

    features {
        perfmon {
            id = "perfmon"
        }
    }

    dependencies {
        dependency(AbsoluteId("Quasardb_Source_Composite")) {
            snapshot {
                onDependencyFailure = FailureAction.FAIL_TO_START
            }

            artifacts {
                id = "ARTIFACT_DEPENDENCY_109"
                cleanDestination = true
                artifactRules = """
                    *-windows-64bit-core2-c-api.zip!**/*.*=>qdb
                    *-windows-64bit-core2-server.zip!**/*=>qdb
                    *-windows-64bit-core2-utils.zip!**/*=>qdb
                """.trimIndent()
            }
        }
    }

    requirements {
        noLessThanVer("system.cl.version", "18", "RQ_137")
        noLessThanVer("system.cmake.version", "2.8", "RQ_26")
    }
})
