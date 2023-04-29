package Debug.buildTypes

import jetbrains.buildServer.configs.kotlin.*
import jetbrains.buildServer.configs.kotlin.buildFeatures.XmlReport
import jetbrains.buildServer.configs.kotlin.buildFeatures.perfmon
import jetbrains.buildServer.configs.kotlin.buildFeatures.xmlReport
import jetbrains.buildServer.configs.kotlin.buildSteps.exec
import jetbrains.buildServer.configs.kotlin.buildSteps.script

object Debug_Build : Template({
    name = "Build"

    artifactRules = """
        dist/quasardb*.egg
        dist/quasardb*.whl
    """.trimIndent()

    params {
        param("archive.ext", "tar.gz")
        param("JUNIT_XML_FILE", "build/test/pytest.xml")
        param("dependencies.artifact.rules", """
            *-c-api.%archive.ext%!**/*=>qdb
            *-utils.%archive.ext%!**/*=>qdb
            *-server.%archive.ext%!**/*=>qdb
        """.trimIndent())
        param("env.PYTHON_EXECUTABLE", "python3")
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
        script {
            name = "Test"
            id = "RUNNER_48"
            scriptContent = """
                export QDB_TESTS_ENABLED=ON
                %env.PYTHON_EXECUTABLE% -m pip install --user -r dev-requirements.txt
                %env.PYTHON_EXECUTABLE% setup.py test --addopts "--junitxml=%JUNIT_XML_FILE%"
                export QDB_TESTS_ENABLED=OFF
            """.trimIndent()
        }
        exec {
            name = "Stop services"
            id = "RUNNER_143"
            executionMode = BuildStep.ExecutionMode.ALWAYS
            path = "bash"
            arguments = "scripts/tests/setup/stop-services.sh"
        }
        script {
            name = "Build"
            id = "RUNNER_112"
            scriptContent = """
                rm -rf dist/ build/
                mkdir dist
                %env.PYTHON_EXECUTABLE% -m pip install --user --upgrade setuptools wheel
                %env.PYTHON_EXECUTABLE% -m pip install --user -r dev-requirements.txt
                %env.PYTHON_EXECUTABLE% setup.py bdist_egg -d dist/
                %env.PYTHON_EXECUTABLE% setup.py bdist_wheel -d dist/
            """.trimIndent()
        }
        script {
            name = "Smoke test"
            id = "RUNNER_98"
            workingDir = "scripts/test/"
            scriptContent = "./smoke-test.sh ../../dist/quasardb*.whl"
        }
    }

    failureConditions {
        executionTimeoutMin = 120
    }

    features {
        xmlReport {
            id = "BUILD_EXT_3"
            enabled = false
            reportType = XmlReport.XmlReportType.JUNIT
            rules = "%JUNIT_XML_FILE%"
            verbose = true
        }
        perfmon {
            id = "perfmon"
        }
    }

    dependencies {
        dependency(AbsoluteId("Quasardb_Source_Composite")) {
            snapshot {
                onDependencyFailure = FailureAction.FAIL_TO_START
                synchronizeRevisions = false
            }

            artifacts {
                id = "ARTIFACT_DEPENDENCY_171"
                cleanDestination = true
                artifactRules = """
                    *-%platform%-debug-c-api.*!**/*=>qdb
                    *-%platform%-debug-utils.*!**/*=>qdb
                    *-%platform%-debug-server.*!**/*=>qdb 
                """.trimIndent()
            }
        }
    }

    requirements {
        noLessThanVer("system.cmake.version", "2.8", "RQ_26")
    }
})
