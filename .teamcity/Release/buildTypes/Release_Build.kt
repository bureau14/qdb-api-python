package Release.buildTypes

import jetbrains.buildServer.configs.kotlin.*
import jetbrains.buildServer.configs.kotlin.buildFeatures.XmlReport
import jetbrains.buildServer.configs.kotlin.buildFeatures.perfmon
import jetbrains.buildServer.configs.kotlin.buildFeatures.xmlReport
import jetbrains.buildServer.configs.kotlin.buildSteps.exec
import jetbrains.buildServer.configs.kotlin.buildSteps.placeholder
import jetbrains.buildServer.configs.kotlin.buildSteps.script

object Release_Build : Template({
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

        showDependenciesChanges = true
    }

    steps {
        placeholder {
            id = "RUNNER_321"
        }
        script {
            name = "Choose Python version"
            id = "RUNNER_141"
            enabled = false
            scriptContent = """
                sudo port select --set python python36
                python --version 2>&1 | grep 'Python 3\.' >/dev/null || (echo "Wrong Python version (${'$'}(python --version 2>&1))" && false)
            """.trimIndent()
        }
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
        exec {
            name = "Build"
            id = "RUNNER_112"
            path = "bash"
            arguments = "scripts/teamcity/10.build.sh"
            param("script.content", """
                rm -rf build/ dist/
                mkdir dist
                %env.PYTHON_EXECUTABLE% -m pip install --user --upgrade setuptools wheel
                %env.PYTHON_EXECUTABLE% -m pip install --user -r dev-requirements.txt
                %env.PYTHON_EXECUTABLE% setup.py bdist_egg -d dist/
                %env.PYTHON_EXECUTABLE% setup.py bdist_wheel -d dist/
            """.trimIndent())
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

    requirements {
        noLessThanVer("system.cmake.version", "2.8", "RQ_26")
    }
})
