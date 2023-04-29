package Windows_Win64.buildTypes

import jetbrains.buildServer.configs.kotlin.*
import jetbrains.buildServer.configs.kotlin.buildSteps.exec

object Windows_Win64_Python36 : BuildType({
    templates(Windows_Win64_Build)
    name = "Python 3.6"

    params {
        param("PYTHON_CMD", "%system.python36-64.exe%")
        param("env.PYTHON_EXECUTABLE", "%system.python36-64.exe%")
        param("env.PYTHON_CMD", "%system.python36-64.exe%")
    }

    steps {
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
    }

    requirements {
        equals("system.python36.cpu", "x64", "RQ_153")
    }
    
    disableSettings("RQ_153")
})
