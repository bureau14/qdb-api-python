package Windows_Win64.buildTypes

import jetbrains.buildServer.configs.kotlin.*

object Windows_Win64_Python310 : BuildType({
    templates(Windows_Win64_Build)
    name = "Python 3.10"

    params {
        text("PYTHON_CMD", "%system.python310-64.exe%", label = "Python executable", description = """Absolutely location to Python executable to use for the build, e.g. c:\Python3.6\python.exe""", allowEmpty = false)
        param("env.PYTHON_EXECUTABLE", "%system.python310-64.exe%")
        text("env.PYTHON_CMD", "%system.python310-64.exe%", label = "Python executable", description = """Absolutely location to Python executable to use for the build, e.g. c:\Python3.6\python.exe""", allowEmpty = false)
    }

    requirements {
        equals("system.python39.cpu", "x64", "RQ_191")
    }
    
    disableSettings("RQ_191")
})
