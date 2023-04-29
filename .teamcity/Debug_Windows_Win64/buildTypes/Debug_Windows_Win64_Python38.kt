package Debug_Windows_Win64.buildTypes

import jetbrains.buildServer.configs.kotlin.*

object Debug_Windows_Win64_Python38 : BuildType({
    templates(Debug_Windows_Win64_Build)
    name = "Python 3.8"

    params {
        param("PYTHON_CMD", "%system.python38-64.exe%")
        param("env.PYTHON_EXECUTABLE", "%system.python38-64.exe%")
        text("env.PYTHON_CMD", "%system.python38-64.exe%", label = "Python executable", description = """Absolutely location to Python executable to use for the build, e.g. c:\Python3.6\python.exe""", allowEmpty = false)
    }

    requirements {
        equals("system.python38.cpu", "x64", "RQ_191")
    }
    
    disableSettings("RQ_191")
})
