package Windows_Win32.buildTypes

import jetbrains.buildServer.configs.kotlin.*

object Windows_Win32_Python39 : BuildType({
    templates(Windows_Win32_Build)
    name = "Python 3.9"

    params {
        text("PYTHON_CMD", "%system.python39-32.exe%", label = "Python executable", description = """Absolutely location to Python executable to use for the build, e.g. c:\Python3.6\python.exe""", allowEmpty = false)
        param("env.PYTHON_EXECUTABLE", "%system.python39-32.exe%")
        param("env.PYTHON_CMD", "%system.python39-32.exe%")
    }

    requirements {
        equals("system.python39.cpu", "x86", "RQ_191")
    }
    
    disableSettings("RQ_191")
})
