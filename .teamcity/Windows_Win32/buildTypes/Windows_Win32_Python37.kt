package Windows_Win32.buildTypes

import jetbrains.buildServer.configs.kotlin.*

object Windows_Win32_Python37 : BuildType({
    templates(Windows_Win32_Build)
    name = "Python 3.7"

    params {
        param("PYTHON_CMD", "%system.python37-32.exe%")
        param("env.PYTHON_EXECUTABLE", "%system.python37-32.exe%")
        param("env.PYTHON_CMD", "%system.python37-32.exe%")
    }

    requirements {
        equals("system.python37.cpu", "x86", "RQ_191")
    }
    
    disableSettings("RQ_191")
})
