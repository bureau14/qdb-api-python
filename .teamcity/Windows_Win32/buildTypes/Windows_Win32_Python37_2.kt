package Windows_Win32.buildTypes

import jetbrains.buildServer.configs.kotlin.*

object Windows_Win32_Python37_2 : BuildType({
    templates(Windows_Win32_Build)
    name = "Python 3.8"

    params {
        param("PYTHON_CMD", "%system.python38-32.exe%")
        param("env.PYTHON_EXECUTABLE", "%system.python38-32.exe%")
        param("env.PYTHON_CMD", "%system.python38-32.exe%")
    }

    requirements {
        equals("system.python38.cpu", "x86", "RQ_191")
    }
    
    disableSettings("RQ_191")
})
