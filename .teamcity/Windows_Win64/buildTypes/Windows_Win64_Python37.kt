package Windows_Win64.buildTypes

import jetbrains.buildServer.configs.kotlin.*

object Windows_Win64_Python37 : BuildType({
    templates(Windows_Win64_Build)
    name = "Python 3.7"

    params {
        param("PYTHON_CMD", "%system.python37-64.exe%")
        param("env.PYTHON_EXECUTABLE", "%system.python37-64.exe%")
        param("env.PYTHON_CMD", "%system.python37-64.exe%")
    }

    requirements {
        equals("system.python37.cpu", "x64", "RQ_191")
    }
    
    disableSettings("RQ_191")
})
