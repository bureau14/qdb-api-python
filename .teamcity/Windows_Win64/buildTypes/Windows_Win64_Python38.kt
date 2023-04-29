package Windows_Win64.buildTypes

import jetbrains.buildServer.configs.kotlin.*

object Windows_Win64_Python38 : BuildType({
    templates(Windows_Win64_Build)
    name = "Python 3.8"

    params {
        param("PYTHON_CMD", "%system.python38-64.exe%")
        param("env.PYTHON_EXECUTABLE", "%system.python38-64.exe%")
        param("env.PYTHON_CMD", "%system.python38-64.exe%")
    }

    requirements {
        equals("system.python38.cpu", "x64", "RQ_191")
    }
    
    disableSettings("RQ_191")
})
