package Windows_Win32.buildTypes

import jetbrains.buildServer.configs.kotlin.*

object Windows_Win32_Python36 : BuildType({
    templates(Windows_Win32_Build)
    name = "Python 3.6"

    params {
        param("PYTHON_CMD", "%system.python36-32.exe%")
        param("env.PATH", "%system.python36-32.dir%;%env.PATH%")
        param("env.PYTHON_EXECUTABLE", "%system.python36-32.exe%")
        param("env.PYTHON_CMD", "%system.python36-32.exe%")
    }

    requirements {
        equals("system.python36.cpu", "x86", "RQ_153")
    }
    
    disableSettings("RQ_153")
})
