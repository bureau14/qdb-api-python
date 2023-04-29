package Debug_Osx.buildTypes

import jetbrains.buildServer.configs.kotlin.*

object Debug_Osx_Python36 : BuildType({
    templates(Debug_Osx_Build)
    name = "Python 3.6"
    description = "End of life: 2021-12-23"

    params {
        param("env.PYTHON_EXECUTABLE", "/opt/local/Library/Frameworks/Python.framework/Versions/3.6/bin/python3")
        param("cmake.generator", "Ninja")
        param("env.PYTHON_CMD", "python3.6")
    }

    requirements {
        equals("teamcity.agent.jvm.os.name", "Mac OS X", "RQ_39")
        noLessThanVer("system.python.numpy.version", "1.0", "RQ_164")
        noLessThanVer("system.ninja.version", "1.0", "RQ_38")
        exists("system.python36.version", "RQ_170")
        noLessThanVer("system.appleclang.version", "13.0.0", "RQ_296")
    }
})
