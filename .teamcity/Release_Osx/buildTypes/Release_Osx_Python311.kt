package Release_Osx.buildTypes

import jetbrains.buildServer.configs.kotlin.*

object Release_Osx_Python311 : BuildType({
    templates(Release.buildTypes.Release_Build)
    name = "Python 3.11"
    description = "End of life: 2026-10"

    params {
        param("env.PYTHON_EXECUTABLE", "/opt/local/Library/Frameworks/Python.framework/Versions/3.11/bin/python3")
        param("cmake.generator", "Ninja")
        param("env.PYTHON_CMD", "python3.11")
    }

    requirements {
        equals("teamcity.agent.jvm.os.name", "Mac OS X", "RQ_39")
        noLessThanVer("system.python.numpy.version", "1.0", "RQ_164")
        noLessThanVer("system.ninja.version", "1.0", "RQ_38")
        noLessThanVer("system.appleclang.version", "13", "RQ_299")
        exists("system.python311.version", "RQ_148")
    }
})
