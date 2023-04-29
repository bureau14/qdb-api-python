package Release_Osx.buildTypes

import jetbrains.buildServer.configs.kotlin.*

object Release_Osx_Python39 : BuildType({
    templates(Release.buildTypes.Release_Build)
    name = "Python 3.9"
    description = "End of life: 2025-10"

    params {
        param("env.PYTHON_EXECUTABLE", "/opt/local/Library/Frameworks/Python.framework/Versions/3.9/bin/python3")
        param("cmake.generator", "Ninja")
        param("env.PYTHON_CMD", "python3.9")
    }

    requirements {
        equals("teamcity.agent.jvm.os.name", "Mac OS X", "RQ_39")
        noLessThanVer("system.python.numpy.version", "1.0", "RQ_164")
        noLessThanVer("system.ninja.version", "1.0", "RQ_38")
        exists("system.python39.version", "RQ_148")
        noLessThanVer("system.appleclang.version", "13.0.0", "RQ_295")
    }
})
