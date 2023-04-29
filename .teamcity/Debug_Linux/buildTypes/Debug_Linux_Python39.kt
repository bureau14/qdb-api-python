package Debug_Linux.buildTypes

import jetbrains.buildServer.configs.kotlin.*

object Debug_Linux_Python39 : BuildType({
    templates(Debug_Linux_Build)
    name = "Python 3.9"

    params {
        text("PYTHON_VERSION", "3.9.13", label = "Python version", description = "Python version to install. Files will be downloaded from s3://qdbbuilddeps/linux/python/Python-x.y.z.tgz", allowEmpty = false)
    }
})
