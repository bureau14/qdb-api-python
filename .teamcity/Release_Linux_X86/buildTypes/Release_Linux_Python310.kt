package Release_Linux_X86.buildTypes

import jetbrains.buildServer.configs.kotlin.*

object Release_Linux_Python310 : BuildType({
    templates(Release_Linux.buildTypes.Release_Linux_Build)
    name = "Python 3.10"

    params {
        text("PYTHON_VERSION", "3.10.8", label = "Python version", description = "Python version to install. Files will be downloaded from s3://qdbbuilddeps/linux/python/Python-x.y.z.tgz", allowEmpty = false)
    }
})
