import os
import pdoc
import quasardb
import quasardb.pool
import quasardb.stats
import quasardb.numpy as qdbnp
import quasardb.pandas as qdbpd

context = pdoc.Context()

# This is a hack: pydoc has a lot of issues with importing submodules properly. It's
# related to pybind11 generating invalid docstrings, and we would get import errors
# and whatnot. E.g. the quasardb.Blob() has a `expiry=datetime.datetime()` mention
# in the type, _but_ the actual modules (as python sees it) it generates don't even
# import datetime.
#
# This then causes pydoc to throw an error.
#
# As such, we're making our own module subclass here, which will allow us to manually
# tell pybind which submodules to load, rather than telling it to traverse everything
# automatically.
class Module(pdoc.Module):
    def __init__(self, *args, submodules=[], **kwargs):
        super().__init__(*args, **kwargs)
        self._submodules = submodules

    def submodules(self):
        return self._submodules


module_qdb = Module(quasardb.quasardb, context=context,
                    submodules=[pdoc.Module(quasardb.pool, context=context),
                                pdoc.Module(quasardb.stats, context=context),
                                pdoc.Module(quasardb.numpy, context=context),
                                pdoc.Module(quasardb.pandas, context=context)])

modules = [module_qdb]

pdoc.link_inheritance(context)

def recursive_htmls(mod):
    yield mod.name, mod.html()
    for submod in mod.submodules():
        yield from recursive_htmls(submod)

def _strip_prefix(s, p):
    if s.startswith(p):
        return s[len(p):]
    else:
        return s

def write_module(filename, html):
    with open(filename, 'w') as f:
        f.write(html)


for mod in modules:
    for module_name, html in recursive_htmls(mod):
        # This hack is related to the fact that _sometimes_, a module is called
        # `quasardb.quasardb`, and other times it's just `quasardb`. Apparently, when
        # a module's name is `quasardb.pool`, pydoc thinks the file should be called
        # `pool` :/
        #
        # So, just to make sure we have "everything", we write each module twice:
        # once with the quasardb. prefix, and once without.
        write_module("doc/" + module_name + ".html", html)
        write_module("doc/" + _strip_prefix(module_name, 'quasardb.') + ".html", html)

        try:
            os.mkdir("doc/" + _strip_prefix(module_name, 'quasardb.'))
        except FileExistsError:
            pass

        write_module("doc/" + _strip_prefix(module_name, 'quasardb.') + "/index.html", html)
