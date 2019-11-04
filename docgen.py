import pdoc
import quasardb
import quasardb.pandas as qdbpd

modules = [quasardb.quasardb, qdbpd]  # Public submodules are auto-imported
context = pdoc.Context()

modules = [pdoc.Module(mod, context=context)
           for mod in modules]

pdoc.link_inheritance(context)

def recursive_htmls(mod):
    yield mod.name, mod.html()
    for submod in mod.submodules():
        yield from recursive_htmls(submod)

for mod in modules:
    for module_name, html in recursive_htmls(mod):
        f = open("doc/" + module_name + ".html", 'w')
        f.write(html)
        f.close()
