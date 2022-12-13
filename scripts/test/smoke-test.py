import sys
import quasardb

print("# Test 1 -- print version: {}".format(quasardb.version()))
print("# Test 1 -- pass!")
print()

print("# Test 2 -- verify unit tests are not part of release build")
exception_thrown=False
try:
    from quasardb import tests
except:
    exception_thrown=True
    print("# Test 2 -- exception_thrown")
    pass

if exception_thrown is False:
    raise RuntimeError("Smoke test failed: quasardb.tests is importable, which should not be shipped in release builds")

print("# Test 2 -- pass!")
print()
