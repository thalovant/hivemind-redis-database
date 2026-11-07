# START_VERSION_BLOCK
VERSION_MAJOR = 0
VERSION_MINOR = 1
VERSION_BUILD = 1
VERSION_ALPHA = 8
# END_VERSION_BLOCK

# A downstream build carries a PEP 440 local label, so an installed copy says
# it is not the published release of the same number.
VERSION_LOCAL = "thalovant.2"

__version__ = (f"{VERSION_MAJOR}.{VERSION_MINOR}.{VERSION_BUILD}" + (f"a{VERSION_ALPHA}" if VERSION_ALPHA else "")
               + (f"+{VERSION_LOCAL}" if VERSION_LOCAL else ""))
