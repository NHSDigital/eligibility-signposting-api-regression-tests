#!/bin/sh
# This loads the shims directory directly, as there is no asdf.sh to source.
# The `:-` syntax provides a default value if ASDF_DATA_DIR is not set.
export PATH="${ASDF_DATA_DIR:-$HOME/.asdf}/shims:${ASDF_DATA_DIR:-$HOME/.asdf}/bin:$PATH"

# Execute actionlint
exec actionlint
