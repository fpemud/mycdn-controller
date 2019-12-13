#!/bin/bash

FILES="./mirrors"
LIBFILES=""
LIBFILES="${LIBFILES} $(find ./lib -name '*.py' | tr '\n' ' ')"
LIBFILES="${LIBFILES} $(find ./libexec -name '*.py' | tr '\n' ' ')"
ERRFLAG=0

OUTPUT=`pyflakes ${FILES} ${LIBFILES} 2>&1`
if [ -n "$OUTPUT" ] ; then
    echo "pyflake errors:"
    echo "$OUTPUT"
    echo ""
    ERRFLAG=1
fi

OUTPUT=`pycodestyle ${FILES} ${LIBFILES} | grep -Ev "E501|E402"`
if [ -n "$OUTPUT" ] ; then
    echo "pycodestyle errors:"
    echo "$OUTPUT"
    echo ""
    ERRFLAG=1
fi

if [ "${ERRFLAG}" == 1 ] ; then
    exit 1
fi
