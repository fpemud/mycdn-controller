#!/bin/bash

FILES="./mirrors"
LIBFILES=""
LIBFILES="${LIBFILES} $(find ./lib -name '*.py' | tr '\n' ' ')"
LIBFILES="${LIBFILES} $(find ./python3 -name '*.py' | tr '\n' ' ')"

autopep8 -ia --ignore=E501,E402 ${FILES} ${LIBFILES}