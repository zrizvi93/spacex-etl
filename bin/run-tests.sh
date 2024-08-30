: ${PYTHONPATH:=}
export PYTHONPATH="${PYTHONPATH}:`pwd`/src"

pytest "./tests"