set -x
set -e

BUILDROOT=$HOME/aiopg8000
PG_VERSION=9.2.9
PG_PORT=5492

source $BUILDROOT/circleci/install-postgresql-generic.sh
