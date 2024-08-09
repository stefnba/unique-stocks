#!/usr/bin/env bash

set -euxo pipefail

run_migrations(){
  if /opt/hive-metastore/bin/schematool -dbType "$DATABASE_TYPE" -validate | grep 'Done with metastore validation' | grep '[SUCCESS]'; then
    echo 'Database OK'
    return 0
  else
    # TODO: how to apply new version migrations or repair validation issues
    /opt/hive-metastore/bin/schematool --verbose -dbType "$DATABASE_TYPE" -initSchema
  fi
}

# configure hadoop & run schematool
envsubst < $TEMPLATE_PATH/hive-site.xml > /opt/hadoop/etc/hadoop/hive-site.xml
run_migrations

# configure metastore
envsubst < $TEMPLATE_PATH/metastore-site.xml > /opt/hive-metastore/conf/metastore-site.xml
envsubst < $TEMPLATE_PATH/core-site.xml > /opt/hadoop/etc/hadoop/core-site.xml

# start metastore
/opt/hive-metastore/bin/start-metastore
