#!/bin/sh
#../storm-0.9.3/bin/storm jar ../deploy/squall-0.2.0-standalone.jar ch.epfl.data.sql.main.ParserMain ../test/squall/confs/cluster/0_01G_hyracks
export STORM_JAR_JVM_OPTS="-Dscala.usejavacp=true"
../storm-0.9.3/bin/storm jar ../deploy/squall-0.2.0-standalone.jar ch.epfl.data.plan_runner.main.Main ../test/squall_plan_runner/confs/cluster/0_01G_dummy -c scala.usejavacp=true
