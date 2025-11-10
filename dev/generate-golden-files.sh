#!/bin/bash
export SPARK_HOME=`pwd`

#./mvnw install -DskipTests -Pspark-3.4
#SPARK_GENERATE_GOLDEN_FILES=1 ./mvnw -pl spark -Dsuites="org.apache.spark.sql.comet.CometTPCDSV1_4_PlanStabilitySuite" -Pspark-3.4 -nsu test
#SPARK_GENERATE_GOLDEN_FILES=1 ./mvnw -pl spark -Dsuites="org.apache.spark.sql.comet.CometTPCDSV2_7_PlanStabilitySuite" -Pspark-3.4 -nsu test

./mvnw install -DskipTests -Pspark-3.5
SPARK_GENERATE_GOLDEN_FILES=1 ./mvnw -pl spark -Dsuites="org.apache.spark.sql.comet.CometTPCDSV1_4_PlanStabilitySuite" -Pspark-3.5 -nsu test
SPARK_GENERATE_GOLDEN_FILES=1 ./mvnw -pl spark -Dsuites="org.apache.spark.sql.comet.CometTPCDSV2_7_PlanStabilitySuite" -Pspark-3.5 -nsu test

#./mvnw install -DskipTests -Pspark-4.0
#SPARK_GENERATE_GOLDEN_FILES=1 ./mvnw -pl spark -Dsuites="org.apache.spark.sql.comet.CometTPCDSV1_4_PlanStabilitySuite" -Pspark-4.0 -nsu test
#SPARK_GENERATE_GOLDEN_FILES=1 ./mvnw -pl spark -Dsuites="org.apache.spark.sql.comet.CometTPCDSV2_7_PlanStabilitySuite" -Pspark-4.0 -nsu test
