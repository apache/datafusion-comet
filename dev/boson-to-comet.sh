SPARK_BASE=.

sed -i.bak -E 's/com.apple.pie.boson/org.apache.comet/g' $SPARK_BASE/pom.xml
sed -i.bak -E 's/boson/comet/g' $SPARK_BASE/pom.xml

sed -i.bak -E 's/com.apple.pie.boson/org.apache.comet/g' $SPARK_BASE/sql/core/pom.xml
sed -i.bak -E 's/boson/comet/g' $SPARK_BASE/sql/core/pom.xml

BOSON_FILES=$(find $SPARK_BASE \( -name "*.java" -o -name "*.scala" -o -name "*.xml" -o -name "*.sql" -o -name "*.js" -o -name "*.css" \) | xargs grep -li boson)
for file in $BOSON_FILES; do
  sed -i.bak -E 's/com.apple.boson/org.apache.comet/g' $file
  sed -i.bak -E 's/org.apache.spark.sql.boson/org.apache.spark.sql.comet/g' $file
  sed -i.bak -E 's/spark.boson/spark.comet/g' $file
  sed -i.bak -E 's/boson/comet/g' $file
  sed -i.bak -E 's/Boson/Comet/g' $file
  sed -i.bak -E 's/BOSON/COMET/g' $file
done

find $SPARK_BASE -name "*.bak" | xargs rm -f {} \;
