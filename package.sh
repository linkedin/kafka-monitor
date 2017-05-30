VERSION=0.1
OUTPUT="kafka-monitor-$VERSION.zip"

if [ -f $OUTPUT ]; then
    rm $OUTPUT
fi

gradlew jar

zip -r $OUTPUT build/ config/ bin/    
