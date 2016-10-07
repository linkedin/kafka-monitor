# Kafka Monitor Docker build
This build process assumes that you have already built the application
using gradlew as specified in the parent README.  If you haven't, this 
build will error out complaining of missing files.

## Getting Started

### Build the container locally
Decide where you would like the built container to be stored.  The 
default is quay.io/coffeepac/kafak-monitor.  You can probably not
write to this location so you should specifiy a new location either
at the command line or in an environment variable

Using default tag/repo location
```
make container
```

Using command line flags
```
make container -e TAG=0.2
```

Using environment variables
```
export PREFIX=quay.io/your_userid/kafka-monitor
make container
```

### Push container to remote repository
```
make push
```
