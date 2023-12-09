# Kafka Samples

A set of kafka sample applications.
It will include a range set of demo apps using kafka.
Sample project can come from well-known kafka courses as well as 
from own inspiration.

## Projects
 - `wikimedia-to-kafka` - Stream Wikimedia recent changes using SSE endpoint to a kafka topic.

## Gradle

This is multi-module root project.

To build all sub-projects at once - just run gradle:

```bash
./gradlew build
```

to build only particular subproject run inside root dir:

```bash
./gradlew wikimedia-to-kafka:build
```

to run subproject:

```bash
./gradlew wikimedia-to-kafka:run
```

In subproject directories use relative path, e.g.:

```bash
../gradlew ... 
```

Or you can build 