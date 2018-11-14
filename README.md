# Meetup trends analysis

### Problem Statement
For a given meetup RSVP JSON

* To figure out:
    1. Top N trending topics overall
    2. Top N trending topics per day
    3. Top N trending topics over given time frame
### Prerequisites

Gradle, 
Scala 2.11,
Spark 2.3.2

### Build
goto the project directory and run
```
gradle build
gradle copyDeps
```

## Run on Local

```
spark-submit --master "local[3]" --driver-class-path "build/libs/libext/*" --class com.data.MeetupTrendsAnalyzer build/libs/meetups_trends-0.1.jar input_params_as_defined_below
```
Please specify json file path, N for top N records, Y for per day/N for over total time period(Default Y)
and optionally start and end dates all separated by space to get trending topics  
  
  Examples:
- src/main/resources/meetup.json 10
- src/main/resources/meetup.json 10 N
- src/main/resources/meetup.json 20 Y 2018-03-10
- src/main/resources/meetup.json 30 N 2017-03-10 2018-04-10


## Run on YARN

```
export HADOOP_CONF_DIR=XXX
spark-submit \
  --class com.data.MeetupTrendsAnalyzer \
  --master yarn \
  --deploy-mode cluster \  # can be client for client mode
  --executor-memory 128M \
  --num-executors 3 \
  build/libs/meetups_trends-0.1.jar input_params_as_defined_above
```

## Output
Will be stored as json file in locations specified while running spark job
