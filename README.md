
# bpulse-sdk-java

Bpulse SDK Java or BPulse Java Client is a conector between any java based application subscribed to BPULSE Service and the PULSES COLLECTOR REST SERVICE.
This README explains how to integrate the conector with the target client application, configuration parameters and how to use it.

# Requirements

* [bpulse-protobuf-java](https://github.com/bpulse/bpulse-protobuf-java)
* [Apache Maven 3.x.x](https://maven.apache.org/download.cgi)
* [JDK Version 1.7+](http://www.oracle.com/technetwork/java/javase/downloads/jdk7-downloads-1880260.html)

# Build dependencies
The following dependencies are required to build the sdk and are also required in the classpath of your application at runtime:

* **BPulse dependencies**
 * bpulse.protobuf\[latest].jar
* **Google Protobuf dependencies**
 * protobuf-java-format-1.2.jar
 * protobuf-java.2.5.0.jar
* **Apache http dependencies**
 * httpclient-4.4.1.jar
 * httpcore-4.4.1.jar
 * commons-logging-1.2.jar
 * commons-codec-1.9.jar
* **H2 Database Engine dependencies**
 * h2-1.4.186.jar
* **SLF4J dependencies**
 * slf4j-api-1.7.5.jar
 * Corresponding Binding for used logging framework (See **Binding with a logging framework at deployment time** at [http://www.slf4j.org/manual.html](http://www.slf4j.org/manual.html))

This is a maven project, so all of this dependencies are already added in the given pom.xml but you must have them in mind if you build your application without maven or the runtime classpath is provided by another third party.

# Build
Just clone the repository and make sure all the dependencies are satisfied, then in a terminal go to the project folder and execute:

```bash
$ mvn clean install
```
**Notes:**
* If any errors occurs during build related to bpulse-protobuf dependency, please make sure that the version in this pom.xml matches with the version you build of [bpulse-protobuf-java](https://github.com/bpulse/bpulse-protobuf-java).
 

# Importing the project

## Importing to a Maven project
After building the project and install it on the maven repo, add this dependency to your pom.xml

```xml
<dependency>
	<groupId>me.bpulse</groupId>
	<artifactId>bpulse-java-client</artifactId>
	<version>1.0.0-SNAPSHOT</version>
</dependency>

```

Remember the dependencies mentioned above incase your current classpath doesn't have them at runtime, also keep in mind the version of this project you are building.

## Importing to a classic Java project
Build the sdk using
```bash
$ mvn clean package
```
Then take the generated bpulse-java-client-[version].jar under target/ directory and add it to your classpath along with
the other dependencies mentioned.

## Starting your application
The SDK needs some configuration properties that indicate the client how should work and where to connect so you must provide a properties file when you start your application or the application server where your application run, so to do that, simply provide the **bpulse.client.config** as *VM_ARG*, for example:
```bash
$ java -jar myapp.jar -Dbpulse.client.config=path/to/config.properties
```
If your application runs on an application server, simply append this vm arg to the existing one depending on your server.

This is an example of a basic configuration file content:
```properties
#BPULSE JAVA CLIENT CONFIGURATION PROPERTIES
bpulse.client.periodInMinutesNextExecTimer=1
bpulse.client.maxNumberPulsesReadFromTimer=240000
bpulse.client.bpulseUsername=collector@hotelbeds.com
bpulse.client.bpulsePassword=collector123
bpulse.client.bpulseRestURL=http://[bpulse.host]/app.collector/collector/pulses
bpulse.client.pulsesRepositoryDBPath=path/to/any/temp/folder
bpulse.client.pulsesRepositoryDBMaxSizeBytes=10737418240
bpulse.client.pulsesRepositoryMode=MEM
bpulse.client.pulsesRepositoryMemMaxNumberPulses=750000
```

# Usage

The starting point is the BPulseJavaClient class. It implements two methods: getInstance() and sendPulse(PulsesRQ) to publish them via BPULSE COLLECTOR REST SERVICE.

```java
//get the BPulseJavaClient instance. It manages the pulses repository and begins the pulses notification timer.
BPulseJavaClient client = BPulseJavaClient.getInstance();
```

Then use a combination of *me.bpulse.domain.proto.collector.CollectorMessageRQ.PulsesRQ*, *me.bpulse.domain.proto.collector.CollectorMessageRQ.Value* and *me.bpulse.domain.proto.collector.CollectorMessageRQ.Pulse* in order to build the pulses you want to send according to the Pulse Definition made in BPULSE, for example:

```java
//Request instance
PulsesRQ request;
//Use the builder provided to create pulses instances
PulsesRQ.Builder pulses = PulsesRQ.newBuilder();
//Pulse version, send 1.0 always, we will use this field later.
pulses.setVersion("1.0");

//Use the Pulse builder to create each pulse individually
Pulse.Builder pulse = Pulse.newBuilder();

//Name of the pulse definition, the same as defined using the BPULSE web app
pulse.setTypeId("bpulse_hotelbeds_jfp");
//Time of the pulse, usually should be the current time but you can set whatever time you need
pulse.setTime(System.currentTimeMillis());
//
pulse.setInstanceId(String.valueOf(1));

//Use the Value builder to assing the different pulse values to each pulse
Value.Builder value = Value.newBuilder();
//Name of the pulse attribute
value.setName("attribute_name");
//Value of the current attribute
value.addValues("attribute_value");
//Add the created value to the pulse instance
pulse.addValues(value);

//Same as before but for a time value TODO Joda time
value = Value.newBuilder();
value.setName("fechaProceso");			
value.addValues(fmt.print(new DateTime()));
pulse.addValues(value);

//Same as before but for a numeric value
value = Value.newBuilder();
value.setName("numeric_attribute");
value.addValues("123456789");
pulse.addValues(value);

//Add the pulse to the pulses collection
pulses.addPulse(pulse);

//Then build the pulses request
request = pulses.build();

```

Finally send the pulse created with:

```java
//invoke the operation for inserting the pulse into pulses repository.
client.sendPulse(request);

```

And that's it!, now you are sending pulses to BPULSE and can see them using the web dashboard.

## Available Configuration Parameters

BPulse java client has a configuration file to define the main parameters for sending and processing pulses (pulses repository path, number of threads for notifying pulses via BPULSE COLLECTOR REST SERVICE, etc.). It's definition is expected through java options property **bpulse.client.config** (e.g **-Dbpulse.client.config=C:\tmp\config.properties**).

All properties are defined below:

| Variable name        | Description           |
|:------------- |:------------- |
|bpulse.client.periodInMinutesNextExecTimer|Delay time in minutes between timer executions for pulses notification (default value = 1). |
|bpulse.client.periodInMinutesNextExecTimer|Delay time in minutes between timer executions for pulses notification (default value = 1). |
|bpulse.client.maxNumberPulsesReadFromTimer|Max number of read pulses for each timer execution from pulsesRepositoryDB for sending to BPULSE COLLECTOR REST SERVICE (default value = 180000). |
|bpulse.client.bpulseUsername|Client's Username for sending pulses to BPULSE COLLECTOR SERVICE. |
|bpulse.client.bpulsePassword|Client's Password  for sending pulses to BPULSE COLLECTOR SERVICE. |
|bpulse.client.bpulseRestURL| BPULSE COLLECTOR REST SERVICE URL. |
|bpulse.client.pulsesRepositoryDBPath|System Path to create the Pulses Repository (e.g C:/tmp/pulses_repository). |
|bpulse.client.pulsesRepositoryDBMaxSizeBytes|Pulses Repositories' Allowed max size in bytes (default value = 1073741824). |
|bpulse.client.pulsesRepositoryMode|Pulses Repositories' Mode:  MEM=PULSES IN MEMORY DB= PULSES IN EMBEDDED DATABASE. |
|bpulse.client.pulsesRepositoryMemMaxNumberPulses|When the pulses repositories' mode is MEM, it's necessary define the maximum number of pulses in memory(default value = 1000000). |

An example of configuration file is shown:


## About Logging
BPulse Java Client uses SLF4J API for register logs from pulse processing sending via BPULSE REST SERVICE. SLF4J uses a set of binding dependencies for each supported logging framework (log4j, tinylog, jdk logging, logback). If the target application uses someone of these frameworks, it's neccessary add the related 
binding dependency like these:

* **SLF4J Logging Framework Bindings**

```xml
<!-- Dependency for Tinylog binding -->
<dependency>
	<groupId>org.tinylog</groupId>
	<artifactId>slf4j-binding</artifactId>
	<version>1.0</version>
</dependency>

<!-- Dependency for Logback binding -->
<dependency>
	<groupId>ch.qos.logback</groupId>
	<artifactId>logback-classic</artifactId>
	<version>1.0.13</version>
</dependency>

<!-- Dependency for Apache log4j binding -->
<dependency> 
	<groupId>org.slf4j</groupId> 
	<artifactId>slf4j-log4j12</artifactId> 
	<version>1.7.5</version> 
</dependency>

```

Each binding is associated with a version of logging API (i.e in the log4j case, the version 1.7.5 of slf4j-log4j12 uses by default Apache log4j 1.2.17).
If your target application uses another version for these logging APIs, you must exclude it from the maven dependency and manage your own logging version. 
In the case of log4j it would be something like this:


```xml
<!-- Excludes the log4j default version managed by SLF4J binding -->
<dependency> 
	<groupId>org.slf4j</groupId> 
	<artifactId>slf4j-log4j12</artifactId> 
	<version>1.7.5</version> 
	<exclusions> 
		<exclusion> 
			<groupId>log4j</groupId> 
			<artifactId>log4j</artifactId> 
		</exclusion> 
	</exclusions> 
</dependency>

<!-- Includes your own log4j version (1.2.12 instead of 1.2.17 in this case) -->
<dependency> 
	<groupId>log4j</groupId> 
	<artifactId>log4j</artifactId> 
	<version>1.2.12</version> 
</dependency>

```

## Logging Configuration Parameters

After selecting the logging api, it's necessary to add a java option according to the used logging framework:

**tinylog java option:** -Dtinylog.configuration=C:\tmp\tinylog.properties

**Tinylog's properties file example:**

```properties
tinylog.writer = rollingfile
tinylog.writer.filename = C:/tmp/log/bpulse-java-client-tinylog.log
tinylog.writer.backups = 10
tinylog.writer.label = timestamp
tinylog.writer.policies = startup, size: 10KB
```

**log4j java option:** -Dlog4j.configuration=file:"C:\tmp\log4j.properties"

**log4j's properties file example:**

```properties
##LOG4J CONFIGURATION##
log4j.logger.bpulseLogger=INFO, bpulseLogger
# File appender
log4j.appender.bpulseLogger=org.apache.log4j.RollingFileAppender
log4j.appender.bpulseLogger.layout=org.apache.log4j.PatternLayout
#%-7p %d{(dd/MM/yyyy) HH:mm:ss} [%c{1}]%t %m%n
#%d{yyyy-MM-dd HH:mm:ss} %-5p - %m%n
log4j.appender.bpulseLogger.layout.ConversionPattern=%-7p %d{(dd/MM/yyyy) HH:mm:ss} [%c{1}]%t %m%n
log4j.appender.bpulseLogger.File=C:/tmp/log/bpulse-java-client.log
log4j.appender.bpulseLogger.MaxFileSize=25MB
log4j.appender.bpulseLogger.MaxBackupIndex=10
```

**logback java option:** -Dlogback.configurationFile=C:\tmp\logback.xml

**logback's properties file example:**

```xml
<configuration>
<appender name="FILE" class="ch.qos.logback.core.FileAppender">
    <file>C:/tmp/log/bpulse-java-client-logback.log</file>

    <encoder>
      <pattern>%date %level [%thread] %logger{10} [%file:%line] %msg%n</pattern>
    </encoder>
  </appender>

  <appender name="STDOUT" class="ch.qos.logback.core.ConsoleAppender">
    <encoder>
      <pattern>%msg%n</pattern>
    </encoder>
  </appender>

  <root level="debug">
    <appender-ref ref="FILE" />
    <appender-ref ref="STDOUT" />
  </root>
</configuration>
``` 

In case of the target system has its own logging properties file, it's necessary to add the corresponding lines mentioned above to it.

# Contact us

You can reach the Developer Platform team at jtenganan@innova4j.com

# License

The Bpulse Protobuf Java is licensed under the Apache License 2.0. Details can be found in the LICENSE file.

