# README #

BPulse Java Client is a conector between any client subscribed to BPULSE Service and the PULSES COLLECTOR REST SERVICE.
This README explains how to integrate the conector with the target client application, configuration parameters and how to use it.

### REQUIREMENTS ###

BPulse Java Client is a maven project. It requires the following dependencies for being used in any java maven project:

* **bpulse-java-client**


```
#!xml
<dependency>
	<groupId>me.bpulse</groupId>
	<artifactId>bpulse-java-client</artifactId>
	<version>1.0.0-SNAPSHOT</version>
</dependency>

```

* **SLF4J Logging Framework Bindings**

BPulse Java Client uses SLF4J API for register logs from pulses processing and sending via BPULSE REST SERVICE. SLF4J uses a set of binding dependencies for each
supported logging framework (log4j, tinylog, jdk logging, logback). If the target application uses someone of these frameworks, it's neccessary add the related 
binding dependency like these:


```
#!xml
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
If your target application uses another version for these logging APIs, you must excludes it from the maven dependency and manage your own logging version. 
In the case of log4j it would be like this:


```
#!xml
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

If your target application is not a maven project, you must include the following jars in your libs folder:

* BPulse dependencies
 * bpulse.java.client-1.0.0-SNAPSHOT.jar
 * bpulse.protobuf-0.3.3-SNAPSHOT.jar
* Google Protobuf dependencies
 * protobuf-java-format-1.2.jar
 * protobuf-java.2.5.0.jar
* Apache http dependencies
 * httpclient-4.4.1.jar
 * httpcore-4.4.1.jar
 * commons-logging-1.2.jar
 * commons-codec-1.9.jar
* H2 Database Engine dependencies
 * h2-1.4.186.jar
* SLF4J dependencies
 * slf4j-api-1.7.5.jar
 * Corresponding Binding for used logging framework (See **Binding with a logging framework at deployment time** at [http://www.slf4j.org/manual.html](http://www.slf4j.org/manual.html))

### Logging Configuration Parameters ###

After selecting the logging api, it's necessary to add a java option according to the used logging framework:

**tinylog java option:** -Dtinylog.configuration=C:\tmp\tinylog.properties

**Tinylog's properties file example:**

```
tinylog.writer = rollingfile
tinylog.writer.filename = C:/tmp/log/bpulse-java-client-tinylog.log
tinylog.writer.backups = 10
tinylog.writer.label = timestamp
tinylog.writer.policies = startup, size: 10KB
```

**log4j java option:** -Dlog4j.configuration=file:"C:\tmp\log4j.properties"

**log4j's properties file example:**

```
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

```
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

### HOW TO USE ###

Bpulse java client uses BPulseJavaClient class for pulses sending to BPULSE. It implements two methods: getInstance() and sendPulse(PulsesRQ) for create/load pulses repository and assign the number of threads for insert pulses into the repository and the sending of them via BPULSE COLLECTOR REST SERVICE.

```
#!java

//get the BPulseJavaClient instance. It manages the pulses repository and begins the pulses notification timer.
BPulseJavaClient client = BPulseJavaClient.getInstance();
.
.
.
/*Definition of pulse to send*/
PulsesRQ pulseToSend = ...;
.
.
.
//invoke the operation for inserting the pulse into pulses repository.
client.sendPulse(this.pulseToSend);
.
.
.

```


### Available Configuration Parameters ###

BPulse java client has a configuration file to define the main parameters for sending and processing pulses (pulses repository path, number of threads for notifying pulses via BPULSE COLLECTOR REST SERVICE, etc.). It's definition is expected through java options property **bpulse.client.config** (e.g **-Dbpulse.client.config=C:\tmp\config.properties**).

All properties are defined below:

|Variable name|Description
|          --:|--
|bpulse.client.initNumThreadsSendPulses|Number of threads for supporting the sendPulses from target client application to bpulse.java.client (default value = 5).
|bpulse.client.initNumThreadsRestInvoker|Number of threads for supporting the bpulse.java.client pulses notification through BPULSE COLLECTOR REST SERVICE (default value = 5).
|bpulse.client.periodInMinutesNextExecTimer|Delay time in minutes between timer executions for pulses notification (default value = 1).
|bpulse.client.maxNumberPulsesReadFromTimer|Max number of read pulses for each timer execution from pulsesRepositoryDB for sending to BPULSE COLLECTOR REST SERVICE (default value = 180000).
|bpulse.client.bpulseUsername|Client's Username for sending pulses to BPULSE COLLECTOR SERVICE.
|bpulse.client.bpulsePassword|Client's Password  for sending pulses to BPULSE COLLECTOR SERVICE.
|bpulse.client.bpulseRestURL| BPULSE COLLECTOR REST SERVICE URL.
|bpulse.client.pulsesRepositoryDBPath|System Path to create the Pulses Repository (e.g C:/tmp/pulses_repository). 
|bpulse.client.pulsesRepositoryDBMaxSizeBytes|Pulses Repositories' Allowed max size in bytes (default value = 1073741824).

An example of configuration file is shown:


```
#!properties

#BPULSE JAVA CLIENT CONFIGURATION PROPERTIES
bpulse.client.initNumThreadsSendPulses=100
bpulse.client.initNumThreadsRestInvoker=60
bpulse.client.periodInMinutesNextExecTimer=1
bpulse.client.maxNumberPulsesReadFromTimer=240000
bpulse.client.bpulseUsername=test_collector@enterprise01.com
bpulse.client.bpulsePassword=ABclienteuno123
bpulse.client.bpulseRestURL=http://192.168.0.130:8080/app.collector/collector/pulses
bpulse.client.pulsesRepositoryDBPath=C:/tmp/pulses_repository
bpulse.client.pulsesRepositoryDBMaxSizeBytes=10737418240
```