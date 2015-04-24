# README #

BPulse Java Client is a conector between any client subscribed to BPULSE Service and the REST PULSES COLLECTOR SERVICE.
This README explains how to integrate the conector with the target client application, configuration parameters and how to use it.

### REQUIREMENTS ###

BPulse Java Client is a maven project. It requires the following dependencies for being used in any java maven project:

* bpulse-java-client


```
#!xml
<dependency>
	<groupId>me.bpulse</groupId>
	<artifactId>bpulse-java-client</artifactId>
	<version>1.0.0-SNAPSHOT</version>
</dependency>

```

* SLF4J Logging Framework Bindings

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