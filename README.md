# Build a java app with Maven

## Create the directory structure

    └── src
        └── main
            └── java
                └── hello
                    └── HelloWorld.java

## Define a simple Maven build
Maven projects are defined with an XML file named pom.xml. Among other things, this file gives the project’s name, version, and dependencies that it has on external libraries.

Create a file named ```pom.xml``` at the root of the project

## Build Java code
To try out the build, issue the following at the command line:

```
$mvn compile
$mvn package
$mvn install    ## update pom.xml and install the new dependencies
```

When it's finished, you should find the compiled ```.class``` files in the target/classes directory.

To execute the JAR file run:

```
$java -jar target/simple-java-project-0.0.1.jar
```

## Properties file

```
InputStream input = this.class.getClassLoader().getResourceAsStream("kafka.properties")
```

## Setup OpenCV
1. Download [OpenCV 3.2](https://opencv.org/releases/page/6/)
2. For Windows, choose the ```.exe``` and run installation.
3. Next steps, follow this [video](https://www.youtube.com/watch?v=TsUhEuySano)