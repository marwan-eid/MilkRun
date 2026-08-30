@REM Maven Wrapper startup batch script
@REM
@REM Required ENV vars:
@REM JAVA_HOME - location of a JDK home dir

@echo off
set MAVEN_PROJECTBASEDIR=%~dp0
set WRAPPER_JAR="%MAVEN_PROJECTBASEDIR%.mvn\wrapper\maven-wrapper.jar"
set WRAPPER_URL="https://repo.maven.apache.org/maven2/org/apache/maven/wrapper/maven-wrapper/3.3.2/maven-wrapper-3.3.2.jar"
set MAVEN_URL="https://repo.maven.apache.org/maven2/org/apache/maven/apache-maven/3.9.8/apache-maven-3.9.8-bin.zip"

if not exist "%MAVEN_PROJECTBASEDIR%.mvn\wrapper\maven-wrapper.jar" (
    echo Downloading Maven Wrapper...
    powershell -Command "[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12; Invoke-WebRequest -Uri %WRAPPER_URL% -OutFile %WRAPPER_JAR%"
)

if not defined JAVA_HOME (
    set JAVA_HOME=C:\Program Files\Amazon Corretto\jdk17.0.12_7
)

"%JAVA_HOME%\bin\java.exe" ^
    -jar %WRAPPER_JAR% %*
