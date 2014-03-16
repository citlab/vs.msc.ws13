name := "cit-storm"

version := "1.0-SNAPSHOT"

libraryDependencies ++= Seq(
  javaJdbc,
  javaEbean,
  cache,
  "com.google.code.gson" % "gson" % "2.2.4",
  "com.github.kevinsawicki" % "http-request" % "5.6",
  "mysql" % "mysql-connector-java" % "5.1.18"
)     

play.Project.playJavaSettings
