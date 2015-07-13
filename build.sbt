organization := "com.milessabin"

name := "dataframe"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  "com.chuusai" %% "shapeless" % "2.2.4"
)

initialCommands in console := """import shapeless._"""

scalacOptions := Seq(
  "-feature",
  "-language:higherKinds",
  "-language:implicitConversions",
  "-unchecked"
)
