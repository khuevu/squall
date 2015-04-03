#!/bin/bash

# workaround fora github bug (see https://www.drupal.org/node/2304983)
mkdir -p $HOME/.lein/self-installs
cp ../contrib/leiningen-1.7.1-standalone.jar $HOME/.lein/self-installs/
./lein plugin install lein-localrepo "0.3"
./lein localrepo install ../contrib/trove-3.0.2.jar trove 3.0.2
./lein localrepo install ../contrib/bheaven-0.0.3.jar bheaven 0.0.3
./lein localrepo install ../contrib/je-5.0.84.jar bdb-je 5.0.84
./lein localrepo install ../contrib/ujmp-complete-0.2.5.jar ujmp 0.2.5
./lein localrepo install ../contrib/opencsv-2.3.jar opencsv 2.3
# dbtoaster
./lein localrepo install ../contrib/dbtoaster/akka-actor_2.10-2.2.3.jar akka 2.2.3 
./lein localrepo install ../contrib/dbtoaster/cal10n-api-0.7.4.jar cal10 0.7.4 
./lein localrepo install ../contrib/dbtoaster/config-1.0.2.jar config 1.0.2 
./lein localrepo install ../contrib/dbtoaster/lms_2.10-0.3-SNAPSHOT.jar lms 0.3
./lein localrepo install ../contrib/dbtoaster/scala-compiler-2.10.2-RC1.jar scala-compiler 2.10.2-RC1
./lein localrepo install ../contrib/dbtoaster/scala-library-2.10.2-RC2.jar scala-library 2.10.2-RC2
./lein localrepo install ../contrib/dbtoaster/scala-reflect-2.10.2-RC1.jar scala-reflect 2.10.2-RC1
./lein localrepo install ../contrib/dbtoaster/scalariform_2.10-0.1.4.jar scalariform 0.1.4
./lein localrepo install ../contrib/dbtoaster/uncommons-maths-1.2.2a.jar uncommons-maths 1.2.2a

#tmp
./lein localrepo install /Users/khuevu/Projects/DDBToaster/tmp/query.jar dbtoasterquery 0.0.1
./lein localrepo install /Users/khuevu/Projects/DDBToaster/tmp/dbtoaster.jar dbtoaster 0.0.1

#copy appropriate version of storm.yaml on local machine
#mkdir -p ~/.storm
#cp ../resources/storm.yaml ~/.storm/storm.yaml
#./adjust_storm_yaml_locally.sh

#mkdir -p /tmp/ramdisk
