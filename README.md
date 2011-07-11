Hadoop Workshop Exercises
=========================

Contents
--------

This project contains exercises for the Hadoop Workshop started on July, 2011.

### Projects ###

1.  **Word Count**:
    Count the number of words consisting of \w+ characters.

### Building ###
To make executable jar and shells, you can do:  

`mvn package`

To run WordCount job with hadoop command, you can do:

`hadoop jar target/hadoop-workshop-0.0.1.jar com.knownstylenolife.hadoop.workshop.wordcount.WordCountToolMain input output DEBUG`

To run WordCount job with shell, you can do:

`sh target/appassembler/bin/wordcount input output`

To set log level of mapper and reducer tasks DEBUG, add "DEBUG" argument as last argument.

`sh target/appassembler/bin/wordcount input output DEBUG`


### Importing to Eclipse ###



### License ###

Apache Software License 2.0.

### Author ###

* Toshiaki Toyama ([@manboubird](http://twitter.com/manboubird))

    