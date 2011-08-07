Hadoop Workshop Exercises
=========================

Contents
--------

This project contains exercises for the Hadoop Workshop started on July, 2011.

## Projects

### 1. Word Count Simple

Count the number of words consisting of \w+ characters.

See WordCountSimpleMainTool class.

### 2. Word Count with Combiner&Patitiner

Count the number of words consisting of \w+ characters.

Combiner does local aggregation of a map task output.

Partitioner divides into three reduces grouped by

mapper output key string starting with 0-9, A-M or N-Z.

See WordCountWithKeyPrefixPartitionerToolMain class.

### 3. Char Count Simple with Writable&Combiner

Count the number of characters of each lines and each file.
        
**Example:**
    
Input files hello.txt and goodbye.txt includes contents below.
    
**hello.txt Content:**
    
> Hello
>
> World!
    
**goodbye.txt Content:**
    
> Good
>
> Bye!
    
**Output should be:**
    
    {FILENAME}\t{OFFSET}\t{CHARACTER}\t{COUNT}

> Hello.txt	0	H	1
>    
> Hello.txt	0	e	1
> 
> Hello.txt	0	l	2
> 
> ...
> 
> Hello.txt	6	W	1
> 
> Hello.txt	6	o	1
> 
> ...
> 
> goodbye.txt	0	G	1
> 
> goodbye.txt	0	o	2
> 
> ...
> 
> goodbye.txt	5	!	1
 	
Combiner does local aggregation of a map task output.

Writable is used for map output key.

See CharCountSimpleToolMainTest class.
    
### 4. First Char Count

Count the number of words grouped by first character of words.
Both unique count of the word and total count of word are included in output.

See FirstCharCountToolMain class.

**Example:**
    
Input file hello.txt includes content below.
    
**hello.txt Content:**
    
> 0world
> Hello, world
> Hello, Hadoop's
> Hello, MapReduce
   
**Output should be:**
    
{FIRST_CHARACTER_OF_WORD}\t{UNIQUE_COUNT_OF_WORD_APPEAR}\t{TOTAL_COUNT_OF_WORD_APPEAR}

> 0		1	1
>
> h		2	4
>
> m		1	1
>
> s		1	1
>
> w		1	1

## Dependencies

* Cloudera CDH3 version 0.20.2-cdh3u0

Ref. [Cloudera CDH3 Maven Repository](https://ccp.cloudera.com/display/CDHDOC/Using+the+CDH3+Maven+Repository)

## Importing to Eclipse

Run command:

`mvn eclipse:eclipse`

If you have created or checked out the project with eclipse, you only have to refresh the project in your workspace. 

Otherwise you have to import the project into your eclipse workspace (From the menu bar, select File >Import >Existing Projects into Workspace).

Ref. [Maven - Guide to using Eclipse with Maven 2.x](http://maven.apache.org/guides/mini/guide-ide-eclipse.html)

## Building

To make executable jar and shells, you can do:  

`mvn package`

  or

`mvn -P pseudo package`

First one is for standalone mode, second is for pseudo-distributed mode.

## Executing

To execute WordCount job with hadoop command, you can do:

`hadoop jar target/hadoop-workshop-0.0.1.jar com.knownstylenolife.hadoop.workshop.count.tool.WordCountSimpleToolMain input output`

To execute WordCount job with shell, you can do:

`sh target/appassembler/bin/wordcount input output`

This shell script is generated by [Appassembler Maven Plug-In](http://mojo.codehaus.org/appassembler/appassembler-maven-plugin/).

## Executing with DEBUG log level

To set log level of mapper and reducer tasks DEBUG, add "DEBUG" argument as last argument.

`hadoop jar target/hadoop-workshop-0.0.1.jar com.knownstylenolife.hadoop.workshop.count.tool.WordCountSimpleToolMain input output DEBUG`

or

`sh target/appassembler/bin/wordcount input output DEBUG`

## License

Apache Software License 2.0.

## Author

* Toshiaki Toyama ([@manboubird](http://twitter.com/manboubird))

    
