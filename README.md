# RepCRec
A distributed database implementation with multiversion concurrency control, deadlock detection, replication, and failure recovery.

author: Yuhan Zhou, Lillian Huang

To build the project
```
mvn clean install
```

### Standard In

To execute the jar with std as input

```
java -jar RepCRec.jar
```

To execute the jar with std as input, in verbose mode
```
java -jar RepCRec.jar -v
```

While reading from standard in,  at the end please hit enter to end again to get the results. 

We save our outputs in a buffer and then print out the buffer at the end. 

###  File Input

To execute the jar with file as input

```
java -jar RepCRec.jar fileName
```

To execute the jar with file as input, in verbose mode
```
java -jar RepCRec.jar fileName -v
```

### Design
Main Components:
![Alt text](images/image1.png?raw=true "Title")
Here is how the Transaction Manager interact with Sites when it handles requests and events
![Alt text](images/image2.png?raw=true "Title")
![Alt text](images/image3.png?raw=true "Title")
![Alt text](images/image4.png?raw=true "Title")
![Alt text](images/image5.png?raw=true "Title")

Other logics we want to mention:

1. Deadlock detection happens at the beginning of every tick. The youngest transaction in the cycle will be aborted. Every time an attempt to acquire lock failed or blocked by transactions in pending list, edges will be added to waitsForGraph. 

2. retry() will go through the pending list and see if there is any operation could be unblocked. retry will be called when there is site recovery or transaction commit/abort.