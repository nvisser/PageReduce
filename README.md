# Install
`mvn install`
# Run
`mvn exec:java -Dexec.args="input output"`
## HITS
### ZIP bestand
* [Download als ZIP](https://github.com/nvisser/PageReduce/archive/master.zip)
### Output
#### IncomingNeighbors
```
a	{"name":"a","auth":1.0,"hub":1.0}:{"name":"e","auth":1.0,"hub":1.0}
b	{"name":"b","auth":2.0,"hub":1.0}:{"name":"a","auth":1.0,"hub":1.0},{"name":"c","auth":1.0,"hub":1.0}
c	{"name":"c","auth":2.0,"hub":1.0}:{"name":"d","auth":1.0,"hub":1.0},{"name":"e","auth":1.0,"hub":1.0}
d	{"name":"d","auth":2.0,"hub":1.0}:{"name":"a","auth":1.0,"hub":1.0},{"name":"e","auth":1.0,"hub":1.0}
e	{"name":"e","auth":2.0,"hub":1.0}:{"name":"b","auth":1.0,"hub":1.0},{"name":"d","auth":1.0,"hub":1.0}
```
#### OutgoingNeighbors
```
a	2.000000 a 4.000000
b	1.000000 b 2.000000
c	1.000000 c 2.000000
d	2.000000 d 4.000000
e	3.000000 e 5.000000
```