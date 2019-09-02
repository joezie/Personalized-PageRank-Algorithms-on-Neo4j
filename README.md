# Personalized-PageRank-Algorithms-on-Neo4j
* Author: Xiaozhou Liang
## Introduction
As part of my senior design, this is the implementation of two advanced Personalized PageRank (PPR) algorithms, FORA and All-Pair-Backward-Search, on Neo4j Graph Database. For more details, please refer to "Dissertation.pdf", which is my graduation dissertation corresponding to this senior design.

## Test Environment
* Windows 8.1
* JDK 1.8.0
* Neo4j 3.5.1
* Eclipse Oxygen.3a Release (4.7.3a)

## Dataset
For demo purpose, I use the "Game of Thrones" dataset. Directory "target/got.db" stores the Neo4j database files. And directory "dataset/got" stores the relationship and node files in csv format.

To be note is that we need extra effort to generate this "got.db" database file based on "GOT_Nodes.csv" and "GOT_Rels.csv".

Firstly, make sure you have installed Neo4j on your host, and command "neo4j-admin" is available. Then the following command would generate a directory with name format "database-********-****-****-****-************" under "\<neo4j-home\>/neo4jDatabases".
```sh
neo4j-admin import --relationships="GOT_Rels.csv" --nodes="GOT_Nodes.csv"
```

Then under this new directory, we could find directory "installation-3.4.1\data\databases\graph.db". We need to copy and rename this directory to "target/got.db".


## Operation
Run against the GOT dataset by default, this code would test the performance of the following algorithms on Single-Source PPR, Top-k PPR, and All-Pair PPR computing:
* Monte-Carlo
* Forward Push
* Neo4j Naive Method
* FORA
* All-Pair-Backward-Search

The performance report would be exported to \<dataset name\>_AlgoPerfResults.txt. Besides, we provide parameters as follows.
```sh
java PPR [options]
```
- options:
	- -alpha \<alpha\>
	- -eps \<epsilon\>
	- -query \<query number\>
	- -k \<k\>
	- -node \<node property\>
	- -label \<label type\>
	- -rel \<relationship type\>
	- -db \<database directory\>
	- -help
- example:
```sh
java PPR -alpha 0.15 -eps 0.5 -query 50 -k 10 -node name -label Person -rel Relation -db target/got.db
```