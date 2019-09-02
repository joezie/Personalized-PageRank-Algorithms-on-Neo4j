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

## Operation
Run against the GOT dataset by default, this code would test the performance of the following algorithms on Single-Source PPR, Top-k PPR, and All-Pair PPR computing:
* Monte-Carlo
* Forward Push
* Neo4j Naive Method
* FORA
* All-Pair-Backward-Search
And we provide parameters as follows.
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