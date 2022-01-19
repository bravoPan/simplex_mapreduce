# simplex_mapreduce
A golang implemented map reduce

1. distributed map-reduce: Map, Reduce, Merge
 - Design A worker type carry different job types used for executing differnt jobs.
 -
2. master-worker model: Concurrent Control
3. Worker Failer: Can handle and reassign
4. unit test: test on 2 workers

### Map-Reduce: Split, Map, Reduce, Merge

- First, Split the file into nMap(100) pieces, so that we have `1, 2, 3, ... 100` pieces files.
- Second, Count the freuency(1) for every file and save each piece into nReduce(10) files
```
1-1, 1-2, ...1-10
2-1, 2-2, ...2-10
100-1........100-10
```
- Then Reduce (count the frequency for the same key) every column and Sort into one file, so we have 10 files finally.
- Finally, we Merge all Reduced files into one file from sorting nReduced  files.


### Distributed Model

Implemented by Golang RPC. 
