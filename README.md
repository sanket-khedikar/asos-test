# asos-test-results

As I developed this assignment on community edition, there is no option to configure Git with Databricks.

So, adding below steps to run these notebooks on your databricks account

### 1.Make available Movielens data on databricks.

Upload the ratings.csv, movies.csv and tags.csv DBFS location under Filestore.

```
/FileStore/tables/ratings.csv
/FileStore/tables/movies.csv
/FileStore/tables/tags.csv
```

### 2.Adding notebooks to databricks workspace

Add all 3 notebooks in your databricks workspace

### 3. Pipeline creation
We can create a pipeline/job in databricks but with premium account. I have community edition, so I created 3 seperate 
notebooks with the idea of:  
- Initially, two staging notebook will run in parallel
- transformation notebook is depends on both staging notebook, so it will run after staging notebook run completion.

