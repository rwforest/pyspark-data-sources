# Scala vs Python Data Source API - Why Scala Works and Python Doesn't

## Analysis of the Original Scala Implementation

After analyzing the original Scala implementation in Data-Science-Extensions/spark-datasource-rest, I found **why it works in Scala but not in Python**.

## Key Finding: Different API Architectures

### Scala Data Source V2 API

**The Scala version receives `SQLContext` as a parameter!**

```scala
// RestDataSource.scala (lines 34-42)
override def createRelation(
    sqlContext: SQLContext,  // ✅ Spark PROVIDES this!
    parameters: Map[String, String]): BaseRelation = {

  val restOptions = new RESTOptions(parameters)

  // Pass sparkSession to the Relation
  RESTRelation(restOptions)(sqlContext.sparkSession)
}
```

**The Relation class receives and stores the SparkSession:**

```scala
// RestRelation.scala (line 39-40)
case class RESTRelation(
    restOptions: RESTOptions)(@transient val sparkSession: SparkSession)
  extends BaseRelation with TableScan {
```

**Then it can access tables directly:**

```scala
// RestRelation.scala (lines 67-69)
private val inputDf = if (restOptions.inputType == "tableName") {
      sparkSession.sql(s"select * from $inputs")  // ✅ WORKS!
}
```

### Python Data Source API

**The Python version does NOT receive SQLContext/SparkSession:**

```python
# Python API signature
def __init__(self, options: Optional[Dict[str, str]] = None):
    self.options = options or {}
    # ❌ No session parameter!

def reader(self, schema: StructType) -> DataSourceReader:
    return RestReader(self.options, schema)
    # ❌ No session available to pass!
```

**Attempts to get session fail:**

```python
def partitions(self):
    spark = SparkSession.getActiveSession()  # ❌ Returns None!
```

## Architecture Comparison

| Aspect | Scala Data Source V2 | Python Data Source API |
|--------|---------------------|----------------------|
| **API Type** | RelationProvider | DataSource |
| **createRelation signature** | Receives `SQLContext` | No equivalent method |
| **Session Access** | ✅ Passed as parameter | ❌ Not provided |
| **When Created** | During `.load()` call | During `.load()` call |
| **Context** | Driver with full session | Restricted context |
| **Can read tables?** | ✅ YES | ❌ NO |

## The Fundamental Difference

### Scala: Session is Injected

```scala
// Spark's internal code (conceptual)
def load(format: String, options: Map[String, String]): DataFrame = {
  val dataSource = Class.forName(format).newInstance()

  // Spark PASSES the session!
  val relation = dataSource.createRelation(
    this.sqlContext,  // ✅ Injection!
    options
  )

  Dataset.ofRows(sparkSession, relation)
}
```

### Python: No Injection Mechanism

```python
# Spark's internal code (conceptual)
def load(format: str, options: Dict[str, str]) -> DataFrame:
    data_source = Class.forName(format).newInstance()

    # NO session passed!
    reader = data_source.reader(schema)  # ❌ No injection

    # Reader runs in isolated context
    partitions = reader.partitions()  # ❌ No session available
```

## Why This Matters

The Scala version can implement this workflow:

```scala
// ✅ Scala Workflow (Works!)
1. User calls: spark.read.format("rest").options(...).load()
2. Spark creates: RestDataSource instance
3. Spark calls: createRelation(sqlContext, options)
4. DataSource creates: RESTRelation(sparkSession)
5. Relation reads: sparkSession.sql("SELECT * FROM table")
6. Returns: DataFrame with results
```

The Python version **cannot** implement the same workflow:

```python
# ❌ Python Workflow (Fails!)
1. User calls: spark.read.format("rest").options(...).load()
2. Spark creates: RestDataSource instance
3. Spark calls: reader(schema)  # No session!
4. Reader tries: SparkSession.getActiveSession()  # Returns None!
5. ERROR: Cannot read table!
```

## What the Scala Code Does

Looking at RestRelation.scala (lines 64-84):

```scala
// 1. Reads input table (line 67-68)
private val inputDf = sparkSession.sql(s"select * from $inputs")

// 2. Gets column names (line 76)
private val columnNames : Array[String] = inputDf.columns

// 3. Creates RDD with API calls (line 78-83)
private val restRdd : RDD[String] = {
  val parts = restOptions.inputPartitions.toInt
  inputDf.rdd.repartition(parts).map(r => callRest(r))
}

// 4. Infers schema (line 86-104)
override val schema: StructType = {
  // Sample input, call API, infer schema from JSON
  val sampleDf = inputDf.sample(false, samplePcnt)
  val outRdd = sampleDf.rdd.map(r => callRest(r))
  sparkSession.read.json(outRdd).schema
}

// 5. Builds final result (line 49-53)
override def buildScan() : RDD[Row] = {
  sparkSession.read.schema(schema).json(restRdd).rdd
}
```

**This entire workflow depends on having `sparkSession` available!**

## Why Python Can't Replicate This

The Python Data Source API was designed differently:

### Design Philosophy

**Scala API (RelationProvider)**:
- Based on Spark 2.x architecture
- Tight integration with Catalyst optimizer
- Full access to SparkSession
- Designed for complex data source operations

**Python API (DataSource)**:
- New PySpark 4.0+ API
- Designed for security and isolation
- Limited context access
- Intended for self-contained data sources

### Use Cases

**Scala API - Designed For**:
- ✅ Reading from catalogs/tables
- ✅ Complex schema inference
- ✅ Cross-referencing data sources
- ✅ Integration with existing tables

**Python API - Designed For**:
- ✅ Reading external files
- ✅ API calls with fixed parameters
- ✅ Self-contained data generation
- ❌ NOT for reading Spark tables

## The Solution: Work With Python's Design

Since we can't change the Python API architecture, we must adapt:

### What We CAN'T Do
```python
# Can't replicate Scala's approach
class RestDataSource(DataSource):
    def reader(self, schema, session):  # ❌ Can't add session parameter
        ...
```

### What We CAN Do
```python
# Work outside the data source
def rest_api_call(input_df, url, method, **options):
    # 1. Read table OUTSIDE data source (where session IS available)
    input_rows = input_df.collect()

    # 2. Pass data TO data source (not table name)
    serialized = json.dumps([row.asDict() for row in input_rows])

    # 3. Data source uses pre-loaded data
    api_results = spark.read.format("rest") \
        .option("inputData", serialized) \
        .load()

    # 4. Combine results OUTSIDE data source
    ...
```

## Conclusion

### Why Scala Works
✅ Scala's `RelationProvider` API receives `SQLContext` as a parameter
✅ Can access `sparkSession.sql()` directly
✅ Can read from tables, catalog, and other DataFrames
✅ Full integration with Spark's execution model

### Why Python Doesn't Work
❌ Python's `DataSource` API doesn't receive session
❌ `getActiveSession()` returns `None` in data source methods
❌ Cannot access tables or catalog
❌ API designed for different use cases

### The Right Approach for Python
✅ Use `rest_api_call()` helper function
✅ Handle DataFrame operations outside the data source
✅ Pass pre-serialized data to the data source
✅ Work **with** the API's design, not against it

## Final Recommendation

**Don't try to replicate the Scala implementation in Python!**

The Scala version works because it uses a completely different API (RelationProvider) that provides features the Python API (DataSource) intentionally doesn't have.

The `rest_api_call()` helper function is the **correct Python implementation** of the same functionality, adapted to work within Python's API constraints.

---

**References:**
- Scala implementation: `/Users/yipman/Downloads/github/Data-Science-Extensions/spark-datasource-rest/src/main/scala/org/apache/dsext/spark/datasource/rest/RestRelation.scala`
- Scala API: `RelationProvider` with `createRelation(sqlContext, parameters)`
- Python API: `DataSource` with `reader(schema)` (no session parameter)
