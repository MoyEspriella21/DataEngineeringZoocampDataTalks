# Question 1: Bruin Pipeline Structure

Question: In a Bruin project, what are the required files/directories?

Answer: **`.bruin.yml` and `pipeline/` with `pipeline.yml` and `assets/`**

Explanation:

A Bruin project strictly enforces a hierarchical directory structure to manage configurations and orchestrate data workflows properly:

1. Root Directory: Requires a `.bruin.yml` file to store global project configurations, environments, and database connections.

2. Pipeline Directory: Workflows are grouped into specific folders (e.g., `pipeline/`).

3. Pipeline Configuration: Inside the pipeline folder, a `pipeline.yml` file is mandatory to define the workflow's schedule and default connections.

4. Assets Directory: Also inside the pipeline folder, an `assets/` subdirectory is required to store the actual executable tasks (SQL, Python, or YAML files).

# Question 2: Materialization Strategies

Question: You're building a pipeline that processes NYC taxi data organized by month based on `pickup_datetime`. Which incremental strategy is best for processing a specific interval period by deleting and inserting data for that time period?

Answer: **`time_interval` - incremental based on a time column**

Explanation:

The pipeline requires an idempotent incremental load. The `time_interval` strategy in Bruin handles this exact scenario by executing a transaction that first performs a `DELETE` operation bounded by the start and end dates on the incremental key (`pickup_datetime`), and subsequently performs an `INSERT` of the new data for that specific time block. This prevents data duplication while avoiding a full table rebuild.

# Question 3: Pipeline Variables

Question: You have the following variable defined in `pipeline.yml` (an array `taxi_types` defaulting to `["yellow", "green"]`). How do you override this when running the pipeline to only process yellow taxis?

Answer: **`bruin run --var 'taxi_types=["yellow"]'`**

Explanation:

To override pipeline variables via the CLI, Bruin uses the `--var` flag. Since the `taxi_types` variable is strictly typed as an `array` in the YAML configuration, the override value passed through the command line must be formatted as a valid array construct. Using `'taxi_types=["yellow"]'` correctly maps the key to an array containing a single string element, preventing type mismatch errors during execution.

# Question 4: Running with Dependencies

Question: You've modified the `ingestion/trips.py` asset and want to run it plus all downstream assets. Which command should you use?

Answer: **`bruin run ingestion/trips.py --downstream`**

Explanation:

The Bruin CLI triggers individual assets by targeting their specific file path rather than using dot notation or dbt-style selectors. To execute a modified asset and instruct the orchestrator to automatically resolve and trigger all subsequent dependent tasks in the lineage graph, Bruin natively utilizes the `--downstream` execution flag.

# Question 5: Quality Checks

Question: You want to ensure the `pickup_datetime` column in your trips table never has NULL values. Which quality check should you add to your asset definition?

Answer: **`name: not_null`**

Explanation:

In Bruin's asset configuration schema, built-in column-level quality checks are implemented by assigning the exact test identifier to the `name` key. The `not_null` test is a native Bruin primitive designed specifically to enforce data integrity by querying the target column and failing the pipeline step if any NULL values are detected.

# Question 6: Lineage and Dependencies

Question: After building your pipeline, you want to visualize the dependency graph between assets. Which Bruin command should you use?

Answer: **`bruin lineage`**

Explanation:

The `bruin lineage` command is responsible for parsing the pipeline and evaluating the `depends` keys explicitly declared in the configuration headers of the individual assets.  By doing this, it calculates and outputs the dependency graph, mapping out all upstream requirements and downstream dependencies to ensure structural integrity prior to execution.

# Question 7: First-Time Run

Question: You're running a Bruin pipeline for the first time on a new DuckDB database. What flag should you use to ensure tables are created from scratch?

Answer: **`--full-refresh`**

Explanation:

To initialize or completely rebuild database tables, the `--full-refresh` execution flag must be appended to the run command. When this flag is detected, Bruin's execution engine dynamically overrides any predefined incremental materialization strategies (such as `time_interval` or `append`). It instead compiles operations that forcefully drop the existing target tables and recreate them entirely from scratch.
