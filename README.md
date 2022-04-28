# Databricks Spark 3.0 ceritification
Prep for the databricks spark 3 certification using Learning Spark v2 book resource
Sample datasets have been taken from the databricks community edition storage.

The repo covers the practical aspects of dataframe api including the following:

* Subsetting DataFrames (select, filter, etc.)
* Column manipulation (casting, creating columns, manipulating existing columns, complex column types)
* String manipulation (Splitting strings, regex)
* Reading/writing DataFrames (schemas, formats- parquet, avro, json etc)
* Rows, Columns and Expressions
* Common Dataframe operations (filter, select, where, distinct, sort, limit)
* Wide and Narrow Transformations
* Working with dates (extraction, formatting, etc)
* Aggregations (groupBy, orderBy, count)
* Statistical methods (avg, sum, max, min, describe, correlation, sampleBy)
* UDFs 
* Combining datasets (joins, unions, broadcasting)
* Optimising and tuning (caching and persistence, repartitioning, shuffle, catalyst optimiser - logical, optimised plans)

In addition, there is also an example of mlflow pipeline although not part of the certification


### Install env dependencies

Will use mamba to install the dev environment with dependencies. First install mamba 
`conda install mamba -n base -c conda-forge`. 
Then create new env with dependencies from  env.yaml file using the command below:

https://mamba.readthedocs.io/en/latest/installation.html
https://mamba.readthedocs.io/en/latest/user_guide/mamba.html

```
mamba env create  --file env.yaml
```

or if you already have env created by conda or venv then you can update this 

```
$mamba env update  --file env.yaml

...
...
...
...
...
..
libevent-2.1.10      | 1.1 MB    | ########################################################################## | 100% 
arrow-cpp-4.0.1      | 16.8 MB   | ########################################################################## | 100% 
tomli-2.0.1          | 16 KB     | ########################################################################## | 100% 
entrypoints-0.4      | 9 KB      | ########################################################################## | 100% 
typed-ast-1.5.3      | 197 KB    | ########################################################################## | 100% 
libbrotlicommon-1.0. | 63 KB     | ########################################################################## | 100% 
protobuf-3.19.4      | 298 KB    | ########################################################################## | 100% 
Preparing transaction: done
Verifying transaction: done
Executing transaction: done
#
# To activate this environment, use
#
#     $ conda activate pyspark_env
#
# To deactivate an active environment, use
#
#     $ conda deactivate

```


To check the dependencies for a given installed package

```
$ mamba repoquery depends databricks-cli 

                  __    __    __    __
                 /  \  /  \  /  \  /  \
                /    \/    \/    \/    \
███████████████/  /██/  /██/  /██/  /████████████████████████
              /  / \   / \   / \   / \  \____
             /  /   \_/   \_/   \_/   \    o \__,
            / _/                       \_____/  `
            |/
        ███╗   ███╗ █████╗ ███╗   ███╗██████╗  █████╗
        ████╗ ████║██╔══██╗████╗ ████║██╔══██╗██╔══██╗
        ██╔████╔██║███████║██╔████╔██║██████╔╝███████║
        ██║╚██╔╝██║██╔══██║██║╚██╔╝██║██╔══██╗██╔══██║
        ██║ ╚═╝ ██║██║  ██║██║ ╚═╝ ██║██████╔╝██║  ██║
        ╚═╝     ╚═╝╚═╝  ╚═╝╚═╝     ╚═╝╚═════╝ ╚═╝  ╚═╝

        mamba (0.15.3) supported by @QuantStack

        GitHub:  https://github.com/mamba-org/mamba
        Twitter: https://twitter.com/QuantStack

█████████████████████████████████████████████████████████████


Executing the query databricks-cli



 Name           Version Build              Channel
───────────────────────────────────────────────────
 databricks-cli 0.12.1  pyhd8ed1ab_0              
 tenacity       8.0.1   pyhd8ed1ab_0              
 python         3.9.12  h8b4d769_1_cpython        
 six            1.16.0  pyhd3eb1b0_1              
 click          8.1.2   py39h6e9494a_0            
 tabulate       0.8.9   pyhd8ed1ab_0              
 requests       2.27.1  pyhd8ed1ab_0              
 configparser   5.2.0   pyhd8ed1ab_0              

```

or which package depends on other packages e.g. running this query shows mlflow installed 
in this env depends on databricks-cli

```
$ mamba repoquery whoneeds databricks-cli


                  __    __    __    __
                 /  \  /  \  /  \  /  \
                /    \/    \/    \/    \
███████████████/  /██/  /██/  /██/  /████████████████████████
              /  / \   / \   / \   / \  \____
             /  /   \_/   \_/   \_/   \    o \__,
            / _/                       \_____/  `
            |/
        ███╗   ███╗ █████╗ ███╗   ███╗██████╗  █████╗
        ████╗ ████║██╔══██╗████╗ ████║██╔══██╗██╔══██╗
        ██╔████╔██║███████║██╔████╔██║██████╔╝███████║
        ██║╚██╔╝██║██╔══██║██║╚██╔╝██║██╔══██╗██╔══██║
        ██║ ╚═╝ ██║██║  ██║██║ ╚═╝ ██║██████╔╝██║  ██║
        ╚═╝     ╚═╝╚═╝  ╚═╝╚═╝     ╚═╝╚═════╝ ╚═╝  ╚═╝

        mamba (0.15.3) supported by @QuantStack

        GitHub:  https://github.com/mamba-org/mamba
        Twitter: https://twitter.com/QuantStack

█████████████████████████████████████████████████████████████


Executing the query databricks-cli



 Name   Version Build          Depends                Channel
──────────────────────────────────────────────────────────────
 mlflow 1.22.0  py39h8ac9d56_1 databricks-cli >=0.8.7  
```