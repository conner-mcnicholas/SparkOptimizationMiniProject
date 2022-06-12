# SparkOptimizationMiniProject
![alt text](https://raw.githubusercontent.com/conner-mcnicholas/SparkOptimizationMiniProject/main/images/compare.png)
I used several different techniques in an attempt to optimize the script, including:  
&emsp;&emsp;&emsp;-Increasing/decreasing the number of partitions in different dataframes using coalesce() and repartition()
<br>
&emsp;&emsp;&emsp;-Changing the memory footprint of different dataframes using cache()
<br>
&emsp;&emsp;&emsp;-Changing the join order of resultDF
<br>
&emsp;&emsp;&emsp;-Enabling/Disabling Adaptive Query Execution by toggling spark.sql.adaptive.enabled
<br>
&emsp;&emsp;&emsp;-Increasing/decreasing the number of cores/executors by changing params x,y,z in
<br>
&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;spark-submit -master local[x] --num-cores=y --num-executors=z
<br>
The code from these scripts can be found in the test directory.
<br> 
## Results
<br>
Because I am running spark locally using --deploy-mode="client" instead of "cluster", which is the
<br>
application wherein spark thrives given its distributed architecture.  In light of this
<br>
the implications of results from testing with driver and executors locally constrained
<br>
are likely not relevant in the context of distributed computing generally.  That being said, with my
<br>
8 core machine, I found the greatest improvements in efficiency (primarily speed), by setting the
<br>
number of partitions on both joined dataframes to 4.
<br>
<br>
I profiled different variations of configs to narrow down the most optimal settings for my local deployment
![alt text](https://raw.githubusercontent.com/conner-mcnicholas/SparkOptimizationMiniProject/main/images/compare.png)
Before Optimization:
<br>
![alt text](https://raw.githubusercontent.com/conner-mcnicholas/SparkOptimizationMiniProject/main/images/spark_ui_before_summary.png)
<br>
After Optimization:
<br>
![alt text](https://raw.githubusercontent.com/conner-mcnicholas/SparkOptimizationMiniProject/main/images/spark_ui_after_summary.png)
<br>
Physical Plans, Pre/Post Optimization:
<br>
![alt text](https://raw.githubusercontent.com/conner-mcnicholas/SparkOptimizationMiniProject/main/images/physicalplans.png)




