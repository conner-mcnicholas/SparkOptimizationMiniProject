# SparkOptimizationMiniProject

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
 
The following plots are not exhaustive of all of the features available, but those not shown aren't as relevant for this dataset:

![alt text](https://github.com/conner-mcnicholas/MortalityOfNations/blob/main/analysis_images/parallel_axes_full_income.png?raw=true)
![alt text](https://github.com/conner-mcnicholas/MortalityOfNations/blob/main/analysis_images/parallel_axis_filter.png?raw=true)
![alt text](https://github.com/conner-mcnicholas/MortalityOfNations/blob/main/analysis_images/violin_dark_income.png?raw=true)
![alt text](https://github.com/conner-mcnicholas/MortalityOfNations/blob/main/analysis_images/boxplot_light_income.png?raw=true)
![alt text](https://github.com/conner-mcnicholas/MortalityOfNations/blob/main/analysis_images/countor_dark_income.png?raw=true)
![alt text](https://github.com/conner-mcnicholas/MortalityOfNations/blob/main/analysis_images/joyplot_light_region.png?raw=true)
![alt text](https://github.com/conner-mcnicholas/MortalityOfNations/blob/main/analysis_images/anotated_bubble_axisdist.png?raw=true)
![alt text](https://github.com/conner-mcnicholas/MortalityOfNations/blob/main/analysis_images/anotated_bubble_dark_region.png?raw=true)
![alt text](https://github.com/conner-mcnicholas/MortalityOfNations/blob/main/analysis_images/anotated_bubble_light_income.png?raw=true)
![alt text](https://github.com/conner-mcnicholas/MortalityOfNations/blob/main/analysis_images/bar_counts_income_regioncolor.png?raw=true)
![alt text](https://github.com/conner-mcnicholas/MortalityOfNations/blob/main/analysis_images/bar_counts_region_incomecolor.png?raw=true)
