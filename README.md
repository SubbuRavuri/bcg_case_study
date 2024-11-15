# BCG Crash Analytics Project

## Overview
The BCG Crash Analytics Project leverages Apache Spark to process and analyze large-scale crash data. The project contains various analytics modules that provide insights into crash statistics, driver behaviors, and other related factors. The results are saved as CSV files, allowing easy access and further analysis.

## Key Features:
1.Analyze crash data based on different factors such as gender, vehicle types, injury severity, etc.
2.Perform aggregations and filtering on crash data to derive valuable insights.
3.Generate meaningful reports based on data analysis, saved as CSV files for easy access.

## Project Structure:

![image](https://github.com/user-attachments/assets/79dca2ef-fe1e-45d2-84b7-4d62285d8032)




## Requirements:

The following Python libraries are required for the project:

pyspark


## Data Files

This project requires the following data files (ensure they are located in the data/ folder):

1.Primary_Person_use.csv: Contains primary person information related to crashes.
2.Units_use.csv: Contains data related to the vehicles involved in crashes.
3.Endorse_use.csv: Contains information about driver's endorsements.
4.Units_use.csv: Contains information about vehicle units.
5.Charges_use.csv: Contains information about charges on the User
6.Damages_use.csv: Contains information about damages in accidents.



## Setup Instructions
1. Clone this repository to your local machine:
2. Install the required libraries(pyspark)
3. Place your data files (Primary_Person_use.csv, Units_use.csv, Endorse_use.csv, etc.) in the data/ folder.
4. Run the main.py file to start the analytics(spark-submit main.py)
5. Output files will be generated in the output/ folder for each of the analytics.


## Analytics Modules
Each analytics module provides a different type of analysis based on the crash data. Below is a description of each module:

### Analytics 1: Crashes where males are killed in accidents

Objective: Identify and count crashes where males were killed and there were more than 2 people involved.
Result: The number of such crashes is saved as a CSV file.

### Analytics 2: Top 10 Vehicle Makes by Count

Objective: Identify the top 10 vehicle makes involved in crashes.
Result: A list of the top 10 vehicle makes with the count of occurrences.

### Analytics 3: Vehicle Make with the Most Dangerous Drivers (Age and Gender)

Objective: Identify the vehicle makes associated with the most dangerous drivers, based on their age and gender.
Result: A list of vehicle makes and the corresponding number of crashes involving dangerous drivers.

### Analytics 4: Top 10 States with Highest Crash Incidents

Objective: Identify the top 10 states with the highest number of crashes.
Result: A list of the top 10 states and the number of crashes that occurred in each.

### Analytics 5: Crashes Involving Alcohol-Related Incidents

Objective: Identify crashes where alcohol consumption was involved.
Result: The count of such crashes and detailed information for analysis.

### Analytics 6: Injuries by Severity and Gender

Objective: Identify injury severity by gender across crashes.
Result: A breakdown of injuries by severity and gender.

### Analytics 7: Most Common Crash Types

Objective: Analyze and identify the most common crash types.
Result: A list of the most frequent crash types.

### Analytics 8: Top Vehicle Colors Involved in Crashes

Objective: Identify the most common vehicle colors involved in crashes.
Result: A list of top vehicle colors.

### Analytics 9: Top 5 Vehicle Makes with Speeding Offences

Objective: Identify the top 5 vehicle makes that have been involved in speeding offences.
Result: A list of top 5 vehicle makes associated with speeding offences.

### Analytics 10: Top 5 Vehicle Makes with Speeding Offences, Licensed Drivers, Top Vehicle Colors, and Top States

Objective: Combine multiple factors, including vehicle make, speeding offences, licensed drivers, vehicle colors, and top states, to generate a comprehensive analysis.
Result: A list of top 5 vehicle makes meeting the criteria.

## Output
For each analytics module, an output CSV file will be saved in the output/ folder. The result will include the necessary headers and data.

The CSV files are named based on the analytics module, e.g., analytics_1.csv, analytics_2.csv, etc.

