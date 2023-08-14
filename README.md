
# **Extract Transform Load (ETL) Engineer**

In this final part you will:

- Run the ETL process
- Extract bank and market cap data from the JSON file `bank_market_cap.json`
- Transform the market cap currency using the exchange rate data
- Load the transformed data into a seperate CSV

For this lab, we are going to be using Python and several Python libraries. Some of these libraries might be installed in your lab environment or in SN Labs. Others may need to be installed by you. The cells below will install these libraries when executed.

```bash
#!mamba install pandas==1.3.3 -y
#!mamba install requests==2.26.0 -y
```
## Imports

Import any additional libraries you may need here.

```bash
import glob
import pandas as pd
from datetime import datetime
```

**glob**

In Python, the glob function is used to match files and directories in a specific folder using a specified pattern. This function is useful for filtering or listing specific file types or name patterns when performing file and directory operations.

The underlying logic of the glob function is to match files and directories in a folder by using a specific pattern and returning the matched items as a list.


 **pandas**

Pandas is a powerful library used for data manipulation and analysis in the Python programming language. It provides high-level structured data structures and data analysis tools that simplify data processing workflows. Essentially, pandas is used by data analysts and data scientists to load, process, clean, transform, and analyze data.

Pandas can be used to load data files (such as CSV, Excel, SQL, etc.), perform operations to fill or drop missing data in datasets, sort and filter data, group and aggregate data, merge and transform data, and visualize data using charts and graphs.

As the exchange rate fluctuates, we will download the same dataset to make marking simpler. This will be in the same format as the dataset you used in the last section  

## Load Files
As the exchange rate fluctuates, we will download the same dataset to make marking simpler. This will be in the same format as the dataset you used in the last section  

```bash
!wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Lab%20-%20Extract%20Transform%20Load/data/bank_market_cap_1.json
!wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Lab%20-%20Extract%20Transform%20Load/data/bank_market_cap_2.json
!wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Final%20Assignment/exchange_rates.csv
```

# Extract

### JSON Extract Function

This function will extract JSON files.

```bash
def extract_from_json(file_to_process):
    dataframe = pd.read_json(file_to_process)
    return dataframe                                                      
```

## Extract Function

Define the extract function that finds JSON file `bank_market_cap_1.json` and calls the function created above to extract data from them. Store the data in a `pandas` dataframe. Use the following list for the columns.

```bash
columns=['Name','Market Cap (US$ Billion)']                                               
```
```bash
def extract():
    extracted_data=pd.DataFrame(columns=['Name','Market Cap (US$ Billion)'])
    for jsonfile in glob.glob("*.json"):
        extracted_data=extract_from_json(jsonfile)
        
    return extracted_data                                       
```

Load the file exchange_rates.csv as a dataframe and find the exchange rate for British pounds with the symbol GBP, store it in the variable exchange_rate, you will be asked for the number. Hint: set the parameter index_col to 0.
```bash
df=pd.read_csv("exchange_rates.csv")

df.rename( columns={'Unnamed: 0':'Pounds'}, inplace=True )
df1=df.loc[df["Pounds"] == 'GBP']
df1
exchange_rate=float(df1["Rates"].values)
print(exchange_rate)                                                         
```

# Transform 
Using exchange_rate and the exchange_rates.csv file find the exchange rate of USD to GBP. Write a transform function that

  * Changes the Market Cap (US$ Billion) column from USD to GBP
  * Rounds the Market Cap (US$ Billion)` column to 3 decimal places
  * Rename Market Cap (US$ Billion) to Market Cap (GBP$ Billion)

```bash
def transform(data):
    data['Market Cap (US$ Billion)']= round(data['Market Cap (US$ Billion)']*exchange_rate,3)
    #print(data['Market Cap (US$ Billion)'])
    data=data.rename(columns={"Market Cap (US$ Billion)":"Market Cap (GBP$ Billion)"},inplace=False)
    return data                                                        
```

# Loading
Create a function that takes a dataframe and load it to a csv named bank_market_cap_gbp.csv. Make sure to set index to False.
```bash
targetfile="bank_matket_cap_gbp.csv"
def load(targetfile,datatoload):
    datatoload.to_csv(targetfile, index=False)                                         
```

# Logging
Write the logging function log to log your data:

```bash
def log(message):
    timestamp_format='%Y-%h-%d-%H%M%S'
    now=datetime.now()
    timestamp=now.strftime(timestamp_format)
    with open("logfile.txt","a") as f:
        f.write(timestamp+','+message+'\n')
                        
```

# Running the ETL Process
Log the process accordingly using the following "ETL Job Started" and "Extract phase Started"

```bash
log("ETL Job Started")
```

### Extract
Use the function extract, and print the first 5 rows
```bash
# Call the function here
log("Extract Phase Started")
extract_data=extract()
# Print the rows here
extract_data.head(5)
```

Log the data as "Extract phase Ended"

```bash
log("Extract Phase Ended")
```

### Transform
Log the following "Transform phase Started"
```bash
log("Transform Phase Started")
```

Use the function transform and print the first 5 rows of the output
```bash
# Call the function here
transformed_data=transform(extract_data)

# Print the first 5 rows here
transformed_data.head(5)
```

Log your data "Transform phase Ended"
```bash
log("Transform Phase Ended")
```

### Load
Log the following "Load phase Started".
```bash
log("load Phase Started")
```

Call the load function
```bash
load(targetfile,transformed_data)
```

Log the following "Load phase Ended".
```bash
log("load Phase Ended")
```

# Author
  Merve İbiş
