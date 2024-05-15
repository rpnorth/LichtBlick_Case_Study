
# LichtBlick Analytics Engineer Case Study
#### Submitted by Ryan North

## Summary of completed work
The work consisted of loading the data, exploring the data, cleaning the data, creating the new table and summarizing revenue changes over a time period.

## Setup
To run the scripts use the following setup: 
```
make setup
#or
pyenv local 3.9.8
python -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```
Requirements:
- pyenv with Python: 3.9.8
- data stored in the folder "src_data" (not stored on github)

## Summary of Notebooks and scripts
The Notebooks describe the different steps used to accomplish the task. The most relevant Notebook is [3_Running_Final_Version.ipynb](3_Running_Final_Version.ipynb), which shows how to execute the script to run the ETL and Revenue analysis. The script itself is found in [load_date_create_table.py](load_date_create_table.py).

In addition, the EDA Notebook [1_EDA.ipynb](1_EDA.ipynb) was used to get familiar with the data, including problems that needed to be corrected. 
And the test Notebook [2_Create_new_table.ipynb](2_Create_new_table.ipynb) was where the scripts found in [load_date_create_table.py](load_date_create_table.py) were developed and tested.

## Summary of Assumptions:
- contracts with product 2000 were dropped as pricing/prodcut information was not available. As they made up <8% of the contracts, it was assumed that this was more accurate than trying to estimate the price of this product
- the analysis focused on active contracts at the time the data was downloaded. This made it possible to provide average revenue at the three different download dates. 
- revenue was calculated over three periods, based on when the pricing info started, and when the data was downloaded.
    - thus contract count, revenue, and revenue per contract could be compared at three different time points
    - the values correspond to active customers at the time the data was downloaded