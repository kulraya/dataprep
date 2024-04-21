df_parameter = spark.createDataFrame([["1"]],["id"])\
    .withColumn("bck1mth",date_format((add_months(current_date(),-1)), "yyyyMM").cast('int'))\
    .withColumn("first_bck1mth",date_format((add_months(current_date(),-1)), "yyyyMMdd").cast('int'))\
        .collect()
bck1mth = [i[1] for i in df_parameter]
bck1mth_date = [i[2] for i in df_parameter]                                       
print(bck1mth, bck1mth_date)
[202401] [20240101]


from datetime import datetime, timedelta
import calendar

# Function to subtract a month from the current date and format it
def subtract_one_month(current_date):
    first_day_this_month = current_date.replace(day=1)
    last_day_last_month = first_day_this_month - timedelta(days=1)
    return last_day_last_month

# Current date
current_date = datetime.now()

# Calculate dates
bck1mth_date = subtract_one_month(current_date)
bck1mth = int(bck1mth_date.strftime('%Y%m'))
first_bck1mth = int(bck1mth_date.strftime('%Y%m%d'))

# Print results
print([bck1mth], [first_bck1mth])


# changing pyspark coding in python coding,
# no require explain
import pandas as pd

# Assuming data is loaded into pandas DataFrame from a relevant source.
# For example, data loading might need to be done from CSV files or any other database.
ot_postpaid = pd.read_csv('ot_postpaid.csv')
customer360 = pd.read_csv('customer360.csv')
dim_consent_data = pd.read_csv('dim_consent.csv')

# Filter, group, pivot, sort and count operations similar to Spark's DataFrame
filtered_ot_postpaid = ot_postpaid[ot_postpaid['cc_postpaid_flg'] == 1]
grouped_ot_postpaid = filtered_ot_postpaid.groupby(['demo_payment_monthly_payment_group_cat', 'demo_affluence_grid_v1_affluence_group_bin'])
pivot_ot_postpaid = grouped_ot_postpaid.size().unstack(fill_value=0)
sorted_ot_postpaid = pivot_ot_postpaid.sort_index(key=lambda x: x.map({'00-19':1, '20-29':2, '30-39':3, '40-49':4, '50+':5}))

# Filter operations for customer360
consent = customer360[(customer360['activated_flag'] == 1) & 
                      (customer360['is_blocklist'] == 0)]

# Filter and select operations for dim_consent
dim_consent = dim_consent_data[(dim_consent_data['company'] == 'tuc') & 
                               (dim_consent_data['active'] == 1) & 
                               (dim_consent_data['consent_status'] == 1) & 
                               (dim_consent_data['purpose_id'] == 'dc41e23a-67f0-4f37-a897-60cdde9f28bf')]
dim_consent = dim_consent[['msisdn']].drop_duplicates()

# Join and aggregation operation
joined_data = pd.merge(dim_consent, consent, on='msisdn', how='inner')
msisdn_count = joined_data['msisdn'].count()

# Output the result
print(sorted_ot_postpaid)
print(msisdn_count)


import pandas as pd

# Define the values as a list of domain strings
values = [
    "baac.or.th", "bangkokbank.com", "cimbthai.com", "cimbclicks.in.th", "citibank.co.th",
    "kasikornbank.com", "kiatnakin.co.th", "krungsri.com", "servicekrungsrigroup.com",
    "krungsrionline.com", "ktb.co.th", "ktbnetbank.com", "lhbank.co.th", "mebytmb.com",
    "scb.co.th", "scbeasy.com", "scbbusinessnet.com", "thanachartbank.co.th", "tmbbank.com",
    "tmbdirect.com", "uob.co.th", "uobgroup.com", "uobthailand.com", "ghbank.co.th",
    "gsb.or.th", "smebank.co.th", "exim.go.th"
]

# Create a DataFrame with these values
df = pd.DataFrame(values, columns=['Domain'])

# Display the DataFrame
print(df)

import pandas as pd
import time

# Assuming the DataFrame is loaded from a CSV or similar source into pandas
df = pd.read_csv('customer360_current.csv')

# Print columns that include the substring 'bulk'
for col in df.columns:
    if 'bulk' in col:
        print(col)

# Timing execution in Python
start_time = time.time()

# Perform operations to be timed here
# Example dummy operation to mimic timing an action
time.sleep(1)  # Simulate a delay

# Print elapsed time
print("--- %s seconds ---" % (time.time() - start_time))

# Demonstrating string formatting with different specifiers
number = 4.3
print('%s %d %f %g' % (number, int(number), number, number))

import pandas as pd
import numpy as np

# Define functions similar to your PySpark functions for use with pandas DataFrame
def percent(df, var):
    tot = len(df)
    count = df[var].value_counts().reset_index()
    count.columns = [var, 'count']
    count['percent'] = (count['count'] / tot) * 100
    print(count)

def count_distinct(df):
    print("Total:", len(df), "Distinct:", df['msisdn'].nunique())

# Read CSV with inferred schema and select column
int_sig = pd.read_csv("correlation_gt_point02.csv", usecols=['value'])
int_sig_list = int_sig['value'].tolist()

# Assuming ORC read equivalent (pandas cannot read ORC natively; you would need PyArrow or similar)
# df = pd.read_orc('train_202110_202203.orc')
# df = df.drop(columns=['msisdn'])

# Example DataFrame selection using a list
# df = df[int_sig_list]

# Simulating correlation computation and drop features
drop_features = set()
for column in df.columns:
    corr = df['label'].corr(df[column])
    if corr >= 0.03:
        drop_features.add(column)

# Convert set to DataFrame
drop_features_df = pd.DataFrame(list(drop_features), columns=['variable'])
drop_features_df.to_csv("correlation_lt_point05.csv", index=False)

# Assuming empty DataFrame creation and manipulation
empty_df = pd.DataFrame(columns=['variable', 'correlation'])

# Assume list of months and assertions as comments since pandas can't do direct assert like Spark
mth_list = [202110, 202111, 202112, 202201, 202202, 202203]
for mth in mth_list:
    print(mth)
    # Example of reading and asserting counts
    # base = pd.read_orc(f'digipulse/final/par_month={mth}.orc')
    # assert len(base) == base['msisdn'].nunique()

import pandas as pd
import pyarrow.orc as orc
import pyarrow as pa

user_path = 'your_user_path_here'
mth_obs_window = 'your_month_obs_window_here'
mth_list = [202110, 202111, 202112, 202201, 202202, 202203]

for i in mth_list:
    if i == 202201:
        print(i)
        df = pd.read_orc(f'{user_path}par_month={mth_obs_window}/temp/lotus_visit/par_month={i}.orc')
    else:
        print(i)
        temp = pd.read_orc(f'{user_path}par_month={mth_obs_window}/temp/lotus_visit/par_month={i}.orc')
        df = df.merge(temp, on='msisdn', how='outer')

# Writing the combined DataFrame back as ORC, using PyArrow for ORC write support and compression
table = pa.Table.from_pandas(df)
with pa.orc.ORCWriter(f'{user_path}par_month={mth_obs_window}/temp/lotus_visit/par_month=all.orc', compression='zlib') as writer:
    writer.write_table(table)


import pandas as pd
from datetime import datetime, timedelta

def monthdelta(date, delta):
    m, y = (date.month - delta) % 12, date.year + ((date.month) - delta - 1) // 12
    if not m: m = 12
    return date.replace(month=m, year=y)

def func_init():
    start_dt = datetime.strptime(start_dt_str, '%Y-%m-%d')
    dates = [monthdelta(start_dt, i).strftime('%Y%m') for i in range(7)]

    func_init.run_mth = [dates[0]]
    func_init.bck2mth = [dates[2]]
    func_init.bck4mth = [dates[4]]
    func_init.lst6mths = dates[1:7]
    func_init.lst3mths = dates[1:4]
    func_init.snapshot6mth_start = f"{dates[5]}01"
    func_init.snapshot6mth_end = f"{dates[0]}01"

# Replace 'start_dt_str' with the appropriate start date string, e.g., '2023-01-01'
start_dt_str = '2023-01-01'
func_init()

run_mth = func_init.run_mth
bck2mth = func_init.bck2mth
bck4mth = func_init.bck4mth
lst6mths = func_init.lst6mths
lst3mths = func_init.lst3mths
snapshot6mth_start = func_init.snapshot6mth_start
snapshot6mth_end = func_init.snapshot6mth_end

print('run_mth:', run_mth)
print('bck2mth:', bck2mth)
print('bck4mth:', bck4mth)
print('lst6mths:', lst6mths)
print('lst3mths:', lst3mths)
print('snapshot6mth_start:', snapshot6mth_start)
print('snapshot6mth_end:', snapshot6mth_end)


import pandas as pd

# Assuming the data is loaded from a CSV file
data = pd.read_csv('customer360_current.csv')

# Filtering the data
data = data[(data['activated_flag'] == 1) & (data['is_blocklist'] == 0)]

# Renaming column for easier access
data.rename(columns={'demo_age_fact_v2_final_age_num': 'age'}, inplace=True)

# Check if any row has a null 'age' and perform conditional actions
if data['age'].isnull().any():
    # If there are any null values, filter to only rows where 'age' is not null and save
    data_age = data.dropna(subset=['age'])
    data_age.to_orc(path + "data_age.orc", index=False)  # Save to ORC file, you need pyarrow for this
else:
    # If there are no null values, filter to only rows where 'age' is null and save
    data_age_missing = data[data['age'].isnull()]
    data_age_missing.to_orc(path + "data_age_missing.orc", index=False)  # Save to ORC file, you need pyarrow for this


import pandas as pd

# Assuming data is loaded from a CSV or similar source into pandas DataFrame
df = pd.read_csv('your_data.csv')

# Calculating the difference between two columns and counting rows with a non-positive difference
df['diff'] = df['bank_hour_mth'] - df['bank_hour_lst7d']
count_non_positive_diff = (df['diff'] <= 0).sum()
print(count_non_positive_diff)

# Lowercasing column names
df.columns = [c.lower() for c in df.columns]

# Simulating a join in a loop with DataFrame initialisation
ot = pd.DataFrame()  # Initialize empty DataFrame
mths = [202110, 202111, 202112]  # Example list of months

for i in mths:
    # Dummy read DataFrame, this would be replaced by an actual file read if data available
    temp_df = pd.DataFrame({'msisdn': [1, 2], 'segment': ['A', 'B']})
    temp_df = temp_df.rename(columns={'segment': f'segment_{i}'})
    if ot.empty:
        ot = temp_df
    else:
        ot = ot.merge(temp_df, on='msisdn', how='full')

# Dropping a column and group by aggregation
ot.drop(columns=['segment_none'], inplace=True, errors='ignore')  # errors='ignore' to handle if column doesn't exist
group_cols = [col for col in ot.columns if col != 'msisdn']
grouped = ot.groupby(group_cols).agg({'msisdn': 'count'}).rename(columns={'msisdn': 'count_msisdn'})
print(grouped)

# Simulating percent function
def percent(file_path, grp, var):
    df = pd.read_csv(file_path)
    df_count = df.groupby([grp, var]).size().reset_index(name='cnt')
    total_count = df_count.groupby(grp)['cnt'].transform('sum')
    df_count['cnt_percent'] = (df_count['cnt'] / total_count) * 100
    print(df_count)

# Assuming files for each category are available at specified paths
percent('digipulse_feature_manual_pct_grp_202110.csv', 'label_diagonal_manual', 'lazada_doubleDay')
percent('digipulse_feature_manual_pct_grp_202110.csv', 'label_diagonal_manual', 'shopee_doubleDay')


import pandas as pd
import numpy as np

# Load DataFrame (assuming data is already loaded into `df`)
# Cast columns to string
df['lazada_doubleDay'] = df['lazada_doubleDay'].astype(str)
df['shopee_doubleDay'] = df['shopee_doubleDay'].astype(str)

# Replace spaces with underscores in column names
df.columns = df.columns.str.replace(' ', '_')

# Example DataFrame loading for the not_found logic
not_found = pd.DataFrame()  # Assuming this is loaded or defined somewhere
if not not_found.empty:
    # Load dim_prod_tmh DataFrame (simulated)
    trn_not_found = pd.read_csv('dim_prod_tmh.csv')
    trn_not_found['start'] = pd.to_datetime(trn_not_found['start_datetime']).dt.strftime('%Y%m%d')
    trn_not_found['end'] = pd.to_datetime(trn_not_found['end_datetime']).dt.strftime('%Y%m%d')
    trn_not_found['start_mth'] = trn_not_found['start'].str[:6]
    trn_not_found['end_mth'] = trn_not_found['end'].str[:6]
    trn_not_found.rename(columns={'ref_resre_nbr': 'msisdn'}, inplace=True)
    trn_not_found = trn_not_found.sort_values(by='end', ascending=False).drop_duplicates(subset=['msisdn'])
    # Example join
    trn_not_found = trn_not_found.merge(not_found, on='msisdn')
    trn_not_found.to_orc(path + f'not_found_{lot}_{nm}.orc')  # Requires pyarrow
else:
    print("found all")

# Modify values example
def modify_values(r):
    if r == "A" or r == "B":
        return "dispatch"
    else:
        return "non-dispatch"

df['wo_flag'] = df['wo_flag'].apply(modify_values)

# Create DataFrame from list of tuples
values = [(1, 5), (2, 6), (3, 7), (4, 8)]
df = pd.DataFrame(values, columns=["mvv", "count"])
mvv_list = df['mvv'].tolist()
firstvalue = mvv_list[0]

# Trim spaces from DataFrame columns
for c_name in df.columns:
    df[c_name] = df[c_name].str.strip()

# Example if-else conditions for number checking
num = 1  # Example number
if num > 0:
    print("Positive number")
elif num == 0:
    print("Zero")
else:
    print("Negative number")

# Aggregate operations with max function
max_clm_month = pd.read_csv('clm_mastertable.csv')['par_month'].max()
chk = pd.read_csv('sms_received.csv')['cnt_distinct_smsheader'].max()
print(max_clm_month)

import pandas as pd
import numpy as np

# Assuming 'df1' is a pandas DataFrame already loaded with data
df = df1.copy()

# Fill NaN values with 0
df = df.fillna(0)

# Change data types
df['ctr'] = df['ctr'].astype(float)
df['campaign_base'] = df['campaign_base'].astype(int)

# Check if the count of unique 'msisdn' is equal to the number of rows
assert df['msisdn'].nunique() == len(df), "Counts do not match"

# Filtering DataFrames based on a condition
major_df = df[df['target'] == 0]
minor_df = df[df['target'] == 1]
ratio = int(len(major_df) / len(minor_df))
print(f'ratio: {ratio}')

# Function to calculate percentage and display it
def percent(var):
    tot = len(df)
    count_per_group = df[var].value_counts().reset_index()
    count_per_group.columns = [var, 'cnt_per_group']
    count_per_group['perc_of_count_total'] = (count_per_group['cnt_per_group'] / tot) * 100
    print(count_per_group)

percent('label')

# Adding two lists
listone = [1, 2, 3]
listtwo = [4, 5, 6]
joinedlist = listone + listtwo
print(joinedlist)

# Group by operation and aggregate functions
group_cols = [c for c in step1_base.columns]  # assuming 'step1_base' is another DataFrame
df_grouped = df.groupby(group_cols).agg(
    f_gambling=pd.NamedAgg(column='isGambling', aggfunc=lambda x: x.sum().astype(int)),
    inte_office_conf=pd.NamedAgg(column='inte_office_conference_platform_v1_office_conference_platform_v1_cat', aggfunc=lambda x: x.max().astype(int))
)
print(df_grouped)

# Assertions for numpy array operations and checking
x_pad = np.zeros((4, 9, 9, 2))  # Assuming x_pad is defined elsewhere
assert isinstance(x_pad, np.ndarray), "Output must be a np array"
assert x_pad.shape == (4, 9, 9, 2), f"Wrong shape: {x_pad.shape} != (4, 9, 9, 2)"
print(x_pad[0, 0:2, :, 0])
assert np.allclose(x_pad[0, 0:2, :, 0], np.zeros((2, 9)), atol=1e-15), "Rows are not padded with zeros"
assert np.allclose(x_pad[0, :, 7:9, 1].transpose(), np.zeros((2, 9)), atol=1e-15), "Columns are not padded with zeros"
assert np.allclose(x_pad[:, 3:6, 3:6, :], np.zeros((4, 3, 3, 2)), atol=1e-15), "Internal values are different"


import pandas as pd
import numpy as np

# Assuming 'selected_cols' is a list of column names in the DataFrame `df`
selected_cols = ['col1', 'col2', 'col3']  # Example column names

# Print customized expressions
for cols in selected_cols[1:]:
    upper_col_name = 'upper_' + cols
    lower_col_name = 'lower_' + cols
    ci_expr = ", udf_ci_upper(ci_struct('{cols}')).alias('{upper_col_name}'), udf_ci_lower(ci_struct('{cols}')).alias('{lower_col_name}')".format(cols=cols, upper_col_name=upper_col_name, lower_col_name=lower_col_name)
    print(ci_expr)

# Calculate group size
grp_size = len(df) // 5

# Add a new column for row numbers, simulating a row identifier (like Spark's monotonically_increasing_id())
df['row'] = np.arange(len(df))

# Sorting by 'spend_amount' for later binning operation
df.sort_values('spend_amount', inplace=True)

# Add a 'bin' column based on 'row' value, divided by group size
df['bin'] = df['row'] // grp_size

# Display modified DataFrame
print(df[['row', 'bin']])

import pandas as pd

# Assuming 'ot' is already a pandas DataFrame
cols_new = []
seen = set()
for c in ot.columns:
    if c in seen:
        cols_new.append(f"{c}_dup")
    else:
        cols_new.append(c)
    seen.add(c)

# Renaming columns in the DataFrame based on duplication check
ot.columns = cols_new

# Pandas does not have a caching mechanism similar to Spark; it inherently holds data in memory.
# The count here is simply getting the number of rows.
ot_count = len(ot)

# Output the columns to verify the new names
print(ot.columns)


import pandas as pd

# Example paths
path_data = 'your_path_data/'

# Define a dictionary of date ranges
date_ranges = {
    '201901': [20190101, 20190131], '201902': [20190201, 20190228], '201903': [20190301, 20190331],
    '201904': [20190401, 20190430], '201905': [20190501, 20190531], '201906': [20190601, 20190630],
    '201907': [20190701, 20190731], '201908': [20190801, 20190831], '201909': [20190901, 20190930],
    '201910': [20191001, 20191031], '201911': [20191101, 20191130], '201912': [20191201, 20191231]
}

for month, days in date_ranges.items():
    # Assume data is loaded from CSV files or another source
    fact_cdr_msc_agg_day = pd.read_csv('fact_cdr_msc_agg_day.csv')
    fact_cdr_ims_agg_day = pd.read_csv('fact_cdr_ims_agg_day.csv')
    fact_seq_agg_day = pd.read_csv('fact_seq_agg_day.csv')
    
    # Filter rows between start and end dates
    call1 = fact_cdr_msc_agg_day[(fact_cdr_msc_agg_day['par_day'] >= days[0]) & (fact_cdr_msc_agg_day['par_day'] <= days[1])].drop_duplicates(subset=['msisdn', 'par_day'])
    call2 = fact_cdr_ims_agg_day[(fact_cdr_ims_agg_day['par_day'] >= days[0]) & (fact_cdr_ims_agg_day['par_day'] <= days[1])].drop_duplicates(subset=['msisdn', 'par_day'])
    search = fact_seq_agg_day[(fact_seq_agg_day['par_day'] >= days[0]) & (fact_seq_agg_day['par_day'] <= days[1])].drop_duplicates(subset=['msisdn', 'par_day'])
    
    # Union the DataFrames and remove duplicates again
    call_data = pd.concat([call1, call2, search]).drop_duplicates(subset=['msisdn', 'par_day'])
    call_data['trn_cnt'] = 1
    
    # Assuming a directory structure that can accept DataFrame saves in ORC format, which requires PyArrow
    call_data[['par_day', 'msisdn', 'trn_cnt']].to_orc(f'{path_data}{month}/call_data.orc', index=False)

# Dictionary operation in Python
prices = {'apple': 0.40, 'orange': 0.35, 'banana': 0.25}
for k, v in prices.items():
    prices[k] = round(v * 0.9, 2)  # Apply a 10% discount

print(prices)


import pandas as pd
import numpy as np

# Define path data and dictionary for demo purposes
path_data = 'your_path_data/'
dictx = {
    '202003': [16.0, 55.0, 112.0, 180.0, 267.0, 364.0, 532.0, 783.0, 1340.0],
    '202004': [15.0, 48.0, 93.0, 147.0, 210.0, 296.0, 425.0, 623.0, 1169.0],
    '202005': [16.0, 45.0, 105.0, 168.0, 253.0, 350.0, 508.0, 745.0, 1263.0],
    '202006': [17.0, 58.0, 115.0, 175.0, 265.0, 374.0, 510.0, 749.0, 1417.0],
    '202007': [19.0, 67.0, 116.0, 202.0, 285.0, 396.0, 567.0, 813.0, 1334.0],
    '202008': [14.0, 55.0, 116.0, 188.0, 274.0, 383.0, 532.0, 768.0, 1302.0]
}

# Processing each month
for i, thresholds in dictx.items():
    # Read the corresponding DataFrame
    df = pd.read_csv(f'{path_data}{i}/search_engine.csv')  # assuming the ORC data is available as CSV

    # Process each threshold and assign groups
    bins = [0] + thresholds + [np.inf]
    labels = ['P1', 'P2', 'P3', 'P4', 'P5', 'P6', 'P7', 'P8', 'P9', 'P10']
    df['hit_cnt_grp'] = pd.cut(df['hit_cnt'], bins=bins, labels=labels, right=False)

    # Assuming you want to use only the last grouping for sampling
    # Randomly sample approximately 50% of the rows
    sampled = df.sample(frac=0.5, random_state=123)

    # Optional: Save the sampled DataFrame back to CSV or any other format
    sampled.to_csv(f'{path_data}{i}/sampled_search_engine.csv', index=False)


import pandas as pd
import numpy as np
from pathlib import Path

# Assuming data is loaded into pandas DataFrame, possibly from a CSV or another source
# Example DataFrame for 'genie_oneoff_detail_bot_rerun_no_search_engine_202007_bot_backfill_exec_202009300402_20201014'
df_genie = pd.read_csv('genie_oneoff_detail.csv')
# Simulate array_contains function in 'label' column for "bot_payment_app"
df_genie['new_label'] = df_genie['label'].apply(lambda x: "bot_payment_app" in x)
df_genie = df_genie[df_genie['new_label']]

# Assuming 'all_location' DataFrame is loaded from somewhere
all_location = pd.read_csv('all_location.csv')
# Convert 'province_en' to uppercase and remove spaces
all_location['province_en'] = all_location['province_en'].str.upper().str.replace(" ", "")

# For the operation on file paths
# Assuming data from file paths is loaded, simulate the input_file_name() operation
file_paths = ['/trueanalytics/data/enterprise/customer360/feature_detail_deprecated/geog_resident_detail/somefile_202009.orc']
df = pd.DataFrame({'input_file': file_paths})
df['par_month'] = df['input_file'].str[104:110]

# Try block for the substring operation in the column
try:
    # Extract substring from a column 'COLUMN_NAME'
    df['COLUMN_NAME_fix'] = df['COLUMN_NAME'].str[0:10]
except Exception as e:
    print(e)

# For ranking operations, using Window functions
# Sample data
data = {'col1': ['a', 'a', 'a'], 'col2': [10, 10, 20]}
df_rank = pd.DataFrame(data)

# Creating ranks
df_rank['rank'] = df_rank.groupby('col1')['col2'].rank(method='min')
df_rank['dense_rank'] = df_rank.groupby('col1')['col2'].rank(method='dense')
df_rank['row_number'] = df_rank.groupby('col1')['col2'].cumcount() + 1
print(df_rank)

import pandas as pd
import numpy as np

# Creating a sample DataFrame
data = {
    'ID': [1, None, None, None],
    'TYPE': ['B', None, 'B', 'C'],
    'CODE': ['X1', None, 'X1', None]
}
df = pd.DataFrame(data)

# Display the DataFrame
print("Original DataFrame:")
print(df)

# Drop rows where all elements are NaN
df1 = df.dropna(how='all')
print("\nDataFrame after dropping rows where all elements are NaN:")
print(df1)

# Dropping rows based on nulls in specific columns except 'msisdn'
customer360 = pd.DataFrame({
    'msisdn': [None, '123', '456', None],
    'data1': [None, None, None, None],
    'data2': [None, 1, 2, None]
})
customer360_used = customer360.dropna(subset=[c for c in customer360.columns if c != 'msisdn'], how='all')
print("\nCustomer360 DataFrame after dropping rows:")
print(customer360_used)

# Assuming 'df' is already defined
df['total'] = df['count'].transform(np.sum)
df['percent'] = df['count'] / df['total'] * 100
print("\nDataFrame with percentages:")
print(df)

# Example DataFrame creation
df = pd.DataFrame({
    'label': [['payment', 'app'], ['other']]
})

# Creating a new label based on array contents
conditions = [
    df['label'].apply(lambda x: 'bot_payment_app' in x)
]
choices = [True]

df['new_label'] = np.select(conditions, choices, default=False)
print("\nDataFrame with new label based on conditions:")
print(df)


import pandas as pd
from scipy.stats import rankdata

# Creating a sample DataFrame
data = {'col1': ['a', 'a', 'a'], 'col2': [10, 10, 20]}
df = pd.DataFrame(data)

# Apply ranking within groups
df['rank'] = df.groupby('col1')['col2'].rank(method='min')
df['dense_rank'] = df.groupby('col1')['col2'].rank(method='dense')
df['row_number'] = df.groupby('col1').cumcount() + 1
print("\nDataFrame with rankings:")
print(df)
