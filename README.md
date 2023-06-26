# Codac assignment

## Description
This application is written to help company called **KommatiPara** that deals with bitcoin trading.
Company has two separate datasets dealing with clients that they want to collate to starting interfacing more with their clients.
One dataset contains information about the clients and the other one contains information about their financial details.
The company now needs a dataset containing the emails of the clients from the United Kingdom and the Netherlands and some of their financial details to starting reaching out to them for a new marketing push.

## Input data
For application to work properly, user needs to provide:

1. Path to csv files:
    - Path to clients dataset file. This data should contain columns:

        |id|first_name|last_name|email|country|
        |---|---------|---------|-----|-------|

    - Path to financial details dataset file. This data should contain columns:

        |id|btc_a|cc_t|cc_n|
        |---|---------|---------|-----|

2. Countries for which data should be filtered

## Run

To run this application, after installing package, run this command in root directory

    > Codac  --personal 'personal_data.csv'  --financial 'financial_data.csv'  --country 'country1' 'country2' ...

### Example:

    > Codac  --personal ./raw_data/dataset_one.csv  --financial ./raw_data/dataset_two.csv  --country 'United Kingdom' 'Netherlands'

Only country argument is optional and defaults to *"Netherlands"*. If paths to files are not provided or are incorrect, application returns OSError.


## Output

Result is saved in *client_data* folder as csv file with joined, filtered and renamed data from both datasets.
Output file should contain columns like this:

|client_identifier|email|country|bitcoin_address|credit_card_type|
|-----------------|-----|-------|---------------|----------------|

