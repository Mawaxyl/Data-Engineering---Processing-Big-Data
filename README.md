# Data-Engineering---Processing-Big-Data
In this project, as a Data Engineer tasked with ingesting and transforming stock market data into a readable, reliable, and robust format. This stock market data contains historical daily prices for all tickers currently trading on Nasdaq.

The tasks include using *Apache Spark* to harness parallel processing in the transformation phase, profiling the data set according to the six dimensions of data quality, and implementing data quality testing with *Deequ*.

## Task 1 - Data Ingestion
The data was downloaded from S3 bucket and stored it on my local machine.

This first step in the data engineering process serves to ingest the data into a working environment which makes it available for wider use for anyone else.

A specific subset of the dataset was use to develop the ingestion, data quality and testing process. This first create a robust process before starting on the productionisation of the process and code.

I used the first data subset (1962) in the dataset as a testing set.

Some of the reasons for choosing this dataset is:

- it is the first year within the dataset;
- it contains a small portion of the dataset and will be very fast to develop on, while not requiring a lot of computation; and since it is the oldest in the dataset, it likely contains the most errors.
- At the end of this task, the notebook would be able to produce parquet files for a specific year.

## Task 2 - Data Profiling
After the data has been ingested and basic sense checks have been performed on the dataset, the next step is to ensure that we have a full view of the dataset. This does not refer to exploratory data analysis (EDA) that data scientists are typically familiar with, but rather an exploration of the dataset considering the six (or more) dimensions of data quality which are:
>1. Validity
>2. Completeness
>3. Timeliness
>4. Uniqueness
>5. Accuracy
>6. Consistency

This ensures that we are completely aware of the data landscape, any possible flaws in the dataset, and characteristics that will have to be considered when building models or performing analytics.


In this section, I had to:

- Derive summary statistics on the dataset across the six dimensions of data quality; and
- identify possible concerns in the data quality, and correct any issues identified.
- At the end of the task, I produced a csv file of all the data transformations I have made during the data quality checks.

## Task 3 - Data Deequ Analysis

Manually inspecting the data for characteristics and flaws is very powerful but not practical when working with production-grade datasets.

Production systems have several challenges, the largest being:

- Size: Incoming data can be so large that manual inspection becomes unfeasible; and
- Frequency: Data can arrive at any interval, sometimes being near real-time. At which point, manual inspection would be impossible as well.

This can be mediated by having an automated process for monitoring data quality and performing tests every time the data gets ingested.

In this section, I was required to set up a generic process on which we can perform continuous data quality monitoring and testing.
