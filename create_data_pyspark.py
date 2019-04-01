from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import functions as F
from pyspark.sql.types import *
from constants import *


if __name__ == '__main__':

	# Start PySpark session
	conf = SparkConf()
	sc = SparkContext(conf=conf)
	sqlContext = SQLContext(sc)

	# Load in PROCEDURES_ICD.csv.gz
	df_proc = sqlContext.read.format("com.databricks.spark.csv") \
		.option('header', 'true') \
		.load('%s/PROCEDURES_ICD.csv.gz' % MIMIC_3_DIR)

	# Initial processing of PROCEDURES_ICD.csv.gz,
	# including proper decimal placement for ICD9 codes
	df_proc = df_proc \
		.withColumn('ICD9_CODE', F.regexp_replace('ICD9_CODE', '"', '')) \
		.withColumn('SUBJECT_ID', F.col('SUBJECT_ID').cast('integer')) \
		.withColumn('HADM_ID', F.col('HADM_ID').cast('integer')) \
		.withColumn('ICD9_CODE', F.concat_ws(
			'.',
			F.substring(F.col('ICD9_CODE'), 1, 2),
			F.substring(F.col('ICD9_CODE'), 3, 6)))

	# Load in DIAGNOSES_ICD.csv.gz
	df_diag = sqlContext.read.format("com.databricks.spark.csv") \
		.option('header', 'true') \
		.load('%s/DIAGNOSES_ICD.csv.gz' % MIMIC_3_DIR)

	# Initial processing of DIAGNOSES_ICD.csv.gz,
	# including proper decimal placement for ICD9 codes
	df_diag = df_diag \
		.withColumn('ICD9_CODE', F.regexp_replace('ICD9_CODE', '"', '')) \
		.withColumn('SUBJECT_ID', F.col('SUBJECT_ID').cast('integer')) \
		.withColumn('HADM_ID', F.col('HADM_ID').cast('integer')) \
		.withColumn('ICD9_CODE',
					F.when(
						F.substring(F.col('ICD9_CODE'), 1, 1) == 'E',
						F.when(
							F.length(F.col('ICD9_CODE')) > 4,
							F.concat_ws(
								'.',
								F.substring(F.col('ICD9_CODE'), 1, 4),
								F.substring(F.col('ICD9_CODE'), 5, 4)
							)
						).otherwise(F.col('ICD9_CODE'))
					).otherwise(
						F.when(
							F.length(F.col('ICD9_CODE')) > 3,
							F.concat_ws(
								'.',
								F.substring(F.col('ICD9_CODE'), 1, 3),
								F.substring(F.col('ICD9_CODE'), 4, 5)
							)
						).otherwise(F.col('ICD9_CODE'))
					))

	# Concatenate ICD9 codes together
	df_codes = df_diag.union(df_proc)

	# Load in NOTEEVENTS.csv.gz
	df_notes = sqlContext.read.format("com.databricks.spark.csv") \
		.option('header', 'true') \
		.option('multiLine', 'true') \
		.option('escape', '"') \
		.load('%s/NOTEEVENTS.csv.gz' % MIMIC_3_DIR)

	# Only consider discharge summaries:
	df_notes = df_notes.filter(F.col('CATEGORY') == 'Discharge summary')

	# Drop rows with null HADM_IDs
	df_notes = df_notes.dropna(subset=['HADM_ID'])

	# Convert all IDs to int
	df_notes = df_notes \
		.withColumn('SUBJECT_ID', F.col('SUBJECT_ID').cast('integer')) \
		.withColumn('HADM_ID', F.col('HADM_ID').cast('integer'))

	# Some non-aggressive preprocessing. More preprocessing will be done by model tokenizer.
	df_notes = df_notes \
		.withColumn('TEXT', F.regexp_replace('TEXT', '\n', ' ')) \
		.withColumn('TEXT', F.regexp_replace('TEXT', '[\[].*?[\]]', ''))

	# Append all notes and addenda
	#  !!! Running into errors here due to large size of operation, but it's technically sound (tested on smaller dataset)
	df_notes_grouped = df_notes \
		.groupBy('HADM_ID', 'SUBJECT_ID') \
		.agg(F.concat_ws(' ', F.collect_list('TEXT')) \
			 .alias('TEXT'))

	# Left join the two dataframes such that we discard any HADM_IDs not appearing in df_notes
	merged = df_notes_grouped \
		.withColumnRenamed('SUBJECT_ID', 'SUBJECT_ID_notes') \
		.join(df_codes, ['HADM_ID'], how='left_outer')

	# Group by HADM_ID and SUBJECT_ID, aggregating on the ICD9_codes and appending them to a list.
	grouped = merged.groupBy('HADM_ID', 'SUBJECT_ID') \
		.agg(
			F.concat_ws(';', F.collect_list('ICD9_CODE')).alias('LABELS'),
			F.first('TEXT').alias('TEXT')
	)
	# Final cosmetic stuff (rest of operations from initial file have already been incorporated above)
	grouped = grouped \
		.orderBy('SUBJECT_ID', 'HADM_ID') \
		.select('SUBJECT_ID', 'HADM_ID', 'TEXT', 'LABELS')

	# Save dataframe
	grouped.rdd.saveAsPickleFile('dataframes/df_data.pkl')
