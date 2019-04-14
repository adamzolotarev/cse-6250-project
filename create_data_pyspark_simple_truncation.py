from pyspark.sql import functions as F
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql.types import *
import re
import os
from constants import *


def SimpleCleaning(text):
	splitText = [
		part.lower().replace('\n', ' ').replace('  ', ' ').replace('pt', 'patient')
		for part in text.split('\n\n')
	]
	regexText = [
		re.sub(
			r'([\[].*?[\]])|([\(].*?[\)])'
			r'|(\b\d+[^a-zA-Z]+\b)'  # Remove numbers not immediately followed by a letter 
			r'|(\*|\?)'
			r'|(\s\s)'
			r'|(\S*\d+\S*)'
			r'|(\_|\-)'
			r'|(\S+\/\S+)'
			r'|(\.{2,100})'
			r'|(\,)',
			'',
			part
		).strip()
		for part in splitText
	]
	string = ' '.join([item for item in regexText if item != '' and item != ' '])
	# Additional removal of abbreviations and other anomalies
	newStr = re.sub(
		r'(m\.d\.)|'
		r'(\smr\.)|'
		r'(\sdr\.)|'
		r'(\smg)|'
		r'(\smm)|'
		r'(\sml)|'
		r'(\scm)|'
		r'(\sct)|'
		r'(\sd:)|'
		r'(\st:)|'
		r'(\sjob\#:)|'
		r'(\sa\.m\.?)|'
		r'(\sp\.?m\.?)',
		'',
		string
	)
	newStr = re.sub(
		r'(w/)',
		'with',
		newStr
	)
	newText = ' '.join(newStr.split()[:512])
	return newText


if __name__ == "__main__":
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

	# Create UDF for SimpleCleaning() function
	simpleCleaning_udf = F.udf(SimpleCleaning, StringType())

	# Remove all addenda entries and perform preprocessing methods defined above
	df_notes = df_notes \
		.filter((F.col('DESCRIPTION') != 'Addendum')) \
		.withColumn('TEXT', simpleCleaning_udf(F.col('TEXT'))) \
		.filter(F.col('TEXT').rlike('addendum') == False) \
		.dropDuplicates(subset=['TEXT'])

	# Drop all notes related to HAMD_IDs with multiple note entries (~2% of notes)
	singleEntries = df_notes.groupBy('HADM_ID').count().filter(F.col('count') == 1).select('HADM_ID')
	df_notes = singleEntries.join(df_notes, ['HADM_ID'], how='left_outer')

	# Left join the two dataframes such that we discard any HADM_IDs not appearing in df_notes
	merged = df_notes \
		.withColumnRenamed('SUBJECT_ID', 'SUBJECT_ID_notes') \
		.join(df_codes, ['HADM_ID'], how='left_outer')

	# Group by HADM_ID and SUBJECT_ID, aggregating on the ICD9_codes and appending them to a list.
	grouped = merged.groupBy('HADM_ID', 'SUBJECT_ID') \
		.agg(
			F.concat_ws(';', F.collect_list('ICD9_CODE')).alias('LABELS'),
			F.first('TEXT').alias('TEXT')
	)
	# Final cosmetic stuff
	grouped = grouped \
		.orderBy('SUBJECT_ID', 'HADM_ID') \
		.select('SUBJECT_ID', 'HADM_ID', 'TEXT', 'LABELS')

	# Save dataframe, creating dataframe folder if it doesn't already exist
	folderName = 'dataframes'
	os.makedirs(os.path.dirname(folderName), exist_ok=True)
	grouped.rdd.saveAsPickleFile('dataframes/df_data_simple_trunc.pkl')