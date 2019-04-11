from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import functions as F
from pyspark.sql.types import *
import re
import os
from constants import *


def CleanNotes(text):
	# Truncate everything after the below unnecessary sections, if they exist
	text2 = re.sub(
		r'((discharge follow up|followup instructions|discharge condition|'
		r'discharge instructions|follow-up appointment).*)',
		'',
		text.lower(),
		flags=re.DOTALL
	)
	# Split the text into sections
	splitText = [
		part.replace('\n', ' ').replace('  ', ' ').replace('pt', 'patient')
		for part in text2.split('\n\n')
	]
	# Remove the below sections and various occurrences within other sections
	regexText = [
		re.sub(
			r'(^\s?('
			r'admission date|discharge date|date of birth|sex|attending|social history|'
			r'family history|allergies|discharge medication|physical exam|medications on admission|'
			r'medications on discharge|discharge exam|discharge disposition|medications|'
			r'neurologic|vitals|transitional issues|pertinent results|relevant labs|'
			r'labs on discharge|discharge labs|studies pending|echocardiogram|microbiology|'
			r'dictated by|laboratory data|followup:|disposition|labs from|status on discharge|'
			r'condition on discharge|home medication|laboratories|discharge followup'
			r').*)'
			r'|([\[].*?[\]])|([\(].*?[\)])'
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
	# Put the 'discharge diagnosis' section at the front if it exists
	diagRgx = re.compile(r'^(discharge diagnoses.*|discharge diagnosis.*)', flags=re.IGNORECASE | re.DOTALL)
	diagLine = [re.search(diagRgx, part).group(1) for part in regexText if re.search(diagRgx, part)]
	regexText = diagLine + regexText

	# Snipping unneeded text after '#' and diagnoses lines (extra detail after the word 'patient')
	regexText = [
		re.sub(
			r'(\#.*?)(patient.*)',
			'\\1',
			part
		)
		for part in regexText
	]
	return ' '.join([item for item in regexText if item != '' and item != ' '])


def ExtraWordRemoval(text):
	# Additional removal of abbreviations and other anomalies
	newText = re.sub(
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
		text
	)
	newText = re.sub(
		r'(w/)',
		'with',
		newText
	)
	newText = ' '.join(newText.split()[:512])
	return newText


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

	# Define the UDFs for preprocessing
	cleanNote_udf = F.udf(CleanNotes, StringType())
	extraWord_udf = F.udf(ExtraWordRemoval, StringType())

	# Remove all addenda entries and perform preprocessing methods defined above
	df_notes = df_notes \
		.filter((F.col('DESCRIPTION') != 'Addendum')) \
		.withColumn('TEXT', cleanNote_udf(F.col('TEXT'))) \
		.filter(F.col('TEXT').rlike('addendum') == False) \
		.withColumn('TEXT', extraWord_udf(F.col('TEXT'))) \
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
	grouped.rdd.saveAsPickleFile('dataframes/df_data.pkl')
