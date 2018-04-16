# -*- coding: utf-8 -*-


"""
	Google Cloud Wrapper

	Usage : 

	>>> from le_lp_tools import StorageWrapper
	>>> storage_client = StorageWrapper(project_id, bucket_name)
	>>> storage_client.upload_blob(file_name, blob_name)
"""


from google.cloud import bigquery
from google.cloud import storage

import numpy as np 
import re 

from datetime import datetime, timedelta


__all__ = ['StorageWrapper', 'BigQueryTableWrapper']

class StorageWrapper:

	def __init__(self, project_id, bucket):
		self.client = storage.Client(project=project_id)
		self.bucket = self.client.get_bucket(bucket)


	def upload_blob(self, file_name, blob_name):
	
		blob = self.bucket.blob(blob_name)
		blob.upload_from_filename(file_name)

		print('File {} uploaded to {}.'.format(\
			file_name,\
			blob_name))

	def delete_blob(self,  blob_name):
		blob = self.bucket.blob(blob_name)

		if blob.exists():
			blob.delete()
			print("Remove file : {}".format(blob_name))

	def get_next_extraction_date(self,  prefix , delimiter = None, pattern ="\d{4}-\d{2}-\d{2}" ):
		blobs = self.bucket.list_blobs(prefix=prefix, delimiter =delimiter)
		blobs = list(blobs)

		pattern = pattern

		# Parsing blobs to get last update 
		datetime_list = []

		for blob in blobs:
			search = re.search(pattern,blob.name)
			if search : 
				datetime_file = search.group()
				datetime_object = datetime.strptime(datetime_file, "%Y-%m-%d")
				datetime_list.append(datetime_object)

		date_extract = max(datetime_list) + timedelta(days = 1)

		return(date_extract.date())

class BigQueryTableWrapper:
	def __init__(self, project_id, dataset_id, table_id):
		self.client = bigquery.Client(project=project_id)
		self.dataset_ref = self.client.dataset(dataset_id)
		self.table_ref = self.dataset_ref.table(table_id)
		self.table = self.client.get_table(self.table_ref)



	def stream_to_bg(self, data, account, event_order):
		"""
			Stream list of dicts to bigquery 
		"""
		data_ordered = []

		for index in np.arange(len(data)):
			data[index]['account'] = account

			dict_ordered = OrderedDict()
			for key in event_order:
				if key in data[index].keys():
					dict_ordered[key] = data[index][key]
				else :
					dict_ordered[key] = ''

			data_ordered.append(dict_ordered)

		rows_to_insert = [tuple(row.values()) for row in data_ordered]
		errors = self.client.insert_rows(self.table, rows_to_insert)

		return errors

	def upload_csv_bq(self, blob_name, sep = ','):
		
		job_config = bigquery.LoadJobConfig()
		job_config.write_disposition = "WRITE_APPEND"
		job_config.source_format = "CSV"
		job_config.field_delimiter = sep

		load_job = self.client.load_table_from_uri(\
			blob_name,\
			self.table_ref,\
			job_config=job_config) 

		load_job.result()
		
		return 0 