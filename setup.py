# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

setup(
	name="le_lp_tools",
	version="0.0.1",
	packages=['le_lp_tools'],
	author="Camille Douay",
	author_email="camille.douay@gmail.com",
	description="set of tools to help datascience application developpement on GCP",
	install_requires=[
		"google-cloud-bigquery==0.28.0",
		"google-cloud-storage==1.8.0",
		"google-api-core<0.2.0dev,>=0.1.1",
		"numpy==1.14.2"
	],
	include_package_data=True,
)	