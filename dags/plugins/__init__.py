# # import sys
# # sys.path.append('../')

# from __future__ import division, absolute_import, print_function

# from airflow.plugins_manager import AirflowPlugin
# # from airflow.plugins_manager import AirflowPlugin

# import operators
# import helpers

# #defining the plugin class
# class UdacityPlugin(AirflowPlugin):
#     name="udacity_plugin"
#     operators = [
#         operators.StageToRedshiftOperator,
#         operators.LoadFactOperator,
#         operators.LoadDimensionOperator,
#         operators.DataQualityOperator
#     ]

#     helpers = [
#         helpers.SqlQueries
#     ]