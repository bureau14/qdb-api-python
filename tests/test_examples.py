# # pylint: disable=C0103,C0111,C0302,W0212
# import os
# import sys
# import unittest
# import settings

# sys.path.append(os.path.join(os.path.split(__file__)[0], '..', 'examples'))

# class QuasardbExamples(unittest.TestCase):

#     def test_batch_ts_insert_example(self):
#         pass

#     def test_csv_insert_example(self):
#         HAS_PANDAS = True
#         try:
#             import pandas
#         except ImportError:
#             HAS_PANDAS = False
#         if HAS_PANDAS:
#             import csv_insert
#             csv_insert.main(settings.INSECURE_URI, settings.entry_gen.next(), os.path.join(
#                 os.path.split(__file__)[0], '..', 'examples', "fake_currency_minutes.csv"))
#         else:
#             print (
#                 "\nCannot run the example csv_insert as the module pandas is not found.\n")

#     def test_expiry_example(self):
#         import expiry
#         expiry.main(settings.INSECURE_URI, settings.entry_gen.next())

#     def test_query_tags_example(self):
#         import query_tags
#         query_tags.main(settings.INSECURE_URI)

#     def test_time_series_example(self):
#         import time_series
#         time_series.main(settings.INSECURE_URI, settings.entry_gen.next())

#     def test_temperature_example(self):
#         import temperature
#         temperature.main(settings.INSECURE_URI, settings.entry_gen.next(), 100)


# if __name__ == '__main__':
#     if settings.get_lock_status() is False:
#         settings.init()
#         test_directory = os.getcwd()
#         test_report_directory = os.path.join(os.path.split(
#             __file__)[0], '..', 'build', 'test', 'test-reports')
#         import xmlrunner
#         unittest.main(testRunner=xmlrunner.XMLTestRunner(  # pylint: disable=E1102
#             output=test_report_directory), exit=False)
#         settings.terminate()
