#!/usr/bin/python

__author__ = 'hofmann'

import unittest
import os
import sys
import shutil
import gzip
import itertools
from compress import Compress


class DefaultCompress(unittest.TestCase):
	_test_case_id = 0
	_success = False
	log_filename = 'unittest_log.txt'

	dir_input = "unittest_input"
	dir_output = "unittest_output_fa_{}"

	file_input = "input.txt"
	file_input2 = "input2.txt"
	dict_file_names = {
		'gz': "input.txt.gz",
		'bz2': "input.txt.bz2",
		'zip': "input.txt.zip",
		'7z': "input.txt.7z",
		}

	def setUp(self):
		self.dir_output = self.dir_output.format(self._test_case_id)
		if os.path.isdir(self.dir_output):
			shutil.rmtree(self.dir_output)
		os.mkdir(self.dir_output)
		sys.stderr.write("\n{}... ".format(self._test_case_id)),
		DefaultCompress._test_case_id += 1

		logfile = os.path.join(self.dir_output, self.log_filename)
		self.file_stream = open(logfile, 'a')
		self.test_object = Compress(logfile=self.file_stream, verbose=False)

	def tearDown(self):
		self.test_object = None
		self.table = None
		self.file_stream.close()
		self.file_stream = None
		if self._success:
			shutil.rmtree(self.dir_output)


class TestCompressMethods(DefaultCompress):

	def test_write_gz(self):
		file_path_input = os.path.join(self.dir_input, self.file_input)
		file_path_output = os.path.join(self.dir_output, self.dict_file_names['gz'])
		self.test_object.compress_file(file_path_input, file_path_output, compression_type='gz')
		with open(file_path_input) as ih, gzip.open(file_path_output) as oh:
			for linei, lineo in itertools.izip_longest(ih.readline(), oh.readline()):
				self.assertEqual(linei, lineo)
		self._success = True

	def test_read_gz(self):
		file_path_input = os.path.join(self.dir_input, self.file_input)
		file_path_output = os.path.join(self.dir_output, self.dict_file_names['gz'])
		self.test_object.compress_file(file_path_input, file_path_output, compression_type='gz')
		with open(file_path_input) as ih, self.test_object.open(file_path_output) as oh:
			for linei, lineo in itertools.izip_longest(ih.readline(), oh.readline()):
				self.assertEqual(linei, lineo)
		self._success = True

	def test_write_gz_list(self):
		file_path_input = os.path.join(self.dir_input, self.file_input)
		file_path_input2 = os.path.join(self.dir_input, self.file_input2)
		file_path_output = self.dir_output
		input = [file_path_input, file_path_input2]
		self.test_object.compress_list_of_files(input, file_path_output, compression_type='gz', max_processors=4)
		for file_path in input:
			with open(file_path) as ih, gzip.open(os.path.join(file_path_output, os.path.basename(file_path+".gz"))) as oh:
				for linei, lineo in itertools.izip_longest(ih.readline(), oh.readline()):
					self.assertEqual(linei, lineo)
		self._success = True

	def test_compress_list_tuples(self):
		file_path_input = os.path.join(self.dir_input, self.file_input)
		file_path_input2 = os.path.join(self.dir_input, self.file_input2)
		file_path_output = self.dir_output
		list_tuples = [(file_path_input, file_path_output), (file_path_input2, file_path_output)]
		self.test_object.compress_list_tuples(list_tuples, compression_type='gz', max_processors=4)
		for file_path, file_path_output in list_tuples:
			with open(file_path) as ih, gzip.open(os.path.join(file_path_output, os.path.basename(file_path+".gz"))) as oh:
				for linei, lineo in itertools.izip_longest(ih.readline(), oh.readline()):
					self.assertEqual(linei, lineo)
		self._success = True

# TODO: test everything else!

if __name__ == '__main__':
	suite0 = unittest.TestLoader().loadTestsFromTestCase(TestCompressMethods)
	# suite1 = unittest.TestLoader().loadTestsFromTestCase()
	# alltests = unittest.TestSuite([suite0, suite1])
	# unittest.TextTestRunner(verbosity=2).run(alltests)
	unittest.TextTestRunner(verbosity=2).run(suite0)
