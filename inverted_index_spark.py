from pyspark import SparkContext
import sys
import os

# Set input and output paths
input_dir = '/home/hadoop/dataset'  # Local path to the three folders you have
output_dir = '/home/hadoop/spark_output'  # Local output folder

sc = SparkContext(appName="InvertedIndexSpark")

def read_file(filepath):
    filename = os.path.basename(filepath)
    with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
        for line in f:
            for word in line.strip().split():
                # Clean word: remove punctuation, lower case
                word_clean = ''.join(filter(str.isalnum, word)).lower()
                if word_clean:
                    yield (word_clean, filename)

import glob

# Get all text files from your dataset subfolders
files = glob.glob(os.path.join(input_dir, '*/*.txt'))

# Parallelize the list of files
rdd = sc.parallelize(files)

# FlatMap: for each file, emit (word, filename)
word_file_pairs = rdd.flatMap(lambda filepath: list(read_file(filepath)))

# Map to ((word, filename), 1)
word_file_ones = word_file_pairs.map(lambda x: ((x[0], x[1]), 1))

# Reduce by key to count occurrences per (word, filename)
word_file_counts = word_file_ones.reduceByKey(lambda a, b: a + b)

# Map to (word, "filename1:count1 filename2:count2 ...")
word_to_filecount = word_file_counts.map(lambda x: (x[0][0], (x[0][1], x[1]))) \
    .groupByKey() \
    .mapValues(lambda vals: ' '.join([f"{fn}:{cnt}" for fn, cnt in vals]))

# Save as text file (output as lines: word \t filename1:count1 filename2:count2 ...)
word_to_filecount.map(lambda x: f"{x[0]}\t{x[1]}").saveAsTextFile(output_dir)

sc.stop()
