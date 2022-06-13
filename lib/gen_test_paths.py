import random
import os
import argparse

'''
Problem Statement:

To generate random file paths with specified sample size'''


#Load up MLHD Data Folder
folderlist = os.listdir('/data/mlhd/')
get_file_list = lambda fol_name: os.listdir('/data/mlhd/{}/'.format(fol_name))
get_path = lambda folder, file: '/data/mlhd/{}/{}'.format(folder, file)

def gen_file_list(sample_size):
    return None


# # Get a list of all file paths
# file_paths = [path(name) for name in filelist]
