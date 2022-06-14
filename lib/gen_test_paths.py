#gen_test_paths.py
import random
import os
import argparse

'''
Problem Statement:

To generate random file paths with specified sample size'''

parser = argparse.ArgumentParser()
parser.add_argument('size', metavar='n', type=int)
args = parser.parse_args()

n = int(args.size)

#Load up MLHD Data Folder
folderlist = os.listdir('/data/mlhd/')
get_file_list = lambda fol_name: os.listdir('/data/mlhd/{}/'.format(fol_name))
get_path = lambda folder, file: '/data/mlhd/{}/{}'.format(folder, file)

def gen_file_list(sample_size):
    ls = []
    for i in range(sample_size):
        fol_name = random.choice(folderlist)
        fil_name = random.choice(get_file_list(fol_name))
        
        ls.append(get_path(fol_name, fil_name))
    return ls

def write(file_name, list_of_paths):
    '''
    1. Take in list of paths and filename
    2. Check if file already exists
    3. If file exists, open it and copy its contents in a var
    4. write these contents to another file with extension .old
    5. write required contents into file mentioned in func arg'''

    if file_name in os.listdir():
        with open(file_name, 'r') as f:
            file_content = f.readlines()
        
        with open(file_name+'.old', 'w+') as f:
            f.writelines(file_content)

        with open(file_name, 'w') as f:
            f.writelines([name+'\n' for name in list_of_paths])


# Driver code
generated_ls = gen_file_list(n)
write('random_file_paths.txt', generated_ls)