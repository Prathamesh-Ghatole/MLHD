import pandas as pd


# NOTE this fuction drops all rows with missing recording-MBID values.

# Reads a list of file paths and reads + compiles data into a single pd.DataFrame
def read_files(file_path_repo):
    # init new empty main dataframe
    df = pd.DataFrame(columns = ['timestamp', 'artist-MBID', 'release-MBID', 'recording-MBID'])
    
    
    # If input is not a list, open the string as path.
    if not isinstance(file_path_repo, list):
        # Open a file with MLHD file paths to process
        with open(file_path_repo, 'r') as f:
            file_paths = f.readlines()
            file_paths= [item.strip() for item in file_paths]
    
    # If it's a list, just use it.
    else:
        file_paths = file_path_repo    
    
    # Read files and compile into single df
    for pth in file_paths:
        temp = pd.read_csv(pth, sep='\t', names=['timestamp', 'artist-MBID', 'release-MBID', 'recording-MBID'])
        temp = temp[-temp['recording-MBID'].isna()]

        df = pd.concat([df, temp])
    
    return df

def gen_and_load(samplesize):
    import lib.gen_test_paths as gen_test_paths
    
    file_list = gen_test_paths.gen_file_list(sample_size=samplesize)

    return read_files(file_list)