# Cleaning the Music Listening Histories Dataset

## 1. **Introduction**
### **What is MLHD?**
The Music Listening Histories Dataset (MLHD) is a large-scale collection of music listening events assembled from more than 27 billion time-stamped logs extracted from Last.fm.

This results in 583k users, 555k unique artists, 900k albums, and 7M tracks. Here each scrobble is represented in the following format: ```<timestamp, artist-MBID, release-MBID, recording-MBID>``` 

Research Paper: https://simssa.ca/assets/files/gabriel-MLHD-ismir2017.pdf

Download the dataset from: https://ddmal.music.mcgill.ca/research/The_Music_Listening_Histories_Dataset_(MLHD)/

### **What is this repository for?**
Unfortunately, the original MLHD has some significant fallbacks due to last.fm’s out-of-date matching algorithms with the MusicBrainz DB, resulting in frequent mismatches & errors in the MBID data, affecting the quality of the available dataset. 

Overall, the goal of this project is to create an updated version of the MLHD in the same format as the original, but with incorrect data resolved and invalid data removed.

## 2. **Getting Started**
1. Clone the repo, and setup a Python3 virtual environment using: 
    - ```   python3 -m venv env   ```
2. Activate the virtual environment using: 
    - ```   . env/bin/activate   ```
3. Install the required packages using: 
    - ```   pip install -r requirements.txt   ```
4. Download the original MLHD dataset from: https://ddmal.music.mcgill.ca/research/The_Music_Listening_Histories_Dataset_(MLHD)/
5. Copy ```config.py.sample``` to ```config.py``` and update parameters as required. 
6. Run ```python gen_tables.py``` to generate the required tables for the dataset.
7. Ready to go!

## 3. **Usage**
### Processing Tools:
- ```clean_master.py``` - Cleans the dataset.
- ```rec_track_checker.py``` - Loops through the datasets and checks if any artist_mbid is present in the recording_mbid column and converts every file from CSV+GZIP to CSV+ZSTD.

### Utility Tools:
- ```gen_tables.py``` - Generates the required tables for the dataset.
- ```config.py``` - Configuration file for the project. (Can also be run as a script to setup the project incase the scripts don't cover it already.)
- ```lib/gen_test_paths.py``` - Utility to generate a random set of paths for testing.
    - usage: ```python lib/gen_test_paths.py <num_paths> <output_path>```
- ```mapper_gen_names.py``` - Utility for cleaning mlhd_recording_mbid, and fetches (rec_name, artist_credit) for given set of files.

### Experimentation Tools:
- ```mapper.py``` - Takes a set of random file paths and generates a report of the mapping results. (Useful for testing the MBC mapper.)
- ```test_arrow_vsd_pandas.ipynb``` - Tests the performance of the arrow library vs pandas for reading the dataset.
- ```test_csv_parser.ipynb``` - Experimental notebook for testing custom vectorized CSV parsers in Python.
- ```test_file_type_io_testing.ipynb``` - Tests <readtime, writetime, and filesize> for <CSV+GZIP, CSV+ZSTD, and Parquet+ZSTD, Parquet+Snappy> files.
- ```test_file_type_io_testing_sql.ipynb``` - Tests <readtime, writetime, and filesize> for SQL tables dumped as <Snappy+Parquet, ZSTD+Parquet, and ZSTD10+Parquet> files.
- ```test_mapper.py``` - An experimental notebook to test different mappers.
- ```test_MLHD_conflation_mapping.ipynb``` - A predecessor to ```mapper.py```. Uses the [MBID-Mapping API](https://labs.api.listenbrainz.org/mbid-mapping/json) to check if a recording_mbid corresponds to a given artist_mbid. Similar to mapper.py, but uses the API instead of the local database.
- ```test_MLHD_conflation.ipynb``` - A predecessor to ```mapper.py```. Cleans recording_mbids, and fetches artist name and artist credit for cleaned recording_mbids.
- ```test_MLHD_old.ipynb``` - Legacy notebook for testing the original MLHD dataset. Surveys a lot of the issues with the dataset.
- ```test_rec_track_checker.ipynb``` - Experimental notebook for testing the ```rec_track_checker.py``` script.
- ```test_write_arrow.ipynb``` - Compares the performance of writing CSV+ZSTD files using the arrow library and pd.to_csv() functions.

# Resources
This project has been pretty fun to work on, and I've learned a lot along the way. 

I've compiled all my learnings, and resources that I used throughout this project in the following sections. Hope you find them useful!

## Lessons Learnt
- Don’t rely on autosave.
- Getting around and moving every 45 min aids in debugging.
- Use ```pandas.DataFrame.isin()``` to make boolmaps. It uses Cython, and reaches C level of speeds. It’s faster than ```loc``` or ```iloc```.
- [Use ```IPython.display.Markdown``` for making dynamic dashboards within Jupyter Notebook!
- With enough work, you can speed up python code by _~596915%_.](http://shvbsle.in/computers-are-fast-but-you-dont-know-it-p1/)
- [Sometimes even replacing Pandas with custom python functions can speed up the process by _~9900% times_](http://shvbsle.in/computers-are-fast-but-you-dont-know-it-p1/).
- Numba works best when used mindfully for optimizing specific low level functions. Not as easy as just slapping a function decorator before every function.
- Use the with statement when making connections with SQL. (probably a best practice?) 
    - e.g. ```with sqlite3.connect(config.DB_PATH) as conn:```
- Use ```isinstance()``` instead of ```type()``` to check type of objects. (again, a best practice. Part of the liskov substitution principle)
- Use ```is``` instead of ```==``` for comparing ```None```, ```True```, ```False``` in Python
- Lambda functions are overpowered
- Use Caching when running slow queries. It saves time and compute power on the API.
- Think before bruteforcing through an issue. Saves time in the long run.
- Use ```os.walk()``` for directory trees. 
- Use ```os.path.join()``` for path concatenation instead of string concatenation.
- Use ```time.monotonic()``` instead of time.time() for measuring time. (more accurate, and doesn’t change with system time)
- Unittests are powerful (but painful to write)
- Learnt more about [Dask, VAEX, and other data processing libraries](https://www.kdnuggets.com/2021/03/pandas-big-data-better-options.html)
- Apparently, ```time.perf_counter()``` is more accurate than ```time.monotonic()``` with only a small performance hit. It’s also used in ```time.timeit()``` by default.
- Sometimes a simple restart can solve seemingly impossible issues.
- Don’t make calculation mistakes when calculating calculation time.
- [Keep a process running after terminating SSH session using ```tmux```](https://askubuntu.com/questions/8653/how-to-keep-processes-running-after-ending-ssh-session/220880#220880)
- Auto Docstring extension for vscode is OP
- [Apache Arrow](https://arrow.apache.org/use_cases/) is 7x faster for reading and writing CSV files than Pandas!
- Use [main guard](https://stackoverflow.com/questions/19578308/what-is-the-benefit-of-using-main-method-in-python) for running scripts. (```if __name__ == "__main__":```)
- Learnt about [Modin](https://modin.readthedocs.io/en/latest/) as a drop in replacement of Pandas for faster performance.
- [Learnt how to fetch a list of variables from an imported python module](https://stackoverflow.com/questions/9759820/how-to-get-a-list-of-variables-in-specific-python-module)

## **Interesting Findings**
- In Pandas/Numpy: Numeric types include: ```int, float, datetime, bool, category```. They exclude ```object``` dtype and can be held in contiguous memory blocks. (i.e. faster performance) ([reference](https://stackoverflow.com/questions/52673285/performance-of-pandas-apply-vs-np-vectorize-to-create-new-column-from-existing-c))
- Vectorized Loops are a LOT faster than simple for loops. ```pandas.DataFrame.apply()``` is unvectorized under the hood.
- ```pandas.DataFrame.at()``` is wayy faster than ```Pandas.DataFrame.loc()```
    - ```pd.DataFrame.loc()``` returns the whole row, but ```pd.DataFrame.at()``` only returns single value.
    - But running ```pd.DataFrame.at()``` 2 times for fetching multiple values is still ~55x faster than running ```pd.DataFrame.loc()``` once for fetching the complete row.
-   ```pandas.DataFrame.to_sql()``` is slow. Use ```pandas.DataFrame.to_csv()``` and ```psycopg2``` to write to SQL.
- ```pandas.read_sql()``` is ridiculously slow for some reason.
- ```pd.read_sql()``` with ```psycopg2``` connector is _80% faster_ than ```pd.read_sql()``` with ```SQLalchemy``` connector

## **Articles**
- https://www.kdnuggets.com/2021/03/pandas-big-data-better-options.html
- https://towardsdatascience.com/ten-reasons-to-use-staticframe-instead-of-pandas-f368cc81e50a
- https://stackoverflow.com/questions/58595166/how-to-compress-parquet-file-with-zstandard-using-pandas
- https://www.webucator.com/article/python-clocks-explained/
- https://askubuntu.com/questions/8653/how-to-keep-processes-running-after-ending-ssh-session/220880#220880
- https://colab.research.google.com/drive/1UjD5Nsm_2fX2zp5QCu0DOtEGAe1cJy_I#scrollTo=FVmoLlUGd0w3
- https://arrow.apache.org/use_cases/
- [Talk - Deepak K Gupta: Speed Up Data Access with PyArrow Apache Arrow Data is the new API - YouTube](https://www.youtube.com/watch?v=akfsWPsvmrM)
- https://towardsdatascience.com/stop-using-pandas-to-read-write-data-this-alternative-is-7-times-faster-893301633475
- [G-Research Distinguished Speaker Series: Apache Arrow - High Performance Columnar Data Framework - YouTube](https://www.youtube.com/watch?v=6EOFtzB3IPk)
- https://ys-l.github.io/posts/2015/08/28/how-not-to-use-pandas-apply/
- https://towardsdatascience.com/do-you-use-apply-in-pandas-there-is-a-600x-faster-way-d2497facfa66
- https://kanoki.org/2022/02/11/how-to-return-multiple-columns-using-pandas-apply/
- https://stackoverflow.com/questions/19578308/what-is-the-benefit-of-using-main-method-in-python
- https://modin.readthedocs.io/en/latest/
- https://datascience.stackexchange.com/questions/172/is-there-a-straightforward-way-to-run-pandas-dataframe-isin-in-parallel
- https://stackoverflow.com/questions/9759820/how-to-get-a-list-of-variables-in-specific-python-module
- http://shvbsle.in/computers-are-fast-but-you-dont-know-it-p1/
- https://pandas.pydata.org/docs/user_guide/enhancingperf.html

## **Books**
- [Fast Python High performance techniques for large datasets](https://www.manning.com/books/fast-python)
- [Designing Data-Intensive Applications The Big Ideas Behind Reliable, Scalable, and Maintainable Systems (Martin Kleppmann)](https://www.oreilly.com/library/view/designing-data-intensive-applications/9781491903063/)
- https://livebook.manning.com/book/high-performance-python-for-data-analytics/chapter-7/v-9/10