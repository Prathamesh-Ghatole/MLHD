# Cleaning the Music Listening Histories Dataset

## **Quickstart**
### 1. **What is MLHD?**
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
6. To setup required folders and fetch tables, run:
    - ```python config.py```
7. Ready to go