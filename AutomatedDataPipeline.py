
from glob import glob
import os


import pandas as pd
import numpy as np



#test_filepath = [r'Q:\Department\QC\Finished Product Data\Sigma']

def list_files_in_directory(filepath):
    FilesInDirectories=[]
    for i in range(len(filepath)):
        FilesInDirectories= FilesInDirectories+[y for x in os.walk(filepath[i]) for y in glob(os.path.join(x[0], '*.xls'))]
        FilesInDirectories= FilesInDirectories+[y for x in os.walk(filepath[i]) for y in glob(os.path.join(x[0], '*.xlsx'))]
        FilesInDirectories= FilesInDirectories+[y for x in os.walk(filepath[i]) for y in glob(os.path.join(x[0], '*.xlsm'))]
    return FilesInDirectories


if __name__ == '__main__':
    conf = pd.read_csv("./config.csv")
    tasks = conf[conf["Automate"] == "Yes"]

    for i in range(len(tasks)):

        FilesInSourceDirectory = list_files_in_directory([x.strip() for x in tasks["SourcePath"].iloc[i].split(";")])
        try:
            AlreadyProcessedFiles = [i for i in [x for y in pd.read_csv(tasks["LogFile"].iloc[i]).values.tolist() for x in y] if str(i) != 'nan']
        except:
            AlreadyProcessedFiles=[]
        PathsToProcess = [x for x in FilesInSourceDirectory if os.path.split(x)[1] not in AlreadyProcessedFiles]

        Files_Argument = ""

        #split the file list into chunks as only 20 or so files can be submitted through the commmand line
        #to avoid problems with long filenames the chunk size will be limited to 10
        n = 10
        PathChunks = [PathsToProcess[i:i+n] for i in range(0, len(PathsToProcess), n)]

        for chunk in PathChunks:
            Files_Argument = ""
            for j in (chunk):
                Files_Argument = Files_Argument+" \""+j+"\""
            os.system("./venv/scripts/python.exe AnalyticalDataReaderCommandLine.py -p {} -f {} ".format(tasks["Product"].iloc[i], Files_Argument))
