# -*- coding: utf-8 -*-
"""
Created on Wed Feb  3 16:05:43 2021

@author: dwosiek
"""

# -*- coding: utf-8 -*-
"""
Simple Pandas program to combine Excel files and summarize data.

"""

from gooey import Gooey, GooeyParser
import xlwings as xw
import pandas as pd
import numpy as np
import os
import time
import sys
import argparse


#nonbuffered_stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)
#sys.stdout = nonbuffered_stdout


@Gooey(required_cols=1, program_name="Analytical Data Collector", progress_regex=r"^Progress: (?P<current>\d+)/(?P<total>\d+)$",
       progress_expr="current / total * 100")
def parse_args(products,default_option):
    """ Use GooeyParser to build up the arguments for the script.
    """

  
    settings_msg: str =      'Read in Files'
  
  
    parser = GooeyParser(description=settings_msg)

    
    parser.add_argument( "Product", choices=products,default=default_option,action='store',
                        help='Choose the product category'
                        )
    

    
    parser.add_argument('Files',
                        action='store',
                        help='Select the files to merge',
                        nargs="+",
       #                 default=stored_args.get('data_directory'),
                        widget="MultiFileChooser",
                            
                            )

    
     
     
    args = parser.parse_args()
    
    """
    # Store the values of the arguments so we have them next time we run
    with open(args_file, 'w') as data_file:
        # Using vars(args) returns the data as a dictionary
        json.dump(vars(args), data_file)
        """
    return args


def load_inputoutput_files(path_to_log_file, tail_file, path_to_dimension_file, path_to_fact_file):
    try:
        already_processed_files=pd.read_csv(path_to_log_file)
        
    except:
        already_processed_files=pd.DataFrame(columns=["Processed Files", "Failed Files"])
    
    if (tail_file in already_processed_files["Processed Files"].values or tail_file in already_processed_files["Failed Files"].values):
        skip=True
    else:
        skip=False

    try:
        dimension_df = pd.read_csv(path_to_dimension_file, dtype=object)
    except:
        dimension_df=pd.DataFrame()

    try:
        fact_df = pd.read_csv(path_to_fact_file, dtype=object)
    except:
        fact_df=pd.DataFrame()
    
    return (skip,dimension_df, fact_df, already_processed_files)

    
def read_in_files(wb_index,template_file_path, file_path, tail_index, tail_file, dimensions_results, facts_results):

    flag_dimensions,flag_facts = False, False

    try:
        wb_source = xw.books[tail_file]
    except (KeyError, AttributeError) as e:      
        wb_source = xw.Book(file_path, update_links=False, read_only=True)
    wb_index.sheets("Index").range("A1").value=tail_file
    ### wait until formulas are updated in excel spreadsheets 
    time.sleep(1)
    #print(wb_index.sheets("Index").range("B3").value)
    #print(wb_index.sheets("Index").range("C3").value)

    if (wb_index.sheets("Index").range("B3").value==True):
        ###Process dimensions 
        temp_dimensions_df = wb_index.sheets("Dimensions").range("A1").options(pd.DataFrame,
                                                                     header=1,
                                                                     index=False,
                                                                     expand='table').value

        temp_dimensions_df = temp_dimensions_df[
        temp_dimensions_df[wb_index.sheets("Index").range("B4").value].astype(bool)]  ### deletes empty rows
        dimensions_results = dimensions_results.append(temp_dimensions_df, ignore_index=True)  ###append new data on loaded dataframe
        dimensions_results = dimensions_results.drop_duplicates(subset=[wb_index.sheets("Index").range("B5").value])  ###eliminate potential duplicate rows based on specific column with primary key
        flag_dimensions = True

    if (wb_index.sheets("Index").range("C3").value == True):
    ###Process facts
        temp_facts_df = wb_index.sheets("Facts").range("A1").options(pd.DataFrame,
                                 header=1,
                                 index=False,
                                 expand='table').value
        #print(temp_facts_df.tail())
        temp_facts_df = temp_facts_df[temp_facts_df[wb_index.sheets("Index").range("C4").value].astype(bool)] ### deletes empty rows
        facts_results = facts_results.append(temp_facts_df,ignore_index=True)    ###append new data on loaded dataframe
        facts_results = facts_results.drop_duplicates(subset=[wb_index.sheets("Index").range("C5").value])    ###eliminate potential duplicate rows based on specific column with primary key
        flag_facts = True




    wb_source.close()
    return (flag_dimensions & flag_facts, dimensions_results, facts_results)



if __name__ == '__main__':

    #app = xw.App()
    #First load basic config data from csv
    ## Structure:     Command	TrendTemplate	LogFile	OutputFileCSV	OutputFileXLSX
    conf = pd.read_csv("./config.csv")

    #Next start the GUI
    args = parse_args(conf.Product.to_list(), conf.iloc[0]["Default"])
    total = len(args.Files)

    #Now open up the intermediate data template dependent on the parsed command
    template_file_path = conf[conf["Product"] == args.Product].iloc[0]["TrendTemplate"] #get the path of the trend template
    head_index, tail_index = os.path.split(template_file_path)    #split the path into folder and file name
    
    path_to_log_file = conf[conf["Product"] == args.Product].iloc[0]["LogFile"]
    path_to_dimension_file = conf[conf["Product"] == args.Product].iloc[0]["DimensionsFileCSV"]
    path_to_fact_file = conf[conf["Product"] == args.Product].iloc[0]["FactsFileCSV"]
    
    
    # Open the template file
    ### workaround to open files stored in a OneDrive or SharePoint directory
    try:
        wb_index = xw.books[tail_index]
    
    except (KeyError, AttributeError) as e:       # Handles instance when no excel files are open and the xw.books attribute does not exist
        wb_index = xw.Book(template_file_path, update_links=False, read_only=True)
   

    ##### main loop to open and merge files############
    current = 0
    #print(args.Files)
    for file_path in args.Files:
        current += 1
        print("Progress: {}/{}".format(current,total))
       
        head_file, tail_file = os.path.split(file_path)    #splits the filepath into parent folder and file
      
        print("Chosen file: {}".format(tail_file))
        #print("")
        sys.stdout.flush()


        if ((os.path.splitext(tail_file)[-1] ==".xls") or (os.path.splitext(tail_file)[-1] ==".xlsx") or (os.path.splitext(tail_file)[-1] ==".xlsm")):
                                            ##(path_to_log_file, tail_file, path_to_dimension_file, path_to_fact_file)
            skip, dimension_df, fact_df, files_df= load_inputoutput_files(path_to_log_file, tail_file, path_to_dimension_file, path_to_fact_file)
        else:
            print("Not a valid file, skipping")
            skip = True

        #print ("Skip File?: {}".format(skip))
       # print("")
        if not skip:
                                            #wb_index,template_file_path, file_path, tail_index, tail_file, dimensions_results, facts_results
            fileprocessed, dimensions_results, facts_results = read_in_files(wb_index,template_file_path,file_path,tail_index,tail_file,dimension_df, fact_df)
            print("File was processed?: {}".format(fileprocessed))
            
            if (fileprocessed): 
                files_df = files_df.append(pd.DataFrame([[tail_file, np.nan]], columns=["Processed Files", "Failed Files"]),ignore_index=False)
                #print(dimensions_results)
                dimensions_results.to_csv(path_to_dimension_file, index=False)
                facts_results.to_csv(path_to_fact_file, index=False)
               

                                     
            else:
                files_df = files_df.append(pd.DataFrame([[np.nan,tail_file]],columns=["Processed Files", "Failed Files"]),ignore_index=False)
            files_df.to_csv(path_to_log_file, index=False)
            
         
            
    wb_index.app.quit()
    #app.quit()
    

    
    #file_path=r"C:\Users\dwosiek\Werfen\ILSD TEM R&D - General\TEM_2020-11-11_Manufacturing and QC support projects\TEM_2020-11-11.1\ROTROLtargetRange\Rotrol N\2020\Control Ranges sigma ROTROL N 22757030 Charge_Lyo_22820060 _Dil_22867056_2020-12.xlsx"
