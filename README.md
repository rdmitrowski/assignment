  Application joins two datasets loaded from csv files (clients data and transactions data), files are stored in input_data directory.
  Additional operation like data filtering, column calculating and renaming are preparing on dataset and as a output file in client_data directory is saveed.

  usage: 
  python \__main__.py -u FILE_USERS -t FILE_TRANSACTIONS -f FILTER [-out FILE_OUTPUT]
  
  FILE_USERS        - users fileneme located in input_data directory
  FILE_TRANSACTIONS - transactions fileneme located in input_data directory
  FILTER            - filter data for specific country
  FILE_OUTPUT       - optional parameter with output filename located in client_data directory
                       if not added default filename is:
                       output_data_~date~.csv 
                                   where ~date~  dd-mm-yyyy_hh24miss
