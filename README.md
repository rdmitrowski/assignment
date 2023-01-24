  Application joins two datasets loaded from files, prepare filtering based on input parameter and save limited information in output file.

  usage: python3 BitcoinTrading.py <users_input> <transactions_input> <country> 
  or 
  usage: python3 BitcoinTrading.py <users_input> <transactions_input> <country> <output_file>
  
  <users_input>        - users fileneme located in input_data directory
  <transactions_input> - transactions fileneme located in input_data directory
  <country>            - filter data for specific country
  <output_file>        - optional parameter with output filename located in client_data directory
                       if not added default filename is:
                       output_data_~date~.csv 
                                   where ~date~  dd-mm-yyyy_hh24miss