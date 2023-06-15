import main_functions
import process_data

import cProfile
import os
import pandas as pd

# access_json = main_functions.setup_ubi_nadeo()

# main_functions.update_campaigns_and_totds(access_json)

# main_functions.reset_user_accounts(access_json)
# main_functions.process_new_user_ids(access_json)

# main_functions.process_campaigns_per_user(access_json)
# main_functions.sort_files_into_folders()

# process_data.merge_user_records_to_map()
# process_data.clean_map_records()

process_data.summarize_individual_seasons()
process_data.summarize_all_seasons()

import shutil
filenames = [os.path.join(root, file) for root, dirs, files in os.walk('map_records\\') for file in files if 'summary' in file]

for file in filenames:
    shutil.copy2(file, 'C:\\Users\\abyss\\Desktop\\GitHub Projects\\TrackMania_Medal_Analysis\\TM Data Summary')