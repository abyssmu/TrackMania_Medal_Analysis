import campaigns
import maps
import setup
import totds
import users

import os
import pandas as pd
from datetime import datetime

ACCESS_JSON_FILENAME = 'access.json'
LOGIN_JSON = 'login.json'

MAPS_INFO_FOLDER = 'maps_info/'
CAMPAIGN_MAPS_CSV = MAPS_INFO_FOLDER + 'campaign_maps_name.csv'
TOTD_MAPS_CSV = MAPS_INFO_FOLDER + 'totd_maps_name.csv'

USER_RECORDS_FOLDER = 'user_records/'

ACCOUNT_INFO_FOLDER = 'account_info/'
ACCOUNT_IDS_CSV = ACCOUNT_INFO_FOLDER + 'account_ids.csv'
USER_ACCOUNT_CSV = ACCOUNT_INFO_FOLDER + 'user_account.csv'

USER_ACCOUNT_LENGTH = 1720371

##################################################
# This loops through every user and gets their campaign data from Nadeo.
# It then places that data into a csv in user_records for each user.
##################################################

def process_campaigns_per_user(access_json):
	process = True

	while(process):
		campaign_maps_df = pd.read_csv(CAMPAIGN_MAPS_CSV)
		user_df = pd.read_csv(USER_ACCOUNT_CSV)
		user_df = user_df.set_index('accountId')

		assert len(user_df.index) == USER_ACCOUNT_LENGTH, 'Size of user dataframe is different from ' + str(USER_ACCOUNT_LENGTH)

		mask = user_df['campaigns_are_finished'] == False
		filtered_by_completed = user_df[mask].copy()

		subset_num = 100000
		user_subset_df = filtered_by_completed.sample(n = subset_num).copy()

		campaign_maps_list = list(campaign_maps_df['mapId'])
		n = 219
		campaigns_split_by_n = [campaign_maps_list[i : i + n] for i in range(0, len(campaign_maps_list), n)]

		user_ids_list = list(user_subset_df.index)
		for i in range(len(user_ids_list)):
			user_map_records = maps.create_map_record_per_user_from_nested_list(access_json, campaigns_split_by_n, user_ids_list[i])

			if user_map_records == 'failed':
				print('failed')

				user_df.to_csv(USER_ACCOUNT_CSV)

				print('expiration_time_level_1: ', datetime.fromtimestamp(access_json['expiration_time_level_1']))
				print('expiration_time_level_2: ', datetime.fromtimestamp(access_json['expiration_time_level_2']))

				access_json = setup.setup_ubi_nadeo(ACCESS_JSON_FILENAME, LOGIN_JSON)

				print('expiration_time_level_1: ', datetime.fromtimestamp(access_json['expiration_time_level_1']))
				print('expiration_time_level_2: ', datetime.fromtimestamp(access_json['expiration_time_level_2']))

				break

			campaigns.create_user_campaigns_csv(user_subset_df[user_subset_df.index == user_ids_list[i]], campaign_maps_df, user_map_records, USER_RECORDS_FOLDER)

			user_df.at[user_ids_list[i], 'campaigns_are_finished'] = True

			i += 1

			if i % 5 == 0:
				print(i)
				user_df.to_csv(USER_ACCOUNT_CSV)

			if i == subset_num - 1:
				process = False

	user_df.to_csv(USER_ACCOUNT_CSV)

##################################################
# This processes any new user ids.
# To operate, delete all former ids in account_ids.csv in the account_info folder
# and paste all new ids in making sure to retain the header (account_id).
# Due to the limitations that Nadeo places on their servers for their API, the
# function takes approximately one second per 100 user ids.
##################################################

def process_new_user_ids(access_json):
	users.process_new_user_ids(access_json, ACCOUNT_IDS_CSV, USER_ACCOUNT_CSV)

##################################################
# This is in case something happens that messes up the user account csv.
# It resets all data in it to align with the PlayerBase.txt file containing
# the username and account id pairs.
##################################################

def reset_user_accounts(access_json):
	import json
	import os

	with open(ACCOUNT_INFO_FOLDER + 'PlayerBase.txt', 'r') as fd:
		text = json.load(fd)

	df = pd.DataFrame(list(text.items()), columns = ['displayName', 'accountId'])
	df['campaigns_are_finished'] = False
	df['totds_are_finished'] = False
	df = df.sort_values(by = 'displayName')
	df = df.drop_duplicates('accountId', keep = 'first')
	df = df.set_index('accountId')

	filenames = [os.path.join(root, name) \
					for root, dirs, files in os.walk(USER_RECORDS_FOLDER) \
						for name in files]
	end_str_len = len('-campaigns.csv')

	for i in range(len(filenames)):
		id = filenames[i][-(end_str_len + 36) : -end_str_len]
		df.at[id, 'campaigns_are_finished'] = True

	df.to_csv(USER_ACCOUNT_CSV)

##################################################
# This sets up the access json that is used across the entire pipeline.
##################################################

def setup_ubi_nadeo():
	return setup.setup_ubi_nadeo(ACCESS_JSON_FILENAME, LOGIN_JSON)

def sort_files_into_folders():
	main_folders = [dir for dir in os.listdir(USER_RECORDS_FOLDER)][1:]
	filenames = [file \
					for root, dirs, files in os.walk(USER_RECORDS_FOLDER) \
						for file in files][6:]

	by_first_letter = [[] for i in range(26)]

	for file in filenames:
		by_first_letter[ord(file[0].upper()) - ord('A')].append(file)

	i = 0
	for li in by_first_letter:
		for file in li:
			one_letter = file[0].upper()
			two_letter = file[:2].upper()

			if os.path.isfile(os.path.join(USER_RECORDS_FOLDER, one_letter, file)):
				os.rename(os.path.join(USER_RECORDS_FOLDER, one_letter, file), os.path.join(USER_RECORDS_FOLDER, one_letter, two_letter, file))

			i += 1
			if i % 1000 == 0:
				print(i)

##################################################
# These update the campaign and totd map csvs.
##################################################

def update_campaigns_and_totds(access_json):
	campaigns.update_campaign_maps(access_json, CAMPAIGN_MAPS_CSV)
	totds.update_totd_maps_csv(access_json, TOTD_MAPS_CSV)