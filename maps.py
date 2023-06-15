import pandas as pd
import requests
import time

DELAY_TIME = 0.5

def create_map_id_lookup_table(access_json = None,
								map_uids_list = None,
								campaign_map_csv = None,
								totd_map_csv = None):
	maps_data_list = []

	for maps in map_uids_list:
		maps_data = get_map_data(access_json, maps)

		for map in maps_data:
			maps_data_list.append([map['name'],
								map['filename'],
								map['mapId'],
								map['mapUid'],
								map['bronzeScore'],
								map['silverScore'],
								map['goldScore'],
								map['authorScore']])

		time.sleep(DELAY_TIME)

	df = pd.DataFrame(maps_data_list)
	df.columns = ['name',
				'filename',
				'mapId',
				'mapUid',
				'bronzeScore',
				'silverScore',
				'goldScore',
				'authorScore']

	if campaign_map_csv is not None:
		temp = pd.DataFrame([[s[0].split()[0], s[0].split()[1], s[1].strip()] for s in df['name'].str.split('-')])
		temp.columns = ['season', 'year', 'number']

		df = pd.concat([temp, df], axis = 1)
		df = df.drop(['name', 'filename'], axis = 1)

		df['season'] = ['0_Winter' if s == 'Winter' \
			else '1_Spring' if s == 'Spring' \
			else '2_Summer' if s == 'Summer' \
			else '3_Fall' \
			for s in df['season']]
		df = df.sort_values(by = ['year', 'season', 'number'])

		df.to_csv(campaign_map_csv, index = False)

	if totd_map_csv is not None:
		names = list(df['name'])
		controls = ['$o', '$i', '$w',
					'$n', '$t', '$s',
					'$L', '$g', '$z',
					'$$']

		for i in range(len(names)):
			for c in controls:
				while c in names[i]:
					names[i].replace(c, '')

		df['name'] = names
		df.to_csv(totd_map_csv, index = False)

def create_map_record_per_user_from_nested_list(access_json, maps_id_list, user_id):
	user_record_data_per_map = []

	for map_group in maps_id_list:
		group_records_data = get_map_records_by_user_id(access_json, user_id, map_group)

		for map_record_data in group_records_data:
			try:
				desired_data = {'mapId' : map_record_data['mapId'],
								'time' : map_record_data['recordScore']['time']}
			except:
				print(group_records_data)
				return 'failed'

			user_record_data_per_map.append(desired_data)

	return user_record_data_per_map

def extract_map_uid(map_data_list):
	map_ids = []

	if 'campaignList' in map_data_list.keys():
		for campaign in map_data_list['campaignList']:
			for map in campaign['playlist']:
				map_ids.append(map['mapUid'])

	elif 'monthList' in map_data_list.keys():
		for month in map_data_list['monthList']:
			for map in month['days']:
				map_ids.append(map['mapUid'])

	return map_ids

def get_map_data(access_json, map_uid_list):
	map_uids_str = ','.join(map_uid_list)
	url_map = f'https://prod.trackmania.core.nadeo.online/maps/?mapUidList={map_uids_str}'

	session = requests.Session()
	session.headers.update({'Authorization' : 'nadeo_v1 t=' + access_json['access_token_level_1']})

	return session.get(url_map).json()

def get_map_records_by_user_id(access_json, user_id, map_id_list):
	map_ids_str = ','.join(map_id_list)
	url_map_records = f'https://prod.trackmania.core.nadeo.online/mapRecords/?accountIdList={user_id}&mapIdList={map_ids_str}'

	session = requests.Session()
	session.headers.update({'Authorization' : 'nadeo_v1 t=' + access_json['access_token_level_1']})

	return session.get(url_map_records).json()