import maps

import requests

def create_user_campaigns_csv(user, campaign_maps_df, user_map_records, user_records_folder):
	campaign_maps_df['personal_best'] = 10e8
	campaign_maps_df['personal_best'] = campaign_maps_df['personal_best'].astype(int)

	for map_record in user_map_records:
		mask = campaign_maps_df['mapId'] == map_record['mapId']
		index = campaign_maps_df[mask].index[0]
		campaign_maps_df.at[index, 'personal_best'] = int(map_record['time'])

	csv_filename = user_records_folder + user['displayName'].values[0] + '-' + user.index[0] + '-campaigns.csv'

	campaign_maps_df.to_csv(csv_filename, index = False)

def get_all_campaigns_as_json(access_json):
	length = 100
	offset = 0
	url_campaign = f'https://live-services.trackmania.nadeo.live/api/token/campaign/official?length={length}&offset={offset}'

	session = requests.Session()
	session.headers.update({'Authorization' : 'nadeo_v1 t=' + access_json['access_token_level_2']})

	return session.get(url_campaign).json()

def update_campaign_maps(access_json, CAMPAIGN_MAPS_CSV):
	campaigns_json = get_all_campaigns_as_json(access_json)
	campaign_maps_uids = maps.extract_map_uid(campaigns_json)

	n = 100
	campaign_split_by_n = [campaign_maps_uids[i : i + n] for i in range(0, len(campaign_maps_uids), n)]

	maps.create_map_id_lookup_table(access_json, campaign_split_by_n, campaign_map_csv = CAMPAIGN_MAPS_CSV)