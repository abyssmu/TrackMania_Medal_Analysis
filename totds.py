import maps

import requests

def get_all_totds_as_json(access_json):
	length = 100
	offset = 0
	url_totd = f'https://live-services.trackmania.nadeo.live/api/token/campaign/month?length={length}&offset={offset}'

	session = requests.Session()
	session.headers.update({'Authorization' : 'nadeo_v1 t=' + access_json['access_token_level_2']})

	return session.get(url_totd).json()

def update_totd_maps_csv(access_json, TOTD_MAPS_CSV):
	totds_json = get_all_totds_as_json(access_json)
	totd_maps_uids = maps.extract_map_uid(totds_json)

	n = 100
	totd_split_by_n = [totd_maps_uids[i : i + n] for i in range(0, len(totd_maps_uids), n)]

	maps.create_map_id_lookup_table(access_json, totd_split_by_n, totd_map_csv = TOTD_MAPS_CSV)