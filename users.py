import pandas as pd
import requests
import time

DELAY_TIME = 0.5

def get_username_from_id(access_json, user_id_list):
	user_ids_str = ''

	if len(user_id_list) == 1: user_ids_str = user_id_list[0]
	else: user_ids_str = ','.join(user_id_list)

	url_username = f'https://prod.trackmania.core.nadeo.online/accounts/displayNames/?accountIdList={user_ids_str}'

	session = requests.Session()
	session.headers.update({'Authorization' : 'nadeo_v1 t=' + access_json['access_token_level_1']})

	return session.get(url_username).json()

def process_new_user_ids(access_json, account_ids_csv, user_account_csv):
	user_ids_list = list(pd.read_csv(account_ids_csv)['account_id'])

	n = 50
	users_split_by_n = [user_ids_list[i : i + n] for i in range(0, len(user_ids_list), n)]

	user_df = pd.DataFrame()

	for n_users in users_split_by_n:
		user_list = get_username_from_id(access_json, n_users)

		for user in user_list:
			df = pd.DataFrame([user])
			df = df.drop('timestamp', axis = 1)

			user_df = pd.concat([user_df, df])

		time.sleep(DELAY_TIME)

	user_df['campaigns_are_finished'] = False
	user_df['totds_are_finished'] = False

	user_df = user_df.reset_index().drop('index', axis = 1)
	user_df.to_csv(user_account_csv, index = False)