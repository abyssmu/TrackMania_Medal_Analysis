import login
import ubi_nadeo

from datetime import datetime

JSON_KEYS = ['ticket',
		'access_token_level_1',
		'access_token_level_2',
		'refresh_token_level_1',
		'expiration_time_level_1',
		'refresh_time_level_1',
		'refresh_token_level_2',
		'expiration_time_level_2',
		'refresh_time_level_2']

def setup_ubi_nadeo(access_json_filename, login_json_filename):
	login_info = login.load_login_json(login_json_filename)
	access_json = ubi_nadeo.get_access_json(access_json_filename)
	get_json = False

	if access_json == {}: get_json = True
	else:
		for i in range(len(JSON_KEYS)):
			if list(access_json.keys())[i] != JSON_KEYS[i] and \
				access_json[JSON_KEYS[i]] != '':
				get_json = True
				break

	if get_json:
		ubi_nadeo.setup_level_0(login_info['email'], login_info['password'], access_json)
		ubi_nadeo.setup_level_1(access_json)
		ubi_nadeo.setup_level_2(access_json)
	else:
		is_expired_level_1 = datetime.now() > datetime.utcfromtimestamp(access_json['expiration_time_level_1'])
		is_expired_level_2 = datetime.now() > datetime.utcfromtimestamp(access_json['expiration_time_level_2'])

		if is_expired_level_1:
			ubi_nadeo.refresh_level_1(access_json)

		if is_expired_level_2:
			ubi_nadeo.refresh_level_2(access_json)

	ubi_nadeo.store_level_access_as_json(access_json, access_json_filename)

	return access_json