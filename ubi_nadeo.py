import base64
import json
import jwt
import os
import requests

URL_REFRESH = 'https://prod.trackmania.core.nadeo.online/v2/authentication/token/refresh'
URL_REFRESH_LIVE = 'https://live-services.trackmania.nadeo.live/v2/authentication/token/refresh'
URL_LEVEL_0 = 'https://public-ubiservices.ubi.com/v3/profiles/sessions'
URL_LEVEL_1 = 'https://prod.trackmania.core.nadeo.online/v2/authentication/token/ubiservices'
URL_LEVEL_2 = 'https://prod.trackmania.core.nadeo.online/v2/authentication/token/nadeoservices'
URL_LEVEL_2_LIVE = 'https://live-services.trackmania.nadeo.live/v2/authentication/token/nadeoservices'

def get_access_json(access_json_filename):
	if os.path.isfile(access_json_filename):
		with open(access_json_filename, 'r') as fp:
			access_json = json.load(fp)
	else:
		return {}

	return access_json

def refresh_level_1(access_json):
	session = requests.Session()
	session.header.update({'Authorization' : 'nadeo_v1 t=' + access_json['refresh_token_level_1']})

	update_level_1_access_json(access_json, session.post(URL_REFRESH).json())

def refresh_level_2(access_json):
	session = requests.Session()
	session.header.update({'Authorization' : 'nadeo_v1 t=' + access_json['refresh_token_level_2']})
	body = {'audience' : 'NadeoServices'}

	update_level_2_access_json(access_json, session.post(URL_REFRESH, data = body).json())

def setup_level_0(email = None, password = None, access_json = None):
	if email is None or password is None or access_json is None: return 0

	session = requests.Session()
	session.headers.update({
		'Authorization' : "Basic " + base64.b64encode((email + ":" + password).encode('utf-8')).decode('utf-8'),
		'Ubi-AppId' : '86263886-327a-4328-ac69-527f0d20a237',
		'Content-Type' : 'application/json',
		'Ubi-RequestedPlatformType' : 'uplay',
		'User-Agent' : 'Medal Data Visualization: sarobin2@illinois.edu, Discord: Abyssmu#2072'
	})

	access_json['ticket'] = session.post(URL_LEVEL_0).json()['ticket']

def setup_level_1(access_json = None):
	if access_json is None: return 0

	session = requests.Session()
	session.headers.update({'Authorization' : "ubi_v1 t=" + access_json['ticket']})

	update_level_1_access_json(access_json, session.post(URL_LEVEL_1).json())

def setup_level_2(access_json = None):
	if access_json is None: return 0

	session = requests.Session()
	session.headers.update({'Authorization' : 'nadeo_v1 t=' + access_json['access_token_level_1']})
	body = {'audience' : 'NadeoLiveServices'}

	update_level_2_access_json(access_json, session.post(URL_LEVEL_2, data = body).json())

def store_level_access_as_json(access_json = None, access_json_filename = None):
	if access_json is None or access_json_filename is None: return 0

	with open(access_json_filename, 'w') as fp:
		json.dump(access_json, fp, indent = 4)

def update_level_1_access_json(access_json, response):
	access_json['access_token_level_1'] = response['accessToken']
	access_json['refresh_token_level_1'] = response['refreshToken']

	info = jwt.decode(access_json['access_token_level_1'], options = {"verify_signature": False})

	access_json['expiration_time_level_1'] = info['exp']
	access_json['refresh_time_level_1'] = info['rat']

def update_level_2_access_json(access_json, response):
	access_json['access_token_level_2'] = response['accessToken']
	access_json['refresh_token_level_2'] = response['refreshToken']

	info = jwt.decode(access_json['access_token_level_2'], options = {"verify_signature": False})

	access_json['expiration_time_level_2'] = info['exp']
	access_json['refresh_time_level_2'] = info['rat']