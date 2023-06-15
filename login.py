import json

def load_login_json(logon_json):
	with open(logon_json, 'r') as fp:
		login = json.load(fp)

	return login