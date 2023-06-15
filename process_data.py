import os
import pandas as pd

MAP_RECORDS_FOLDER = 'map_records\\'
USER_RECORDS_FOLDER = 'user_records\\'

USER_ACCOUNT_LENGTH = 1720371

def clean_map_records():
	map_filenames = [os.path.join(root, name) \
						for root, dirs, files in os.walk(MAP_RECORDS_FOLDER) \
							for name in files]

	for map in map_filenames:
		df = pd.read_csv(map).reset_index()

		for col in ['year', 'season', 'number']:
			df[col] = df[col].astype(str)

		li = list(df.columns[1:]) + list(df.columns[:1])
		df.columns = [li[i] if li[i] != 'index' else 'accountId' for i in range(len(df.columns))]

		df['mapName'] = df['year'].astype(str) + '-' + df['season'] + '-' + df['number'].astype(str)
		df = df[df['userScore'] < 10e8]

		df['obtained_bronze'] = df['userScore'] < df['bronzeScore']
		df['obtained_silver'] = df['userScore'] < df['silverScore']
		df['obtained_gold'] = df['userScore'] < df['goldScore']
		df['obtained_author'] = df['userScore'] < df['authorScore']

		df = df.drop(['mapId', 'mapUid', 'year', 'season', 'number'], axis = 1)

		df.to_csv(map, index = False)
		print(map)

def record_user_records_to_map(map_records, user_filenames):
	while(len(user_filenames) > 0):
		if len(user_filenames) % 10000 == 0:
			print('len(user_filenames):', len(user_filenames))

		data = open(user_filenames[0], 'r').readlines()[1:]

		for i in range(len(map_records)):
			try:
				map_records[i].append(data[i][:-1] + ',' + user_filenames[0][-(len('-campaigns.csv') + 36) : -len('-campaigns.csv')] + '\n')
			except:
				print(user_filenames[0])
				break

		user_filenames.pop(0)

def save_maps(map_records):
	while(len(map_records) > 0):
		li = map_records[0][1].split(',')
		filename = MAP_RECORDS_FOLDER + li[1] + '-' + li[0] + '/' + li[1] + '-' + li[0] + '-' + li[2] + '.csv'
		print(filename)

		open(filename, 'w').writelines(map_records[0])
		map_records.pop(0)

def summarize_all_seasons():
	folders = [os.path.join(root, dir) \
				for root, dirs, files in os.walk(MAP_RECORDS_FOLDER) \
					for dir in dirs]
	cols = ['name',
			'season',
			'finishedPopulation',
			'bronzeMedalCount',
			'silverMedalCount',
			'goldMedalCount',
			'authorMedalCount']

	total_info_df = pd.DataFrame(columns = cols)

	for folder in folders:
		season_df = pd.DataFrame(columns = 'bronzeScore,silverScore,goldScore,authorScore,userScore,accountId,mapName,obtained_bronze,obtained_silver,obtained_gold,obtained_author'.split(','))
		files = [os.path.join(root, file) \
					for root, dirs, files in os.walk(folder) \
						for file in files \
							if 'summary' not in file]

		bronze_medal_count = 0
		silver_medal_count = 0
		gold_medal_count = 0
		author_medal_count = 0

		for file in files:
			df = pd.read_csv(file)
			season_df = season_df.append(df)

			bronze_medal_count += df['obtained_bronze'].sum()
			silver_medal_count += df['obtained_silver'].sum()
			gold_medal_count += df['obtained_gold'].sum()
			author_medal_count += df['obtained_author'].sum()
		
		season_df = season_df.drop_duplicates(subset = 'accountId', keep = 'first')

		season_name = list(season_df['mapName'])[0][:-2]
		finished_population = len(season_df.index)

		season_data_df = pd.DataFrame([[season_name,
										'full_season_summary',
										finished_population,
										bronze_medal_count,
										silver_medal_count,
										gold_medal_count,
										author_medal_count]],
										columns = cols)
		total_info_df = total_info_df.append(season_data_df)

	total_info_df.to_csv(MAP_RECORDS_FOLDER + 'full_season_summary.csv', index = False)

def summarize_individual_seasons():
	folders = [os.path.join(root, dir) \
				for root, dirs, files in os.walk(MAP_RECORDS_FOLDER) \
					for dir in dirs]
	cols = ['name',
			'season',
			'number',
			'finishedPopulation',
			'bronzeMedalCount',
			'silverMedalCount',
			'goldMedalCount',
			'authorMedalCount']

	for folder in folders:
		season_df = pd.DataFrame(columns = cols)
		files = [os.path.join(root, file) \
					for root, dirs, files in os.walk(folder) \
						for file in files \
							if 'summary' not in file]

		for file in files:
			df = pd.read_csv(file)

			map_name = list(df['mapName'])[0].split('-')[-1]
			season = folder.split('\\')[1]
			number = map_name.split('-')[-1]
			finished_population = len(df.index)
			bronze_medal_count = df['obtained_bronze'].sum()
			silver_medal_count = df['obtained_silver'].sum()
			gold_medal_count = df['obtained_gold'].sum()
			author_medal_count = df['obtained_author'].sum()

			map_data_df = pd.DataFrame([[map_name,
											season,
											number,
											finished_population,
											bronze_medal_count,
											silver_medal_count,
											gold_medal_count,
											author_medal_count]],
											columns = cols)
			season_df = season_df.append(map_data_df)

		season_df['number'] = season_df['number'].astype(int)
		season_df = season_df.sort_values('number').drop(columns = ['number'])

		filename = folder.split('\\')[1] + '-summary.csv'
		season_df.to_csv(os.path.join(folder, filename), index = False)

def merge_user_records_to_map():
	columns = ','.join(['season',
						'year',
						'number',
						'mapId',
						'mapUid',
						'bronzeScore',
						'silverScore',
						'goldScore',
						'authorScore',
						'userScore'])

	print('buidling map list')
	map_records = [[columns + '\n'] \
						for i in range(len([os.path.join(root, name) \
							for root, dirs, files in os.walk(MAP_RECORDS_FOLDER) \
								for name in files]))]

	print('recording user records to maps')
	record_user_records_to_map(map_records, [os.path.join(root, name) \
												for root, dirs, files in os.walk(USER_RECORDS_FOLDER) \
													for name in files])

	print('saving maps')
	save_maps(map_records)

	print('end')