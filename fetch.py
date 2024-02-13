#! /usr/bin/env python3
'''
Program for adding realtime X-ray data to postgresql.

Copyright 2024 Finnish Meteorological Institute

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the “Software”), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.


Aurthor(s): Ilja Honkonen
'''

import argparse
from datetime import datetime
from json import loads
import os

try:
	import psycopg2
except:
	print("Couldn't import psycopg2, try pip3 install --user psycopg2")
	exit(1)
try:
	import requests
except:
	print("Couldn't import requests, try pip3 install --user requests")
	exit(1)

parser = argparse.ArgumentParser(
	description = 'Fetches X-ray data into postgresql.',
	formatter_class = argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument(
	'--db-name',
	default = 'test',
	metavar = 'N',
	help = 'Operate on database named N.')
parser.add_argument(
	'--db-user',
	default = 'test',
	metavar = 'U',
	help = 'Operate on database as user U.')
parser.add_argument(
	'--db-password-env',
	default = 'XRAYPW',
	metavar = 'S',
	help = 'Use password from env var S for database connection.')
parser.add_argument(
	'--db-host',
	default = 'localhost',
	metavar = 'H',
	help = 'Operate on database at address H.')
parser.add_argument(
	'--db-port',
	type = int,
	default = 5432,
	metavar = 'P',
	help = 'Operate on database at port P.')
parser.add_argument(
	'--table',
	default = 'test',
	metavar = 'T',
	help = 'Use table T in database N.')
parser.add_argument(
	'--url',
	default = 'https://services.swpc.noaa.gov/json/goes/primary/xrays-6-hour.json,https://services.swpc.noaa.gov/json/goes/secondary/xrays-6-hour.json',
	help = 'Comma-separated list of URLs for downloading new data.')

args = parser.parse_args()
args.url = args.url.split(',')


if not args.db_password_env in os.environ:
	print('Environment variable for db password', args.db_password_env, "doesn't exist")
	exit(1)

try:
	connection = psycopg2.connect(
		dbname = args.db_name,
		user = args.db_user,
		password = os.environ[args.db_password_env],
		host = args.db_host,
		port = args.db_port)
except Exception as e:
	print("Couldn't connect to database: ", e)
	exit(1)

texts = []
for url in args.url:
	text = None
	try:
		text = requests.get(url).text
		# fix common problems
		if text.endswith(', {"'):
			text = text[:-4] + ']'
		elif text.endswith('m"'):
			text += '}]'
		elif text.endswith('"},'):
			text = text[:-1] + ']'
		elif text.endswith('"}'):
			text += ']'
	except Exception as e:
		print("Couldn't download data from " + url + ':', e)
	finally:
		texts.append(text)

ok = False
for i in range(len(texts)):
	if texts[i] == None or len(texts[i]) == 0:
		print('No data from download', args.url[i])
	else:
		ok = True
if not ok:
	print('No data from downloads.')
	exit()

jsons = []
for i in range(len(texts)):
	json = None
	try:
		if texts[i] != None:
			json = loads(texts[i])
	except Exception as e:
		print("Couldn't interpret JSON data from", args.url[i], ': ', e)
	finally:
		jsons.append(json)

ok = False
for i in range(len(jsons)):
	if jsons[i] == None:
		print('No JSON data from', args.url[i])
	elif not 'time_tag' in jsons[i][0]:
		print('No time tag in first item from', args.url[i], ':  ', jsons[i][0])
	else:
		ok = True

if not ok:
	print('No json data.')
	exit(1)

data = []
satellites = set()
energies = set()
for json in jsons:
	for item in json:
		satellites.add(item['satellite'])
		energies.add(item['energy'])
		item['corrected_flux'] = item['flux']
		item.pop('flux', None)
		data.append(item)


cursor = connection.cursor()

# possibly create db
cursor.execute('create table if not exists ' + args.table + ' (datetime varchar, satellite int, energy varchar, corrected_flux real, observed_flux real, electron_correction real, primary key (datetime, satellite, energy))')
connection.commit()

# TODO: make sure that correct columns exist

# exclude old data
latest_data = dict()
for sat in satellites:
	if not sat in latest_data:
		latest_data[sat] = dict()
	for nrj in energies:
		cursor.execute('select max(datetime) from ' + args.table + ' where satellite = %s and energy = %s', [sat, nrj])
		result = cursor.fetchone()[0]
		if result == None:
			latest_data[sat][nrj] = datetime.strptime('1900-01-01T00:00:00Z', '%Y-%m-%dT%H:%M:%S%z')
		else:
			latest_data[sat][nrj] = datetime.strptime(result, '%Y-%m-%dT%H:%M:%S%z')

new_data = []
for d in data:
	if datetime.strptime(d['time_tag'], '%Y-%m-%dT%H:%M:%S%z') > latest_data[d['satellite']][d['energy']]:
		new_data.append(d)

try:
	cursor.execute('begin transaction')
	cursor.execute('lock table ' + args.table + ' in exclusive mode nowait')
except Exception as e:
	print('Someone already writing to table', args.table, 'of database', args.db_name)
	exit()

inserted = 0
for d in new_data:
	cursor.execute('insert into ' + args.table + ' (datetime, satellite, energy, corrected_flux, observed_flux, electron_correction) values (%s, %s, %s, %s, %s, %s) on conflict (datetime, satellite, energy) do nothing', (d['time_tag'], d['satellite'], d['energy'], d['corrected_flux'], d['observed_flux'], d['electron_correction']))
	inserted += 1
connection.commit()
cursor.close()
connection.close()
print(inserted, 'new values added to database')
