import pandas as pd
import requests
import json
import pickle


def get_key_value(obj, key, default=''):
    return obj[key] if key in obj else default


def get_bank_name(value, default=None):
    bank_list = ['Газпромбанк', 'Альфа-Банк', 'ВТБ']
    bank = default

    for item in bank_list:
        if item in value:
            bank = item

    return bank


def is_valid_bank(obj):
    is_bank = False
    rubrics = obj['rubrics'] if 'rubrics' in obj else []

    for item in rubrics:
        is_bank = item['name'] == 'Банкоматы'

    return is_bank


class AtmDownloader:
    def __init__(self, urls=None):
        if urls is None:
            urls = []
        self.urls = urls
        self.atm_id_history = {}
        self.banks = {}
        self.parking = {}
        self.bus_stations = {}
        self.df_data = []
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.96 YaBrowser/20.4.0.1461 Yowser/2.5 Safari/537.36',
        }

    def atm_retrieval(self, url):
        answer = requests.get(url, headers=self.headers)
        return json.loads(answer.content.decode('utf-8'))

    def download_atm(self):
        cnt_atm = 0
        cnt_errors = 0
        print('количество url: ' + str(len(self.urls)))

        for url in self.urls:
            cnt_atm += 1
            if cnt_atm % 10 == 0:
                print(cnt_atm)
            try:
                res = self.atm_retrieval(url)
                res = res['result']['items'][0]
            except:
                cnt_errors += 1
                print(str(cnt_errors) + "/" + str(cnt_atm))
                continue

            if res['id'] not in self.atm_id_history.keys():
                if not is_valid_bank(res):
                    print(f"{res['name']} is not valid bank")
                    continue

                gis_id = res['id']
                bank_name = get_bank_name(res['name'])

                obj = {
                    'gis_id': gis_id,
                    'name': res['name'],
                    'bank': bank_name,
                    'address': get_key_value(res, 'address_name'),
                    'lat': res['point']['lat'],
                    'lon': res['point']['lon'],
                    'schedule': get_key_value(res, 'schedule'),
                }
                self.df_data.append(obj)
                self.atm_id_history[gis_id] = res['name']
                self.parking[gis_id] = res['links']['nearest_parking']
                self.bus_stations[gis_id] = res['links']['nearest_stations']
                self.banks[gis_id] = bank_name

    def save_banks_to_file(self, name='data/banks.xlsx'):
        unique_banks = []

        for item in self.banks:
            if self.banks[item] not in unique_banks:
                unique_banks.append(self.banks[item])

        banks_df = pd.DataFrame(unique_banks)
        banks_df.to_excel(name, index=False)

    def save_atm_parking_to_file(self, name='data/atm_parking.dictionary'):
        unique_parking = []
        json_atm_parking = self.parking

        for item in json_atm_parking:
            for parking in json_atm_parking[item]:
                if parking['id'] not in unique_parking:
                    unique_parking.append(parking['id'])

        with open(name, 'wb') as config_dictionary_file:
            pickle.dump(json_atm_parking, config_dictionary_file)

        parking_df = pd.DataFrame(unique_parking)
        parking_df.to_excel('data/parking.xlsx', index=False)

    def save_atm_bus_stations_to_file(self, name='data/atm_bus_stations.dictionary'):
        unique_stations = []
        stations_obj = []
        json_atm_stations = self.bus_stations

        for item in json_atm_stations:
            for station in json_atm_stations[item]:
                if station['id'] not in unique_stations:
                    unique_stations.append(station['id'])

                    obj = {
                        'gis_id': station['id'],
                        'name': station['name'],
                        'route_types': station['route_types']
                    }
                    stations_obj.append(obj)

        with open(name, 'wb') as config_dictionary_file:
            pickle.dump(json_atm_stations, config_dictionary_file)

        parking_df = pd.DataFrame(unique_stations)
        parking_df.to_excel('data/bus_stations.xlsx', index=False)

    def save_to_file(self, name='data/atm.xlsx'):
        df = pd.DataFrame(self.df_data)
        df.to_excel(name, index=False)
