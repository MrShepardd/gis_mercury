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
    rubrics = obj['categories'] if 'categories' in obj else []

    for item in rubrics:
        is_bank = item['name'] == 'Банкомат'

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
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.83 YaBrowser/20.9.0.933 Yowser/2.5 Safari/537.36',
            'cookie': 'maps_los=0; yandexuid=5517453851594445059; mda=0; ymex=1909805063.yrts.1594445063; _ym_uid=1594445063951245690; font_loaded=YSv1; gdpr=0; L=dmh+eXADa18BWGN9A0MMXn9WcnNyf1YEHzQmJwsLCB80ABE=.1594445196.14293.342702.a27b6a6efcdce24f907a4cf9413cc720; yandex_login=travinkosty; my=YwA=; gdpr=0; yuidss=5517453851594445059; yandex_gid=120836; i=Sox29LV+u1Bg0iYQ6yEIsOruq0eroYLC4Q3Z+oL5f5vKCQ7hmy8frtbFwzeXXXbkk+9Tl+OO96xY87XVPYGFox4pL/w=; Session_id=3:1601556558.5.0.1594445196115:fPkiTQ:1e.1|173519060.0.2|223721.591512.w8BDC8SSuO0claGf11jeWhnvVRA; sessionid2=3:1601556558.5.0.1594445196115:fPkiTQ:1e.1|173519060.0.2|223721.242131._lwmcOlKP5Ir9dPHLdo8HKRRUZQ; _ym_isad=2; cycada=S0IENoHkeJoCFq7me4xY+dTflCaoXU2XmhR66fHjRQ8=; ys=def_bro.1#svt.1#ybzcc.ru#wprid.1601638848701121-976920426665608871200126-production-app-host-sas-web-yp-162; zm=m-white_bender_zen-ssr.webp.css-https%3As3home-static_e1w4WG7kdFM2ORnSXtnM3eYAU6Q%3Al; yc=1601901594.zen.cacS%3A1601645993; is_gdpr=0; _ym_d=1601642447; yabs-frequency=/5/0W0D06A8TbylqdLV/nEHoS9G0001SFI4Se3zjXW0005mz8G00/; is_gdpr_b=CNnvZBC3BCgC; yp=1632399418.cld.2270452#1632399418.brd.6101004764#1632640870.ygu.1#1909805196.udn.cDp0cmF2aW5rb3N0eQ%3D%3D#1610213200.szm.1:1920x1080:1920x964#1625981382.zmblt.1736#1914438058.sad.1594557929:1599078058:3#1605399517.hks.0#1604320804.csc.2#1601651859.gpauto.43_024269:131_893433:140:1:1601644659#1601790195.clh.2270453#1602161360.zmbbr.yandexbrowser:20_9_0_933#1601701531.nwcst.1601620200_120836_1',
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
                res = res['data']['items']
            except:
                # print(Exception)
                cnt_errors += 1
                print(str(cnt_errors) + "/" + str(cnt_atm))
                continue

            for item in res:
                if item['id'] not in self.atm_id_history.keys():
                    if not is_valid_bank(item):
                        print(f"{item['title']} is not valid bank")
                        continue

                    gis_id = item['id']
                    bank_name = get_bank_name(item['title'])

                    obj = {
                        'gis_id': gis_id,
                        'name': item['title'],
                        'bank': bank_name,
                        'address': get_key_value(item, 'address'),
                        'lat': item['coordinates'][1],
                        'lon': item['coordinates'][0],
                        'schedule': get_key_value(item, 'workingTime'),
                    }
                    self.df_data.append(obj)
                    self.atm_id_history[gis_id] = item['title']
                    self.bus_stations[gis_id] = item['stops'] if 'stops' in item.keys() else []
                    self.banks[gis_id] = bank_name

    def save_banks_to_file(self, name='data/banks.xlsx'):
        unique_banks = []

        for item in self.banks:
            if self.banks[item] not in unique_banks:
                unique_banks.append(self.banks[item])

        banks_df = pd.DataFrame(unique_banks)
        banks_df.columns = ['name']

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
