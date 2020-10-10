import pandas as pd
import requests
import json
import pickle


def get_key_value(obj, key, default=''):
    return obj[key] if key in obj else default


def is_valid_stop(obj):
    is_stop = False
    rubrics = obj['categories'] if 'categories' in obj else []

    for item in rubrics:
        is_stop = item['class'] == 'bus stop'

    return is_stop


class StopDownloader:
    def __init__(self, stops=None):
        if stops is None:
            stops = []
        self.stops = stops
        self.df_data = []
        self.stops_id_history = {}
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.83 YaBrowser/20.9.0.933 Yowser/2.5 Safari/537.36',
            'cookie': 'maps_los=0; is_gdpr=0; yandexuid=8601636181601633648; is_gdpr_b=CNnvZBC1BCgC; i=SOTAz5BzdbaG4AJ4WoICbV6NNdTEUTU9/4bI3ypONggWgfqjC+x7bb+92HBP6otTDxGOu9SoljJOwT1plVExGPUb56s=; gdpr=0; _ym_uid=1601633659253538198; _ym_d=1601633659; mda=0; _ym_isad=2; ymex=1916993657.yrts.1601633657; _csrf=lofXHFqMI97lnAFxbPrtPvs1; yabs-frequency=/5/0000000000000000/9cwmS9K0001SFQ00/; ys=wprid.1601638914860806-1186363965508602886300218-production-app-host-sas-web-yp-98; yp=1633171047.p_sw.1601635046#1602238459.szm.1:1920x1080:1920x964#1602238459.zmblt.1736#1602238459.zmbbr.yandexbrowser:20_9_0_933#1601860783.gpauto.43_024353:131_893432:140:0:1601687973#1601723533.ln_tp.01'
        }

    def stop_retrieval(self, url):
        answer = requests.get(url, headers=self.headers)
        return json.loads(answer.content.decode('utf-8'))

    def download_stop(self):
        cnt_stop = 0
        cnt_errors = 0
        print('количество url: ' + str(len(self.stops)))

        for stop in self.stops:
            cnt_stop += 1
            if cnt_stop % 10 == 0:
                print(cnt_stop)
            try:
                res = self.stop_retrieval(stop)
                res = res['data']['items']
            except:
                cnt_errors += 1
                print(str(cnt_errors) + "/" + str(cnt_stop))
                continue

            for item in res:
                if item['id'] not in self.stops_id_history.keys():

                    if not is_valid_stop(item):
                        print(f"{item['title']} is not valid stop")
                        continue

                    obj = {
                        'gis_id': item['id'],
                        'name': item['title'],
                        'address': get_key_value(item, 'address'),
                        'lat': item['coordinates'][1],
                        'lon': item['coordinates'][0],
                    }
                    self.df_data.append(obj)
                    self.stops_id_history[item['id']] = item['title']

    def save_to_file(self, name='data/stops.xlsx'):
        df = pd.DataFrame(self.df_data)
        df.to_excel(name, index=False)
