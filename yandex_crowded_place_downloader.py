import pandas as pd
import requests
import json
import pickle


def get_key_value(obj, key, default=''):
    return obj[key] if key in obj else default


def get_type_place(obj, default=''):
    valid_categories = ['public_transport_stop', 'gas_station', 'shopping_mall']
    categories = obj['categories'] if 'categories' in obj else []

    for item in categories:
        if item['seoname'] in valid_categories:
            return item['seoname']

    return default


class CrowdedPlaceDownloader:
    def __init__(self, crowded_places=None):
        if crowded_places is None:
            crowded_places = []
        self.crowded_places = crowded_places
        self.df_data = []
        self.cp_id_history = {}
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.83 YaBrowser/20.9.0.933 Yowser/2.5 Safari/537.36',
            'cookie': 'maps_los=0; yandexuid=5517453851594445059; mda=0; ymex=1909805063.yrts.1594445063; _ym_uid=1594445063951245690; font_loaded=YSv1; gdpr=0; L=dmh+eXADa18BWGN9A0MMXn9WcnNyf1YEHzQmJwsLCB80ABE=.1594445196.14293.342702.a27b6a6efcdce24f907a4cf9413cc720; yandex_login=travinkosty; my=YwA=; gdpr=0; yuidss=5517453851594445059; zm=m-white_bender_zen-ssr.webp.css-https%3As3home-static_ojdyXJYc6GWtnYsDgnLN0CCgFj4%3Al; yandex_gid=120836; yc=1602389225.zen.cacS%3A1602133623; _ym_d=1602154745; i=7WlpKn6irU90/e+YfJCq/p6PjiBU9PcEgoXpz57t+ZjJUSWG7Tpg7GwJZcsZVz3FhF9vsM/xYvDy81lz80BzH5rro7M=; cycada=YDUzw2lH2Z7U/0LZvFVbQmM2ah8BwwNFRSAInF0PVNk=; yabs-frequency=/5/2G0F039kWL-AMtrV/; is_gdpr=0; is_gdpr_b=CNnvZBDzBSgC; ys=def_bro.1#svt.1#ybzcc.ru#wprid.1602379546655838-1256213076096110433102339-production-report-vla-web-yp-44; Session_id=3:1602379703.5.0.1594445196115:fPkiTQ:1e.1|173519060.0.2|224179.458067.qyJ_RN2Llg2QxIXj8wQA7J3CE-c; sessionid2=3:1602379703.5.0.1594445196115:fPkiTQ:1e.1|173519060.0.2|224179.248153.qSMEme3e_6MM7C4cKRkAeGTc_SI; _ym_isad=2; yp=1632399418.cld.2270452#1632399418.brd.6101004764#1604722023.ygu.1#1909805196.udn.cDp0cmF2aW5rb3N0eQ%3D%3D#1610213200.szm.1:1920x1080:1920x964#1625981382.zmblt.1744#1914438058.sad.1594557929:1599078058:3#1605399517.hks.0#1604320804.csc.2#1602553879.gpauto.43_031543:131_886596:140:0:1602381069#1602389355.clh.2270456#1602767663.zmbbr.yandexbrowser:20_9_1_112'
        }

    def cp_retrieval(self, url):
        answer = requests.get(url, headers=self.headers)
        return json.loads(answer.content.decode('utf-8'))

    def download_cp(self):
        cnt_cp = 0
        cnt_errors = 0
        print('количество url: ' + str(len(self.crowded_places)))

        for place in self.crowded_places:
            cnt_cp += 1
            if cnt_cp % 10 == 0:
                print(cnt_cp)
            try:
                res = self.cp_retrieval(place)
                res = res['data']['items']
            except:
                cnt_errors += 1
                print(str(cnt_errors) + "/" + str(cnt_cp))
                continue

            for item in res:
                if item['id'] not in self.cp_id_history.keys():

                    place_type = get_type_place(item)

                    if place_type == '':
                        print(f"{item['title']} is not valid place")
                        continue

                    obj = {
                        'gis_id': item['id'],
                        'name': item['title'],
                        'type': place_type,
                        'address': get_key_value(item, 'address'),
                        'lat': item['coordinates'][1],
                        'lon': item['coordinates'][0],
                    }

                    self.df_data.append(obj)
                    self.cp_id_history[item['id']] = item['title']

    def save_to_file(self, name='data/crowded_places.xlsx'):
        df = pd.DataFrame(self.df_data)
        df.to_excel(name, index=False)
