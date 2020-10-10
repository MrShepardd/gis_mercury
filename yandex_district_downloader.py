import pandas as pd
import requests
import json
import pickle


def get_key_value(obj, key, default=''):
    return obj[key] if key in obj else default

class DistrictDownloader:
    def __init__(self, path='data/district-urls.xlsx'):
        self.path = path
        self.df = pd.read_excel(self.path)
        self.df_data = []
        self.district_id_history = {}
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.83 YaBrowser/20.9.0.933 Yowser/2.5 Safari/537.36',
            'cookie': 'maps_los=0; yandexuid=5517453851594445059; mda=0; ymex=1909805063.yrts.1594445063; _ym_uid=1594445063951245690; font_loaded=YSv1; gdpr=0; L=dmh+eXADa18BWGN9A0MMXn9WcnNyf1YEHzQmJwsLCB80ABE=.1594445196.14293.342702.a27b6a6efcdce24f907a4cf9413cc720; yandex_login=travinkosty; my=YwA=; gdpr=0; yuidss=5517453851594445059; yandex_gid=120836; i=Sox29LV+u1Bg0iYQ6yEIsOruq0eroYLC4Q3Z+oL5f5vKCQ7hmy8frtbFwzeXXXbkk+9Tl+OO96xY87XVPYGFox4pL/w=; Session_id=3:1601556558.5.0.1594445196115:fPkiTQ:1e.1|173519060.0.2|223721.591512.w8BDC8SSuO0claGf11jeWhnvVRA; sessionid2=3:1601556558.5.0.1594445196115:fPkiTQ:1e.1|173519060.0.2|223721.242131._lwmcOlKP5Ir9dPHLdo8HKRRUZQ; _ym_isad=2; cycada=S0IENoHkeJoCFq7me4xY+dTflCaoXU2XmhR66fHjRQ8=; ys=def_bro.1#svt.1#ybzcc.ru#wprid.1601638848701121-976920426665608871200126-production-app-host-sas-web-yp-162; zm=m-white_bender_zen-ssr.webp.css-https%3As3home-static_e1w4WG7kdFM2ORnSXtnM3eYAU6Q%3Al; yc=1601901594.zen.cacS%3A1601645993; is_gdpr=0; _ym_d=1601642447; yabs-frequency=/5/0W0D06A8TbylqdLV/nEHoS9G0001SFI4Se3zjXW0005mz8G00/; is_gdpr_b=CNnvZBC3BCgC; yp=1632399418.cld.2270452#1632399418.brd.6101004764#1632640870.ygu.1#1909805196.udn.cDp0cmF2aW5rb3N0eQ%3D%3D#1610213200.szm.1:1920x1080:1920x964#1625981382.zmblt.1736#1914438058.sad.1594557929:1599078058:3#1605399517.hks.0#1604320804.csc.2#1601651859.gpauto.43_024269:131_893433:140:1:1601644659#1601790195.clh.2270453#1602161360.zmbbr.yandexbrowser:20_9_0_933#1601701531.nwcst.1601620200_120836_1',
        }

    def district_retrieval(self, url):
        answer = requests.get(url, headers=self.headers)
        return json.loads(answer.content.decode('utf-8'))

    def download_district(self):
        cnt_district = 0
        cnt_errors = 0
        print('количество url: ' + str(len(self.df)))

        for i, v in self.df.iterrows():
            cnt_district += 1
            if cnt_district % 10 == 0:
                print(cnt_district)
            try:
                res = self.district_retrieval(v['URL'])
                res = res['data']['items'][0]
            except:
                cnt_errors += 1
                print(str(cnt_errors) + "/" + str(cnt_district))
                continue

            if res['id'] not in self.district_id_history.keys():

                if res['type'] != 'toponym':
                    print(f"{res['title']} is not valid district")
                    continue

                obj = {
                    'gis_id': res['id'],
                    'name': res['title'],
                    'population': v['Население'],
                    'address': get_key_value(res, 'address'),
                    'lat': res['coordinates'][1],
                    'lon': res['coordinates'][0],
                    'geometry': get_key_value(res, 'displayGeometry'),
                }
                self.df_data.append(obj)
                self.district_id_history[res['id']] = res['title']

    def save_to_file(self, name='data/district.xlsx'):
        df = pd.DataFrame(self.df_data)
        df.to_excel(name, index=False)
