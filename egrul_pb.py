import time

import requests
import json
from fake_useragent import UserAgent
from pymongo import MongoClient

SLEEP_TIME = 75

class Company:
    def __init__(self, inn):
        headers = {
            'User-Agent': UserAgent().chrome,
            'X-Requested-With': 'XMLHttpRequest',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Origin': 'https://pb.nalog.ru',
        }

        cookies = {
            'uniI18nLang': 'RUS'
        }

        url = 'https://pb.nalog.ru/search-proc.json'

        # Очень страшно, но оно так работает, я не виноват
        data = {
            'page': 1,
            'pageSize': 10,
            'pbCaptchaToken': '',
            'token': '',
            'mode': 'search-all',
            'queryAll': inn,
            'queryUl': '',
            'okvedUl': '',
            'statusUl': '',
            'regionUl': '',
            'isMspUl': '',
            'mspUl1': '1',
            'mspUl2': '2',
            'mspUl3': '3',
            'queryIp': '',
            'okvedIp': '',
            'statusIp': '',
            'regionIp': '',
            'isMspIp': '',
            'mspIp1': '1',
            'mspIp2': '2',
            'mspIp3': '3',
            'queryUpr': '',
            'uprType1': '1',
            'uprType0': '1',
            'queryRdl': '',
            'dateRdl': '',
            'queryAddr': '',
            'regionAddr': '',
            'queryOgr': '',
            'ogrFl': '1',
            'ogrUl': '1',
            'npTypeDoc': '1',
            'ogrnUlDoc': '',
            'ogrnIpDoc': '',
            'nameUlDoc': '',
            'nameIpDoc': '',
            'formUlDoc': '',
            'formIpDoc': '',
            'ifnsDoc': '',
            'dateFromDoc': '',
            'dateToDoc': '',
        }

        r = requests.post(url, data=data, cookies=cookies, headers=headers)
        r = json.loads(r.text)
        if r['captchaRequired']:
            print('Сервер требует капчу')
        search_result = r['ul']['data'][0]
        token = search_result['token']

        data = {
            'token': token,
        }
        r = requests.post('https://pb.nalog.ru/company-proc.json', data=data, cookies=cookies, headers=headers)
        r = json.loads(r.text)['vyp']

        self.inn = inn
        self.name = r['НаимЮЛСокр']
        self.name_full = r['НаимЮЛПолн']
        self.reg_date = r.get('ДатаРег') or r.get('ДатаОГРН') or r.get('ДатаЗаписи')
        self.okved = r['КодОКВЭД'] + ' ' + r['НаимОКВЭД']
        self.ogrn = r['ОГРН']

    def __str__(self):
        return f'Имя: {self.name}\nОКВЭД: {self.okved}\n'


if __name__ == '__main__':
    client = MongoClient('LocalHost')
    obrnadzor_vishee_vse_regioni_filtered = client.RKNN.obrnadzor_vishee_vse_regioni_filtered
    egrul_pb = client.RKNN.egrul_pb

    for document in obrnadzor_vishee_vse_regioni_filtered.find({}):
        inn = document['Сведения об образовательной организации или организации, осуществляющей обучение']['ИНН']
        print(f'ИНН: {inn}')
        if egrul_pb.find_one({'inn': inn}):
            print('Этот ИНН уже в базе, пропускаем\n')
            continue

        try:
            company = Company(inn)
            print(company)
            egrul_pb.insert_one(vars(company))
        except Exception as e:
            print(f'Не удалось скачать информацию для ИНН {inn}: {e}')
        time.sleep(SLEEP_TIME)
