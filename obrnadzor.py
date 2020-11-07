import xml.etree.cElementTree as Etree 
from fake_useragent import UserAgent
from pymongo import MongoClient
from bs4 import BeautifulSoup
from threading import Thread
import threading
import requests
import zipfile
import pymongo
import bson
import time
import queue
import os

URL_GENERIC = r'http://isga.obrnadzor.gov.ru/accredreestr/search/?page='
URL_SPECIFIC = r'http://isga.obrnadzor.gov.ru/accredreestr/details/'

data = {
    'regionId': '',
    'searchby': 'organization', 
    'eduOrgName': '', 
    'eduOrgInn': '', 
    'eduOrgOgrn': '',  
    'eduOrgAddress': '', 
    'eduOrgTypeId': '', # Тип образовательной организации
    'eduOrgKindId': '',
    'indEmplLastName': '',
    'indEmplFirstName': '',
    'indEmplMiddleName': '',
    'indEmplAddress': '',
    'indEmplEgrip': '',
    'indEmplInn': '',
    'certRegNum': '', 
    'certSerialNum': '', 
    'certFormNum': '', 
    'certIssueFrom': '', 
    'certIssueTo': '', 
    'certEndFrom': '', 
    'certEndTo': '',  
    'certStatusId': '', # Текущий статус свидетельства
    'certificatesupplementstatusId': '',
    'eduProgCode': '',
    'eduProgName': '',
    'extended': ''
}

eduOrgTypeId = {
    39:'Военное образовательное учреждение высшего профессионального образования',
    40:'Образовательная организация высшего образования', 
    41:'Образовательное учреждение высшего профессионального образования',
    29:'Образовательные учреждения высшего профессионального образования',
    18:'Учреждение высшего профессионального религиозного образования'
}

certStatusId = {
    9:'Возобновлено',
    10:'Выдан дубликат',
    11:'Выдано временное свидетельство',
    1:'Действующее1',
    12:'Действующее2',
    15:'Переоформлено',
    16:'Переоформлено в части приложения',
    5:'Приостановлено частично1',
    19:'Приостановлено частично2'
}

headers = {'User-Agent': UserAgent().chrome}

queue_of_pages = queue.Queue()


def TranserDataToVps():
    '''
        Перемещение локальной коллекции на сервер VPS
    '''
    collection_local = InitDbConnection(True, False)
    collection_vps = InitDbConnection(False, True)

    data = collection_local.find({})
    for document in data:
        collection_vps.insert_one(document)


def GetCountOfPages():
    '''
    Узнаем количество страниц, на которых расположена таблица \n
    с неполной информацией об объектах.
    '''
    try:
        resp = requests.post(URL_GENERIC + '10000000000', data=data, headers=headers)
        soup = BeautifulSoup(resp.text, 'html.parser')
        return int(soup.findAll('li')[-2].find('a').text)
    except IndexError:
        return -1


def GetGenericSoup(page_number):
    '''
    Получаем страницу, на которой находится таблица с информацией о множестве \n
    объектов, но информация о них является неполной.
    '''
    resp = requests.post(URL_GENERIC + str(page_number), data=data, headers=headers)
    soup = BeautifulSoup(resp.text, 'html.parser')
    return soup


def GetSoup():
    resp = requests.get(r'http://isga.obrnadzor.gov.ru/accredreestr/')
    soup = BeautifulSoup(resp.text, 'html.parser')
    return soup


def GetRegionsDict():
    '''
    Получаем словарь вида: 'номер региона': 'название региона'
    '''
    soup = GetSoup()
    select = soup.find('select', attrs = {'name': 'regionId', 'class': 'form-control'})
    options_list = select.find_all('option')
    dictionary = {'': 'Не выбрано'}
    for option in options_list:
        if option.text != 'Не выбрано':
            data = option.text.split(' - ')
            if len(data) > 2:
                dictionary[str(data[1] + ' - ' + data[2])] = data[0]
            else:
                dictionary[data[1]] = data[0]
    return dictionary


def GetSpecificSoup(key):
    '''
    Получаем страницу, на которой находится таблица, в которой хранится \n
    полная информация об одном объекте.
    '''
    resp = requests.get(URL_SPECIFIC + str(key) + '/1/')
    soup = BeautifulSoup(resp.text, 'html.parser')
    return soup


def InitDbConnection(option, option_delete):
    '''
    option == True => подключение к локальной бд \n
    option == False => подключение к бд на VPS

    option_delete == True => удаление коллекции \n
    option_delete == False => коллекция не удаляется
    '''
    if option: # Подключение к локальной, пересоздание и обновление коллекции obrnadzor_vishee_vse_regioni_local
        client = MongoClient('localhost', 27017)
        db = client.RKNN
        if 'obrnadzor_vishee_vse_regioni_local' in db.collection_names() and option_delete:
            db.drop_collection('obrnadzor_vishee_vse_regioni_local')
        collection = db.obrnadzor_vishee_vse_regioni_local
        return collection
    else: # Подключение к бд на сервере, пересоздание и обновление коллекции obrnadzor_vishee_vse_regioni
        client = MongoClient('localhost', 27017)
        db = client.RKNN
        if 'obrnadzor_vishee_vse_regioni' in db.collection_names() and option_delete:
            db.drop_collection('obrnadzor_vishee_vse_regioni')
        collection = db.obrnadzor_vishee_vse_regioni
        return collection


def FillParts(td_list, document_part):
    '''
    Документ, вставляемый в mongo состоит из 2-х частей, т.к таблица \n
    с полной информацией об одном объекте разделена на две части. \n
    Эта функция заполняет ту часть, которая передана в эту функцию.
    '''
    if len(td_list) > 1:
        document_part[td_list[0].text] = td_list[1].text
        return document_part


def CorrectErrors(errors):
    global data
    for generic_page_number, key_type, key_status in errors:
        print("Коррекция ошибок " + str(threading.currentThread().getName()) + " " + str(generic_page_number))
        data['eduOrgTypeId'] = str(key_type)
        data['certStatusId'] = str(key_status)
        package = queue.Queue()
        soup_generic = GetGenericSoup(generic_page_number)
        tr_list = soup_generic.find('tbody').find_all('tr')
        for tr_generic in tr_list:
            dataset = {}
            data_id = tr_generic.attrs.get('data-id')
            soup_specific = GetSpecificSoup(data_id)
            dataset['eduOrgTypeId'] = str(key_type) + ' ' + str(eduOrgTypeId[key_type])
            dataset['certStatusId'] = str(key_status) + ' ' + str(certStatusId[key_status])
            dataset['page'] = soup_specific
            package.put(dataset)
        while not package.empty():
            queue_of_pages.put(package.get())


def GetAllPages(step):
    global data
    errors = []
    for key_type in eduOrgTypeId:
        data['eduOrgTypeId'] = str(key_type)
        print('Тип образовательной организации: ' + str(key_type))
        for key_status in certStatusId:
            print('Текущий статус свидетельства: ' + str(key_status))
            package = queue.Queue()
            data['certStatusId'] = str(key_status)
            count_of_pages = GetCountOfPages() + 1
            print('count_of_pages = ' + str(count_of_pages))
            generic_page_number = 1 + int(threading.currentThread().getName().split('-')[1])
            while generic_page_number < count_of_pages:
                try:
                    print(generic_page_number)
                    soup_generic = GetGenericSoup(generic_page_number)
                    tr_list = soup_generic.find('tbody').find_all('tr')
                    for tr_generic in tr_list:
                        dataset = {}
                        # print("specific" + str(threading.currentThread().getName().split('-')[1]))
                        data_id = tr_generic.attrs.get('data-id')
                        soup_specific = GetSpecificSoup(data_id)
                        dataset['eduOrgTypeId'] = str(key_type) + ' ' + str(eduOrgTypeId[key_type])
                        dataset['certStatusId'] = str(key_status) + ' ' + str(certStatusId[key_status])
                        dataset['page'] = soup_specific
                        package.put(dataset)
                    while not package.empty():
                        queue_of_pages.put(package.get())
                    
                except (requests.exceptions.ConnectionError, AttributeError) as err:
                    print(str(soup_generic))
                    print("Error ------------- " + str(generic_page_number))
                    package = queue.Queue()
                    error = [generic_page_number, key_type, key_status]
                    errors.append(error)
                    time.sleep(10)
                generic_page_number += step
    print('Количество ошибок в потоке ' + str(threading.currentThread().getName()) + ' ' + str(len(errors)))
    CorrectErrors(errors)
    print(threading.currentThread().getName() + " Завершился!")

def InsertIntoDb(option, option_delete):
    '''
    option == True => подключение к локальной бд \n
    option == False => подключение к бд на VPS

    option_delete == True => удаление коллекции \n
    option_delete == False => коллекция не удаляется
    '''
    dictionary = GetRegionsDict()
    list_of_threads = list()
    count_of_threads = 15
    for name in range(count_of_threads):
        thread = Thread(target=GetAllPages, name='Thread-'+str(name), args=(count_of_threads,))
        thread.start()
        list_of_threads.append(thread)

    collection = InitDbConnection(option, option_delete)

    is_all_alive = True
    while is_all_alive or not queue_of_pages.empty():
        for thread in list_of_threads:
            if thread.is_alive():
                is_all_alive = True
                break
            is_all_alive = False

        document = {}
        document_part_1 = {}
        document_part_2 = {}
        if not queue_of_pages.empty():
            dataset = queue_of_pages.get()
            soup_specific = dataset['page']
            count_of_specific_tr = len(soup_specific.find('tbody').find_all('tr'))
            for tr, j in zip(soup_specific.find('tbody').find_all('tr'), range(count_of_specific_tr)):
                td_list = tr.find_all('td')
                if j < 6:
                    document_part_1 = FillParts(td_list, document_part_1)
                elif j > 6:
                    document_part_2 = FillParts(td_list, document_part_2)
            document_part_1['Номер региона'] = dictionary[document_part_1['Субъект РФ']]
            document['Сведения об образовательной организации или организации, осуществляющей обучение'] = document_part_1
            document_part_2['Дата выдачи свидетельства'] = document_part_2['Дата выдачи свидетельства'].split()[0]
            document_part_2['Срок действия свидетельства'] = document_part_2['Срок действия свидетельства'].split()[0]
            document_part_2['Дата публикации сведений в сводном реестре'] = document_part_2['Дата публикации сведений в сводном реестре'].split()[0]
            document['Общие сведения о государственной аккредитации'] = document_part_2
            document['Тип образовательной организации'] = dataset['eduOrgTypeId'].split(' ')[0]
            document['Текущий статус свидетельства'] = dataset['certStatusId'].split(' ')[0]
            collection.insert_one(document)
        else:
            time.sleep(15)

if __name__ == '__main__':
    # print(GetRegionsDict())
    InsertIntoDb(True, True)
    # TranserDataToVps()
    # print(GetCountOfPages())