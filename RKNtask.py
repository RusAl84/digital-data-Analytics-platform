import xml.etree.ElementTree as Etree
from fake_useragent import UserAgent
from pymongo import MongoClient
from bs4 import BeautifulSoup
import requests
import zipfile
import pymongo
import os


class DB:
    def __init__(self, URL = '', PATH_ZIP = '', PATH_FOLDER_XML = '', PATH_XML = ''):
        self.URL = URL
        self.PATH_ZIP = PATH_ZIP
        self.PATH_XML = PATH_XML
        self.PATH_FOLDER_XML = PATH_FOLDER_XML  


    def TranserDataToVps(self):
        '''
        Перемещение локальной коллекции на сервер VPS
        '''
        collection_local = self.InitDbConnection(True, False)
        collection_vps = self.InitDbConnection(False, True)

        data = collection_local.find({})
        for document in data:
            collection_vps.insert_one(document)

    
    def TranserDataFromVpsToLocal(self):
        '''
        Перемещение коллекции с сервера в локальную бд
        '''
        collection_local = self.InitDbConnection(True, True)
        collection_vps = self.InitDbConnection(False, False)

        data = collection_vps.find({})
        for document in data:
            collection_local.insert_one(document)  


    def GetUrlOfZip(self, URL):
        '''
        Извлечение ссылки на zip файл
        '''
        r = requests.get(URL, headers = {'User-Agent': UserAgent().chrome})
        soup = BeautifulSoup(r.text, 'html.parser')
        table = soup.find_all(name='table', attrs = {'class': 'TblList'})
        self.URL += table[0].contents[18].contents[5].contents[0].get('href').split('/')[3]
        return 
        

    def DownloadData(self, PATH_ZIP):
        '''
        Скачивание zip файла
        '''
        try:
            self.GetUrlOfZip(URL)
            z = open(PATH_ZIP, "wb")
            r = requests.get(self.URL, stream = True, headers = {'User-Agent': UserAgent().chrome})
            for chunk in r.iter_content(chunk_size = 8388608):
                    z.write(chunk)
        except FileExistsError as err:
            print(str(err))
        else:
            z.close()
        # print("Файл закрыт! DownloadData")


    def GetXML(self, PATH_ZIP, PATH_FOLDER_XML):
        '''
        Распаковка zip и получение пути xml файла
        '''
        try:
            z = zipfile.ZipFile(PATH_ZIP, 'r')
            z.extractall(PATH_FOLDER_XML)
            self.PATH_XML = PATH_FOLDER_XML + '\\' + str(z.namelist()[0])
        except FileExistsError as err:
            print(str(err))
        except FileNotFoundError as err:
            print(str(err))
        else:
            z.close()
            # print("Файл закрыт! GetXML")


    def InitDbConnection(self, option, option_delete):
        '''
        option == True => подключение к локальной бд \n
        option == False => подключение к бд на VPS

        option_delete == True => удаление коллекции \n
        option_delete == False => коллекция не удаляется
        '''
        if option:
            client = MongoClient('localhost', 27017)
            db = client.RKNN
            if 'records_local' in db.collection_names() and option_delete:
                db.drop_collection('records_local')
            collection = db.records_local
            return collection
        else:
            client = MongoClient('localhost', 27017)
            db = client.RKNN
            if 'records' in db.collection_names() and option_delete:
                db.drop_collection('records')
            collection = db.records
            return collection


    def InsertIntoDb(self, option):
        '''
        Запись в локальную бд \n
        option == True => запись в локальную бд \n
        option == False => запись в бд на сервере
        '''
        collection = self.InitDbConnection(option, True)

        context = Etree.iterparse(self.PATH_XML, events=('start','end'))
        context = iter(context)
        event, _ = context.__next__()

        document = {}
        for event, elem in context:
            if str(elem.tag).split("}")[1] != 'record':
                if str(elem.tag).split("}")[1] == 'is_list':
                    is_list = {}
                    additional_tag = 0
                    event, elem = context.__next__() # Get initial 'is'
                    while str(elem.tag).split("}")[1] != 'is_list':
                        is_name = str(elem.tag).split("}")[1] + str(additional_tag)
                        is_list[is_name] = {}
                        event, elem = context.__next__() # Get inner elements in any 'is'
                        while str(elem.tag).split("}")[1] != 'is':
                            is_list[is_name][str(elem.tag).split("}")[1]] = elem.text
                            event, elem = context.__next__()
                        event, elem = context.__next__() # Get next 'is'
                        additional_tag += 1
                    document['is_list'] = is_list
                    continue
                document[str(elem.tag).split('}')[1]] = elem.text
            if str(elem.tag).split("}")[1] == 'record' and event == 'end':
                collection.insert_one(document)
                elem.clear()
                document = {}


if __name__ == "__main__":
    '''
    URL - ссылка на сайт, где лежит zip
    PATH_ZIP - Куда скачать zip с сайта
    PATH_FOLDER_XML - Куда распаковать zip (папка, в которой будет лежать .xml)
    PATH_XML - Полный путь до .xml (для InsertIntoLocal)
    '''

    URL = 'https://rkn.gov.ru/opendata/7705846236-OperatorsPD/'
    PATH_ZIP = r'd:\rkn\data\data.zip'
    PATH_FOLDER_XML = r'd:\rkn\data'
   # PATH_XML = r'd:\rkn\data\data.xml'
    #db = DB(URL, PATH_ZIP, PATH_FOLDER_XML,PATH_XML)
    #db.DownloadData(db.PATH_ZIP)
    db = DB(URL, PATH_ZIP, PATH_FOLDER_XML)
    db.GetXML(db.PATH_ZIP, db.PATH_FOLDER_XML)
    db.InsertIntoDb(True)
    # db.TranserDataToVps()
    # db.TranserDataFromVpsToLocal()