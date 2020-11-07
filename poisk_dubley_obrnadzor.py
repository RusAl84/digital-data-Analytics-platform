from pymongo import MongoClient

if __name__ == "__main__":
    client = MongoClient('localhost', 27017)


    db = client['RKNN']
    collection = db.obrnadzor_vishee_vse_regioni_local
    list_inn = []  # для наших инн с монго
    for post in collection.find():
        # print( post)
        inn = post["Сведения об образовательной организации или организации, осуществляющей обучение"]['ИНН']
        print(inn)
        list_inn.append(inn)
    dubl_inn = []

    for inn in list_inn:
        # print(inn)
        # print(list_inn.count(inn))
        kol_inn = list_inn.count(inn)
        list_inn.remove(inn)
        if kol_inn > 1:
            # print("ИНН: {0} сколько раз: {1}".format(inn, kol_inn))
            dubl_inn.append(inn)

    dubl_inn = list(set(dubl_inn))
    print(dubl_inn)
    print("Количество дубликатов ИНН {0}".format(len(dubl_inn)))
    print(collection.find().count())
    for inn in dubl_inn:
        kolichestvo = 0
        id = ""
        for post in collection.find({
            "Сведения об образовательной организации или организации, осуществляющей обучение.ИНН": "{0}".format(
                inn)}):
            if kolichestvo == 0:
                id = post['_id']
                # print(id)
            else:
                current_id = post['_id']
                # current_id = f"ObjectId('{current_id}')"
                lic = post['Общие сведения о государственной аккредитации']
                lic_text = f"lic {kolichestvo}"
                new_post = {}
                new_post[lic_text] = lic
                collection.update_one({'_id': id}, {"$set": new_post}, upsert=False)
                # id=f"ObjectId('{id}')"
                print("_id {0}".format(id))
                print("current_id {0}".format(current_id))
                deleteResult = collection.delete_one({"_id": current_id})
                print(deleteResult.deleted_count)
            kolichestvo += 1
    print(collection.find().count())

