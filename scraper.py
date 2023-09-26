import requests
import bs4 
import pymongo
import datetime
from pymongo import MongoClient
from multiprocessing import Pool
import time

URL = "https://www.orsr.sk/hladaj_zmeny.asp"
DOMENA = "https://www.orsr.sk/"
headers = {'Accept-Charset':  'utf8'}

# funkcia ktorá parsne jednotlive typy dát
def parse(left_, right_, data):
    left_ = left_.strip().replace(':','')

    try:
        if 'Oddiel' in left_:
            data['oddiel'] = right_[0].strip()

        elif 'Obchodné meno' in left_:
            data['obchodneMeno'] = right_[0].strip()

        elif 'Sídlo' in left_:
            data['sidlo'] = [r.strip() for r in right_[:-1]] 

        elif 'IČO' in left_:
            data['ico'] = int(right_[0].replace(' ',''))
            
        elif 'Deň zápisu' in left_:
            d,m,y = list(map(int, (right_[0].strip()).split('.')))
            data['denZapisu'] = datetime.datetime(y,m,d) 

        elif 'Právna forma' in left_:
            data['pravnaForma'] = right_[0].strip()

        elif 'Predmet činnosti' in left_:
            data['predmetyCinnosti'] = [r.strip() for r in right_ if '(od: ' not in r]

        elif 'Spoločníci' in left_:
            data['spolocnici'] = dict()
            right_ = [e.strip() for e in right_]
            address=[]

            for e in right_:
                if '(od: ' in e:
                    data['spolocnici'][' '.join(address[:2])] ={"adresa":(' '.join(address)).strip()}
                    address=[]
                else:
                    address.append(e)

        elif 'Výška vkladu' in left_:
            data['vyskaVkladov'] = dict()
            right_ = [e.strip() for e in right_]
            vklady=[]
            vklad,splatene=0,0
            mena = ''

            for i,e in enumerate(right_):
                if 'Vklad' in e:
                    vklad= float((e.split(':')[1]).replace(' ','').replace(',','.'))
                    mena = right_[i+1]
                elif 'Splatené' in e:
                    splatene= float((e.split(':')[1]).replace(' ','').replace(',','.'))

                if '(od: ' in e:
                    data['vyskaVkladov'][' '.join(vklady[:2])] = {'vklad':vklad, 'splatene':splatene, 'mena':mena}
                    vklady=[]
                else:
                    vklady.append(e)

        elif 'Štatutárny orgán' in left_:
            data['statutarnyOrgan'] = dict()
            right_ = [e.strip() for e in right_]
            arr=[]
            typ = ''
            for e in right_:
                if '(od: ' in e:
                    if (len(arr)==1):
                        typ=arr[0]
                        data['statutarnyOrgan'][typ]=dict()
                    else:
                        adresa = []
                        y,m,d,=1,1,1

                        for slovo in arr:
                            if 'Vznik' in slovo:
                                d,m,y = list(map(int, (slovo.split(':')[1].strip()).split('.')))
                            else:
                                adresa.append(slovo)

                        
                        data['statutarnyOrgan'][typ][' '.join(arr[:2])] ={"adresa":(' '.join(adresa)).strip(), 'vznik': datetime.datetime(y,m,d) }
                    arr=[]
                else:
                    arr.append(e)

        elif 'Konanie' in left_:
            data['konanie'] = [r.strip() for r in right_ if '(od: ' not in r]

        elif 'Základné imanie' in left_:
            mena = right_[1].replace(' ','')
            hod=float(right_[0].replace(' ','').replace(',','.'))
            data['zakladneImanie'] = {'hodnota':hod, 'mena':mena}
            if (len(right_)>3):
                splatene=float(right_[2].split(':')[1].replace(' ','').replace(',','.'))
                data['zakladneImanie']['splatene'] = splatene

        elif 'Dátum aktualizácie údajov' in left_:
            d,m,y = list(map(int, (right_[0].strip()).split('.')))
            data['denAktualizacie'] = datetime.datetime(y,m,d) 

        elif 'Dátum výpisu' in left_:
            d,m,y = list(map(int, (right_[0].strip()).split('.')))
            data['denVypisu'] = datetime.datetime(y,m,d) 

        else:
            data[left_] = ' '.join(right_).strip()

    except Exception as e:
        data[left_] = ' '.join(right_).strip()

# funkcia ktorá fetchne detaily o jednej firme a
# tie potom rozdeli na jednotlive kategorie a parse do slovnika
# a nakoniec pridá do databázy
def fetch_decompose_add2db(link):
    global collection,links

    text = requests.get(DOMENA+link, headers=headers).content
   
    data = dict()
    document = bs4.BeautifulSoup(text, 'html.parser')
    tables = document.find('body').find_all("table",recursive=False)

    for table in tables[2:]:
        left_=''
        right_=[]
        for span in table.find_all('span'):
            if span['class'] == ["tl"]:
                if (len(right_)):
                    parse(left_, right_, data)
                left_ = span.text
                right_=[]
            else :
                right_.append(span.text)
        
        if (len(right_)):
            parse(left_, right_, data)


    collection.insert_one(data)
    print(f"Sťahuje sa údaj číslo {collection.count_documents({})}/{len(links)}", end='\r')

# pripojenie k db
try:
    print("Pripojuje sa k databáze.")
    client = MongoClient("mongodb://localhost:27017/")
    db = client["firmy_db"]
    collection = db["firmy_collection"]
    collection.drop()
    print("Databáza bola pripojená.")

except Exception as error:
    print("Databázu sa nepodarilo pripojiť: ", error)

# stiahnutie listu spolocnosti a paralelne fetchovanie ich detailov
try:
    print("Sťahuje sa tabuľka...")


    html = requests.get(URL, headers=headers).content

    document = bs4.BeautifulSoup(html, 'html.parser')
    h3 = document.find('h3', class_='src')
    table = h3.find('table')
    rows = table.find_all('tr')
    links = [row.find('a', class_='link')['href'] for row in rows[1:]]

    with Pool() as p:
        p.map(fetch_decompose_add2db, links)

    print("Údaje sa stiahli a úspešne uložili do databázy.")

except Exception as error:
    print("Sťahovanie zlyhalo: ", error)


client.close()