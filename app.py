from datetime import datetime
import threading
import pandas as pd
import instaloader
import numpy as np
import argparse
import time
import os
import pysftp

def salvataggio (indice_hash_corrente, startTime_corrente, endTime_corrente, volteDentroTods):
    f = open("Informazioni_stato.txt", "w")
    f.write(str(indice_hash_corrente)+",")
    f.write(str(startTime_corrente)+",")
    f.write(str(endTime_corrente)+",")
    f.write(str(volteDentroTods))
    f.close()


def instaloader_download_hash(user: str, pw: str, hashtagList):
    '''
    :param user: username for instagram login
    :param pw: the password for the username
    :param hashtag: the collection of hashtags given to that account
    :param start_time: the first datatime to keep
    :param end_time: the last datetime to keep
    :return: None
    '''

    f = open("Informazioni_stato.txt", "r")
    contents = f.readline()
    contenitore = contents.split(",")
    j = int(contenitore[0])
    startT = datetime.strptime(contenitore[1], '%Y-%m-%d %H:%M:%S')
    endT = datetime.strptime(contenitore[2], '%Y-%m-%d %H:%M:%S')
    n_download_tods = int(contenitore[3])
    f.close()


    L = instaloader.Instaloader(download_videos=False, max_connection_attempts=1000, download_geotags=True,
                               download_comments=True, compress_json=False)
    if os.path.exists('session-' + user):
        L.load_session_from_file(username=user, filename='session-' + user)
    else:
        L.login(user, pw)
        L.save_session_to_file(filename='session-' + user)

    # random.shuffle(hashtagList)
    #startT = datetime(2020,11,1)
    #endT = datetime(2020,11,15 )
    #j = 0

    # for each hashtag, there is the download of all the post associated
    while (True):
        #print("Sono qui")
        #salvataggio(j,startT,endT,n_download_tods)
        hashtag = instaloader.Hashtag.from_name(L.context, hashtagList[j])

        posts = hashtag.get_posts()

        #n_download_tods = 0
        #print("sono qui")
        year = startT.__getattribute__("year")
        monthS = startT.__getattribute__("month")
        monthE = endT.__getattribute__("month")

        k = 0  # initiate k
        k_list = []  # uncomment this to tune k
        conta = 0
        print("Hashtag corrente: ", hashtagList[j])

        for post in posts:
            if (conta != 0) and ((conta % 50) == 0):
                instaloader_download_blog(blogger="Tods", L=L, n_volte=n_download_tods)
                n_download_tods = n_download_tods + 1
                salvataggio(j, startT, endT, n_download_tods)
            if (conta >= 200):
                j = j + 1
                break
            postdate = post.date
            #print(postdate)

            if (postdate > startT) and (postdate < endT):
                print("Hashtag corrente: ", hashtagList[j])
                print("Contatore: ", conta)
                L.download_post(post, hashtagList[j])
                conta = conta + 1
                time.sleep(30)

        print("Cambio hashtag..")

        print('Uploading files on VRAI server...')
        cinfo = {'host': '193.205.130.253', 'username': 'vrai', 'password': 'vrai_2019', 'port': 30022}
        with pysftp.Connection(**cinfo) as sftp:
            sftp.put_r(hashtagList[j] + '/', '/disks/disk3/MarcoMameli/Social/FashionExtractor/ProvaHeroku/', preserve_mtime=True)


        time.sleep(120)
        j = j+1

        if (len(hashtagList) == j):
            print("Cambio data..")
            if(monthS == 1):
                monthS = 12
                monthE = 12
                year = year-1
                startT = datetime(year, monthS, 1)
                endT = datetime(year, monthE, 30)
            else:
                monthS = monthS-1
                monthE = monthE -1
                startT = datetime(year, monthS, 1)
                endT = datetime(year, monthE, 30)

            j == 0
        salvataggio(j, startT, endT, n_download_tods)
    return None


def instaloader_download_blog(blogger, L, n_volte):
    '''
    :param user: username for instagram login
    :param pw: the password for the username
    :param blogger: the collection of blogger given to that account
    :param start_time: the first datatime to keep
    :param end_time: the last datetime to keep
    :return: None
    '''

    # for each bloger, there is the download of all the post associated

    blogger = instaloader.Profile.from_username(L.context, blogger)

    posts = blogger.get_posts()
            #SINCE = start_time #datetime(2020, 6, 29)  # further from today, inclusive (da)
            #UNTIL = end_time #datetime(2020, 7, 5)  # closer to today, not inclusive (a)

    start = 50 * n_volte
    stop = start+50
    n_ciclo = 0
    for post in posts:
        if (n_ciclo >= start) and (n_ciclo <= stop):
            print("Scarico post Tod")
            L.download_post(post,"Tod_Post")
            time.sleep(30)
        if (n_ciclo > stop):
            break
        n_ciclo = n_ciclo + 1
    time.sleep(120)
    return None

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('--choice', type=str, default='hashtag',
                        help='Use "blogger" or "hashtag" to download posts from only one of them')
    args = parser.parse_args()

    # Leggo i vari account con i quali accedere a insta
    accounts = pd.read_csv('Accounts.csv', sep=',')

    # Leggo i vari hashtag
    with open('hashtag.txt', 'r') as f:
        contentH = f.readlines()
    f.close()
    contentH = [x.strip() for x in contentH]

    thread_list = []

    # Creo thread solo per il download di post associati ad hashtag
    if args.choice == "hashtag":

        # Divido gli hashtag in modo da darne ad ogni account un numero uguale
        hashtag_div = np.array_split(contentH, len(accounts))

        i = 0
        # Creo un numero di thread per il download dei post associati agli hashtag pari al numero di account dedicati
        for index, accounts in accounts.iterrows():
            thread_list.append(threading.Thread(target=instaloader_download_hash,
                                                args=(accounts['ACCOUNT'], accounts['PW'], hashtag_div[i])))
            i = i + 1

    r'''
    # Leggo i vari nomi dei blogger
    with open('blogger_list.txt', 'r') as f:
        contentB = f.readlines()
    f.close()
    contentB = [x.strip() for x in contentB]
    contentB = [x.replace(" ", "") for x in contentB]
    
    # Creo thread solo per il download di post associati a blogger
    if(args.choice == 'blogger'):

        # Divido i blogger in modo da darne ad ogni account un numero uguale
        blogger_div = np.array_split(contentB, len(accounts))

        i = 0
        # Creo un numero di thread per il download dei post associati ai blogger pari al numero di account dedicati
        for index, accounts in accounts.iterrows():
            thread_list.append(threading.Thread(target=instaloader_download_blog,
                                                args=(accounts['ACCOUNT'], accounts['PW'], blogger_div[i]
                                                      ,
                                                      datetime(2020, 8, 1),
                                                      datetime(2020, 11, 30))))
            i = i + 1


    # Creo thread per il download di post associati ad hashtag e blogger
    if(args.choice == "all"):

        # Calcolo la lunghezza di metà del dataframe contente i vari profili instagram
        index = accounts.index
        num_Acc = int((len(index) / 2))

        # Definisco quali account saranno utilizzati per il download dei post associati ad hashtag
        accounts_Hash = accounts.iloc[:num_Acc, :]
        # Definisco quali account saranno utilizzati per il download dei post associati a blogger
        accounts_Blog = accounts.iloc[num_Acc:, :]

        # Divido gli hashtag in modo da darne ad ogni account un numero uguale
        hashtag_div = np.array_split(contentH, len(accounts_Hash))
        # Divido i blogger in modo da darne ad ogni account un numero uguale
        blogger_div = np.array_split(contentB, len(accounts_Blog))

        i = 0

        # Creo un numero di thread per il download dei post associati agli hashtag pari al numero di account dedicati
        for index, accounts_Hash in accounts_Hash.iterrows():
            thread_list.append(threading.Thread(target=instaloader_download_hash,
                                                args=(accounts_Hash['ACCOUNT'], accounts_Hash['PW'], hashtag_div[i]
                                                      )))
            i = i + 1
        i = 0

        # Creo un numero di thread per il download dei post associati ai blogger pari al numero di account dedicati
        for index, accounts_Blog in accounts_Blog.iterrows():
            thread_list.append(threading.Thread(target=instaloader_download_blog,
                                                args=(accounts_Blog['ACCOUNT'], accounts_Blog['PW'], blogger_div[i]
                                                      ,
                                                      datetime(2020, 8, 1),
                                                      datetime(2020, 11, 30))))
            i = i + 1
    '''

    # Avvio i thread creati precedentemente
    for thread in thread_list:
        thread.start()

    for thread in thread_list:
        thread.join()
