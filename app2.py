from datetime import datetime
import threading
import pandas as pd
import instaloader
import numpy as np
import argparse
import time
import os

def instaloader_download_hash(user: str, pw: str, hashtagList, start_time, end_time):
    '''

    :param user: username for instagram login
    :param pw: the password for the username
    :param hashtag: the collection of hashtags given to that account
    :param start_time: the first datatime to keep
    :param end_time: the last datetime to keep
    :return: None
    '''

    L = instaloader.Instaloader(download_videos=False, max_connection_attempts=1000, download_geotags=True,
                                download_comments=False, compress_json=False)
    if os.path.exists('session-' + user):
        L.load_session_from_file(username=user, filename='session-' + user)
    else:
        L.login(user, pw)
        L.save_session_to_file(filename='session-' + user)

    j = 0
    # for each hashtag, there is the download of all the post associated
    while (j < len(hashtagList)):
        try:
            hashtag = instaloader.Hashtag.from_name(L.context, hashtagList[j])

            posts = hashtag.get_posts()

            SINCE = start_time  # datetime(2020, 6, 29)  # further from today, inclusive (da)
            UNTIL = end_time  # datetime(2020, 7, 5)  # closer to today, not inclusive (a)

            k = 0  # initiate k
            k_list = []  # uncomment this to tune k

            for post in posts:
                postdate = post.date

                if postdate > UNTIL:
                    continue
                elif postdate <= SINCE:
                    k += 1
                    if k == 50:
                        break
                    else:
                        continue
                else:
                    time.sleep(5)
                    L.download_post(post, "Hashtag")
                    k = 0  # set k to 0
                    # if you want to tune k, uncomment below to get your k max
                    # k_list.append(k)
            j = j + 1
        except:

            j = j + 1



    return None

def instaloader_download_blog(user: str, pw: str, bloggerList, start_time, end_time):
    '''
    :param user: username for instagram login
    :param pw: the password for the username
    :param blogger: the collection of blogger given to that account
    :param start_time: the first datatime to keep
    :param end_time: the last datetime to keep
    :return: None
    '''
    L = instaloader.Instaloader(download_videos=False, max_connection_attempts=1000, download_geotags=True,
                                download_comments=False, compress_json=False)
    if os.path.exists('session-' + user):
        L.load_session_from_file(username=user, filename='session-' + user)
    else:
        L.login(user, pw)
        L.save_session_to_file(filename='session-' + user)

    j = 0

    # for each bloger, there is the download of all the post associated
    while (j < len(bloggerList)):
        try:
            blogger = instaloader.Profile.from_username(L.context, bloggerList[j])

            posts = blogger.get_posts()

            SINCE = start_time #datetime(2020, 6, 29)  # further from today, inclusive (da)
            UNTIL = end_time #datetime(2020, 7, 5)  # closer to today, not inclusive (a)

            k = 0  # initiate k
            k_list = []  # uncomment this to tune k

            for post in posts:
                postdate = post.date

                if postdate > UNTIL:
                    continue
                elif postdate <= SINCE:
                    k += 1
                    if k == 50:
                        break
                    else:
                        continue
                else:
                    time.sleep(5)
                    L.download_post(post,"Blogger")
                    k = 0  # set k to 0
                    # if you want to tune k, uncomment below to get your k max
                    # k_list.append(k)
            j = j + 1
        except:
            j = j+1

    return None

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('--choice',type=str, default='hashtag', help='Use "blogger" or "hashtag" to download posts from only one of them')
    args = parser.parse_args()

    # Leggo i vari account con i quali accedere a insta
    accounts = pd.read_csv('Accounts.csv', sep=',')

    # Leggo i vari hashtag
    with open('hashtag_list.txt', 'r') as f:
        contentH = f.readlines()
    f.close()
    contentH = [x.strip() for x in contentH]

    thread_list = []

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

        i = 0

        # Creo un numero di thread per il download dei post associati agli hashtag pari al numero di account dedicati
        for index, accounts_Hash in accounts_Hash.iterrows():
            thread_list.append(threading.Thread(target=instaloader_download_hash,
                                                args=(accounts_Hash['ACCOUNT'], accounts_Hash['PW'], hashtag_div[i]
                                                      ,
                                                      datetime(2020, 6, 1),
                                                      datetime(2020, 9, 1))))
            i = i + 1
        i = 0

    # Creo thread solo per il download di post associati ad hashtag

    if(args.choice == "hashtag"):

        # Divido gli hashtag in modo da darne ad ogni account un numero uguale
        hashtag_div = np.array_split(contentH, len(accounts))

        i = 0
        # Creo un numero di thread per il download dei post associati agli hashtag pari al numero di account dedicati
        for index, accounts in accounts.iterrows():
            thread_list.append(threading.Thread(target=instaloader_download_hash,
                                                args=(accounts['ACCOUNT'], accounts['PW'], hashtag_div[i]
                                                      ,
                                                      datetime(2020, 1, 1),
                                                      datetime(2020, 5, 30))))
            i = i + 1

    # Avvio i thread creati precedentemente
    for thread in thread_list:
        thread.start()

    for thread in thread_list:
        thread.join()
