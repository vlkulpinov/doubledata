import requests
import json

from dateutil import rrule
import datetime
import time

import pandas as pd

REQUEST_STR = 'https://api.vk.com/method/newsfeed.search?q={rest_name}&count=200&' \
              'latitude={lat}&longitude={lon}&version=5.62&extended=1&' \
              'fields=bdate,occupation,relation,sex&start_time={start_time}&end_time={end_time}'


def translit(locallangstring):
    """
    Translit any cyr characters into latin
    :param locallangstring: any string
    :return: string with cyrillic characters replaced
    """
    conversion = {
    u'\u0410': 'A', u'\u0430': 'a',
    u'\u0411': 'B', u'\u0431': 'b',
    u'\u0412': 'V', u'\u0432': 'v',
    u'\u0413': 'G', u'\u0433': 'g',
    u'\u0414': 'D', u'\u0434': 'd',
    u'\u0415': 'E', u'\u0435': 'e',
    u'\u0401': 'Yo', u'\u0451': 'yo',
    u'\u0416': 'Zh', u'\u0436': 'zh',
    u'\u0417': 'Z', u'\u0437': 'z',
    u'\u0418': 'I', u'\u0438': 'i',
    u'\u0419': 'Y', u'\u0439': 'y',
    u'\u041a': 'K', u'\u043a': 'k',
    u'\u041b': 'L', u'\u043b': 'l',
    u'\u041c': 'M', u'\u043c': 'm',
    u'\u041d': 'N', u'\u043d': 'n',
    u'\u041e': 'O', u'\u043e': 'o',
    u'\u041f': 'P', u'\u043f': 'p',
    u'\u0420': 'R', u'\u0440': 'r',
    u'\u0421': 'S', u'\u0441': 's',
    u'\u0422': 'T', u'\u0442': 't',
    u'\u0423': 'U', u'\u0443': 'u',
    u'\u0424': 'F', u'\u0444': 'f',
    u'\u0425': 'H', u'\u0445': 'h',
    u'\u0426': 'Ts', u'\u0446': 'ts',
    u'\u0427': 'Ch', u'\u0447': 'ch',
    u'\u0428': 'Sh', u'\u0448': 'sh',
    u'\u0429': 'Sch', u'\u0449': 'sch',
    u'\u042a': '"', u'\u044a': '"',
    u'\u042b': 'Y', u'\u044b': 'y',
    u'\u042c': '\'', u'\u044c': '\'',
    u'\u042d': 'E', u'\u044d': 'e',
    u'\u042e': 'Yu', u'\u044e': 'yu',
    u'\u042f': 'Ya', u'\u044f': 'ya',
    }
    translitstring = []
    for c in locallangstring:
        translitstring.append(conversion.setdefault(c, c))
    return ''.join(translitstring)


def main():

    ab_data = pd.read_excel('../../DD/data/A-B ID_rest vs Rest_attributes.xlsx', header=0)
    
    users_lst = []
    true_words = ['кафе', 'cafe', 'ресторан', 'restaurant', 'пицца', 'pizza', 'cалат', 'salad', 'бургер', 'burger',
                  'грибы', 'mushrooms', 'вкусн', 'новиков', 'novikov', 'паста', 'pasta', 'завтрак', 'ужин', 'breakfast',
                  'сэндвич', 'sandwich', 'атмосфера', 'tasty', 'кулина', 'bbq', 'барбекю', 'свинин', 'говядин',
                  ' мясо', ' рыб', ' cуши', 'пончик', ' кофе', 'coffee', 'латте', 'лимонад', 'овощи']


    for rest in ab_data.itertuples():

        rest_name = rest.Rest_name
        lat = float(rest.Latitude)
        lon = float(rest.Longitude)
        nov = int(rest.Novikov)

        rest_id = rest.ID_rest

        # Novikov Group one love
        if nov != 1:
            continue

        print(rest_name, nov)
        print(rest_name)

        cnt = 0
        for dt in rrule.rrule(rrule.DAILY, dtstart=datetime.datetime.today() - datetime.timedelta(days=9 * 10), 
                              count=10 * 9 / 3 + 1, 
                              interval=3):
            time.sleep(1) # VK API Requirements
            print('%s/%s' % (cnt, 30))
            cnt += 1

            unix_from = str(int(time.mktime(dt.timetuple())))
            unix_to = str(int(time.mktime((dt + datetime.timedelta(days=3)).timetuple())))

            names_list = [rest_name]
            if rest_name != translit(rest_name):
                names_list.append(translit(rest_name))
            for name in names_list:
                if cnt < 30:
                    url = REQUEST_STR.format(
                        rest_name=name, lat=lat, lon=lon, start_time=unix_from, end_time=unix_to
                    )
                else:
                    # any previous news with one request
                    url = REQUEST_STR.format(
                        rest_name=name,
                        lat=lat,
                        lon=lon,
                        start_time=str(int(time.mktime((datetime.datetime.today() - datetime.timedelta(days=360)).timetuple()))),
                        end_time=str(int(time.mktime((datetime.datetime.today() - datetime.timedelta(days=9*10)).timetuple())))
                    )
                    
                r = requests.get(url)
                j = json.loads(str(r.content.decode("utf-8")))

                for item in j['response'][1:]:
                    try:
                        # check whether this is about retaurants
                        is_restaurant = False
                        if len(item['text']) < 30:
                            is_restaurant = True
                        else:
                            for w in true_words:
                                if w in item['text'].lower():
                                    is_restaurant = True
                                    print(w)
                                    break

                        if not is_restaurant:
                            continue

                        print(item['text'])

                        user = item['user']
                        user_id = user.get('uid', '0')
                        user_sex = int(user.get('sex', 0))
                        users_lst.append([rest_id, user_id, user_sex])
                    except:
                        continue

    users = pd.DataFrame(data=users_lst, columns=['Rest_id', 'VK_ID', 'VK_sex'])
    users[['VK_sex']] = users[['VK_sex']].astype(int)
    users.to_csv('test.csv', index=False)

if __name__ == '__main__':
    main()