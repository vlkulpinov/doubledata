import requests
import re
import multiprocessing


def worker(q, q2):
    while True:
        x = q.get()
        if x is None:
            q.task_done()
            q2.put(None)
            return
        r = main2(address=x).split()
        q2.put(r)
        q.task_done()


def worker2(q2):
    fw = open('../data/address5.txt', 'a')
    while True:
        x = q2.get()
        if x is None:
            q2.task_done()
            fw.close()
            return

        print('\t'.join(x), file=fw)
        q2.task_done()


def main2(address):
    # address = "Москва, Никольская, 12, гостиница The St. Regis Moscow Nikolskaya, 1 этаж"
    r = requests.get("https://geocode-maps.yandex.ru/1.x/?geocode=" + address)
    # print(r.content)

    body = str(r.content)
    R = re.compile(r'<pos>(.*?)</pos>')
    try:
        res = R.findall(body)[0]
        return res
    except:
        return ""

def main():
    q = multiprocessing.JoinableQueue(maxsize=50)
    q2 = multiprocessing.JoinableQueue(maxsize=50)

    pp = []

    for i in range(10):
        p = multiprocessing.Process(target=worker, args=(q, q2))
        p.start()
        pp.append(p)

    for i in range(1):
        p = multiprocessing.Process(target=worker2, args=(q2, ))
        p.start()
        pp.append(p)

    # f2 = open('../data/address4.txt', 'w')
    cnt = 0
    with open('../data/address.txt', 'r') as f:
        for line in f:
            name = line.split(';')[0]
            addr = line.split(';')[1]
            if cnt >= 0:
                q.put(str(name) + ' ' + str(addr))
                # geo = main2(str(name) + ' ' + str(addr)).split()
                # print('\t'.join(geo), file=f2)

            cnt += 1
            if cnt % 100 == 0:
                print(cnt)

    # f2.close()
    q.put(None)
    for p in pp:
        p.join()



if __name__ == '__main__':
    main()
    # print(main2(None))