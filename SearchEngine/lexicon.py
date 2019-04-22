import jieba
import mysql.connector
import redis
import threading

word_dict = {}

r = redis.Redis(host="127.0.0.1", port=6379, db=0)

mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        passwd="121314",
        database="spider"
        )
mycursor = mydb.cursor()

mycursor.execute("SELECT id, data FROM page")
result = mycursor.fetchall()
mid = int(len(result)/2)


def lexicon(idd, data):
    print("Lexiconing and building index") 
    word_list = jieba.cut_for_search(str(data))

    # 标点符号和停用词
    with open('stop_words.txt', 'rb') as f:
        stop_words = [line.strip() for line in f.readlines()]

    for item in word_list:
        if item not in stop_words:
            if item not in word_dict:
                word_dict[item] = {}
                word_dict[item][idd] = 1               
                r.hset(item, idd, 1)
            else:
                if idd not in word_dict[item]:
                    word_dict[item][idd] = 1
                    r.hset(item, idd, 1)
                else:
                    word_dict[item][idd] += 1
                    r.hincrby(item, idd, 1)
    # word_dict = sorted(word_dict.items(), key=lambda item:item[1], reverse = True)
    # print(word_dict)

def run(start, end):
    for i in result[start: end]:
        lexicon(i[0], i[1])


if __name__ == "__main__":
    # for row in result:
    #     lexicon(row[0], row[1])
    threads = []
    t1 = threading.Thread(target=run, args=(0, mid))
    threads.append(t1)
    t2 = threading.Thread(target=run, args=(mid, len(result)-mid))
    threads.append(t2)

    for t in threads:
        t.start()

