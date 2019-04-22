import redis
import mysql.connector
import jieba

r = redis.Redis(host="127.0.0.1", port=6379, db=0)

mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        passwd="121314",
        database="spider"
        )
mycursor = mydb.cursor()

hits_count = {}

def search(query):
    word_list = jieba.cut_for_search(query)
    for word in word_list:
        dict = r.hgetall(word)
        for key, value in dict.items():
            if key in hits_count:
                hits_count[key] += value
            else:
                hits_count[key] = value
    result1 = sorted(hits_count.items(), key=lambda x:x[1], reverse = True)
    
    for i in range(10):
        tmp = str(result1[i][0], encoding = "utf-8")
        mycursor.execute("SELECT `url` FROM `page` WHERE `id` = '" + tmp + "'")
        result2 = mycursor.fetchone()
        print(result2)
        print

if __name__ == '__main__':
    while(True):
        query = input('Search: ')
        print
        search(query)