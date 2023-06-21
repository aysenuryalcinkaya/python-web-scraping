import requests
from bs4 import BeautifulSoup
import threading

# Kaydedilen ISBN'leri tutmak için bir liste oluşturuyoruz
bookdata = []

# Threadler arasında veri paylaşımını sağlamak için bir kilit kullanacağız
lock = threading.Lock()

# Her bir iş parçacığı tarafından yürütülecek işlev
def worker(start):
    global bookdata

    # İstenilen aralıkta döngü oluşturuyoruz (100000 dahil)
    for i in range(start, 100001):
        url = f"https://www.nadirkitap.com/yaslilarda-rehabilitasyon-turkiye-gerontoloji-serisi-gokce-yagmur-gunes-gencer-kitap{i}.html"
        response = requests.get(url)

        # Sayfanın içeriğini analiz etmek için BeautifulSoup kullanıyoruz
        soup = BeautifulSoup(response.content, "html.parser")

        # ISBN'i arıyoruz
        isbn_element = soup.find("li", text="ISBN NO")
        if isbn_element:
            isbn = isbn_element.find_next_sibling("li").text.strip()
            with lock:
                bookdata.append(isbn)
        else:
            continue

        # Kaç tane kaydedildiğini yazdırıyoruz
        with lock:
            print(f"Toplam {len(bookdata)} ISBN kaydedildi.")

# 30 işçi iş parçacığı oluşturuyoruz
workers = []
for i in range(1, 31):
    t = threading.Thread(target=worker, args=(i,))
    workers.append(t)
    t.start()

# Tüm işçi iş parçacıklarının tamamlanmasını bekliyoruz
for t in workers:
    t.join()

# Elde edilen ISBN'leri yazdırıyoruz
print("ISBN'ler:")
for isbn in bookdata:
    print(isbn)
