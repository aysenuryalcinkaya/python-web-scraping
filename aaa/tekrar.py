import asyncio
import aiohttp
import time
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import subprocess

base_url = 'https://www.kitapyurdu.com/'
start_url = 'https://www.kitapyurdu.com/index.php?route=product/category&page=1&filter_category_all=true&path=1&filter_in_stock=1&sort=purchased_365&order=DESC&limit=100'

links = []

async def fetch(session, url):
    async with session.get(url) as response:
        return await response.text()

async def parse_page(url):
    async with aiohttp.ClientSession() as session:
        html = await fetch(session, url)
        soup = BeautifulSoup(html, 'html.parser')
        pr_img_links = soup.find_all('a', class_='pr-img-link')
        for link in pr_img_links:
            absolute_link = urljoin(base_url, link['href'])
            links.append(absolute_link)

async def scrape_pages(start_url):
    async with aiohttp.ClientSession() as session:
        html = await fetch(session, start_url)
        soup = BeautifulSoup(html, 'html.parser')
       
        tasks = []

        for page in range(1, 1390):
            page_url = f'https://www.kitapyurdu.com/index.php?route=product/category&page={page}&filter_category_all=true&path=1&filter_in_stock=1&sort=purchased_365&order=DESC&limit=100'
            task = asyncio.create_task(parse_page(page_url))
            tasks.append(task)

        await asyncio.gather(*tasks)

start_time = time.time()

loop = asyncio.get_event_loop()
loop.run_until_complete(scrape_pages(start_url))

end_time = time.time()
execution_time = end_time - start_time

print(f"Toplamda {len(links)} link toplandı.")
print(f"İşlem süresi: {execution_time:.2f} saniye.")

# İstek yapan dosyayı çalıştırma
subprocess.run(["python", "istek_yapan_dosya.py"])
