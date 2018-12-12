import aiohttp
import asyncio
import json
import os
import re
import requests
import ssl
import sys
from bs4 import BeautifulSoup
from datetime import date, datetime, timedelta

sitemapfmt = "https://www.reuters.com/sitemap_{}-{}.xml"
datefmt = "%Y%m%d"

max_request_semaphore = asyncio.BoundedSemaphore(100)
max_coroutines = asyncio.BoundedSemaphore(100)

def log(s):
    print(datetime.utcnow(), end=" ")
    print(s)

def initialize_sitemaps(start = "2007-01-01", end = "2018-11-30"):
    sitemaps = []
    date = datetime.strptime(start, "%Y-%m-%d")
    while date < datetime.strptime(end, "%Y-%m-%d"):
        next_date = date + timedelta(days=1)
        sitemap = sitemapfmt.format(date.strftime(datefmt), next_date.strftime(datefmt))
        sitemaps.append(sitemap)
        date = next_date
    return sitemaps

async def get_response_async(url):
    async with max_request_semaphore:
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url, ssl=ssl.create_default_context(purpose=ssl.Purpose.CLIENT_AUTH)) as resp:
                    return await resp.text()
            except aiohttp.client_exceptions.ClientConnectorError as e:
                return None

async def scrape_urls_from_sitemap(sitemap):
    text = await get_response_async(sitemap)
    if text is None:
        return None, "bad response"
    soup = BeautifulSoup(text, "lxml")
    print(soup.urlset)
    if soup.urlset is None:
        return None, "bad sitemap"
    return [x.loc.text for x in soup.urlset.find_all("url")]

async def scrape_article_from_url(url):
    text = await get_response_async(url)
    log(f"\tLoading {url}")
    if text is None:
        return {"url": url, "error": "bad response"}
    soup = BeautifulSoup(text, "lxml")
    date, headline, body = [soup.find(attrs={"class":x}) for x in ["ArticleHeader_date", "ArticleHeader_headline", "StandardArticleBody_body"]]
    if date is None or headline is None or body is None:
        return {"url": url, "error": "bad article"}
    date = datetime.strptime(date.text.split(" / ")[0], "%B %d, %Y")
    headline = headline.text
    body = [x.text for x in body.find_all("p")]
    return {"url": url, "date":date, "headline":headline, "body":body}

def save_article_to_txt(article):
    name = article["url"].split("/")[-1]
    year, month, day = article["date"].year, article["date"].month, article["date"].day
    dirpath = f"corpus/{year}/{month:02}/{day:02}"
    if not os.path.isdir(dirpath):
        os.makedirs(dirpath)
    with open(os.path.join(dirpath, name + ".txt"), "w") as f:
        f.write(article["headline"])
        if article["headline"][-1] != ".": f.write(".")
        f.write(" ")
        f.write(" ".join(article["body"]))

async def main():
    assert len(sys.argv) >= 3
    sitemaps = initialize_sitemaps(sys.argv[1], sys.argv[2])
    badSitemapsLog = open("bad_sitemaps.log", "w")
    badArticlesLog = open("bad_articles.log", "w")
    for j, sitemap in enumerate(sitemaps):
        sys.stdout.flush()
        urls = await scrape_urls_from_sitemap(sitemap)
        if urls[0] is None: 
            log(f"Failed to scrape from sitemap {sitemap} ({urls[1]}")
            badSitemapsLog.write(f"{urls[1]}\t{sitemap}\n")
            continue
        log(f"Scraped from sitemap {sitemap}")
        future_articles = []
        for i, url in enumerate(urls):
            sys.stdout.flush()
            await max_coroutines.acquire()
            future_articles.append(asyncio.ensure_future(scrape_article_from_url(url)))
            max_coroutines.release()
        sys.stdout.flush()
        await asyncio.gather(*future_articles)
        failures = []
        for future_article in future_articles:
            article = future_article.result()
            if article.get("error") is None:
                save_article_to_txt(article)
            else:
                failures.append(article)
        if failures:
            log(f"Warning: the following {len(failures)} urls (out of {len(urls)}) could not be loaded:")
            for failure in failures:
                errstr = f"{failure['error']}\t{failure['url']}"
                log(errstr)
                badArticlesLog.write(errstr + "\n")
    badSitemapsLog.close()
    badArticlesLog.close()

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
