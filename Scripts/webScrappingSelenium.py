import logging
import time

import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By

logging.basicConfig(level=logging.WARNING)


def get_driver():
    options = webdriver.ChromeOptions()
    # prefs = {
    #     "profile.managed_default_content_settings.images": 2,
    #     "disk-cache-size": 4096,
    # }
    # options.add_experimental_option("prefs", prefs)
    # options.add_argument("--disable-notifications")
    # options.add_argument("headless")
    driver = webdriver.Chrome(
        options=options,
        executable_path="./chromedriver-win64/chromedriver.exe",
    )
    # driver.execute_script("document.body.style.zoom='100%';")
    return driver


driver = get_driver()
main_url = "https://conference.dpw.ai/speakers/"

driver.get(main_url)
menu = driver.find_element(By.XPATH, ".//a[@data-glm-button-selector='#main']")
driver.execute_script("arguments[0].click();", menu)
speakers = driver.find_elements(By.XPATH, ".//article[@class='speaker']")
speakers_data_bis = []

for speaker in speakers:
    name = speaker.find_element(By.XPATH, "./div/h3").text
    print(name)
    title = speaker.find_element(By.XPATH, "./div/h4").text
    company_title = speaker.find_element(By.XPATH, "./div/div").text
    url = speaker.find_element(By.XPATH, "./a").get_attribute("href")
    speakers_data_bis.append([name, title, company_title, url])
    # speakers_url = [speaker.get_attribute("href") for speaker in speakers]

speakers_data = []
for speaker_url in speakers_data_bis:
    time.sleep(2)
    driver.get(speaker_url[3])
    name = driver.find_element(By.XPATH, ".//div[@class='content']/h1").text
    try:
        short_description = driver.find_element(
            By.XPATH, "/html/body/div[1]/div[1]/div/div/div"
        ).text
    except:
        short_description = None
    try:
        location = driver.find_element(
            By.XPATH, ".//div[@class='location']"
        ).text
    except:
        location = None
    try:
        building = driver.find_element(
            By.XPATH, ".//div[@class='building']"
        ).text
    except:
        building = None
    try:
        long_description = driver.find_element(
            By.XPATH, "/html/body/div[1]/div[2]/div/article/div"
        ).text
    except:
        long_description = None
    try:
        linkedin_account = driver.find_element(
            By.XPATH, "/html/body/div[1]/div[2]/div/article/nav/a"
        ).get_attribute("href")
    except:
        linkedin_account = None
    try:
        conference_date = driver.find_element(
            By.XPATH, ".//div[@class='events']/article/span"
        ).text
    except:
        conference_date = None
    try:
        conference_title = driver.find_element(
            By.XPATH, ".//div[@class='events']/article/h3"
        ).text
    except:
        conference_title = None
    try:
        conference_note = driver.find_element(
            By.XPATH, ".//div[@class='events']/article/div"
        ).text
    except:
        conference_note = None
    speakers_data.append(
        [
            name,
            short_description,
            location,
            building,
            long_description,
            conference_date,
            conference_title,
            conference_note,
            linkedin_account,
        ]
    )
driver.quit()

df_bis = pd.DataFrame(
    speakers_data_bis,
    columns=["name", "title", "company", "url"],
)

df = pd.DataFrame(
    speakers_data,
    columns=[
        "name",
        "short_description",
        "location",
        "building",
        "long_description",
        "conference_date",
        "conference_title",
        "conference_note",
        "linekin_account",
    ],
)
df = df_bis.merge(df, on="name", how="inner")
df.to_excel("./data/DPW_Amsterdam_2023_lineup.xlsx", index=False)
