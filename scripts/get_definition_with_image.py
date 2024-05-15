import argparse
import csv
import logging
import time
import random

import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3 import Retry

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/58.0.3029.110 Safari/537.3',
    'Connection': 'keep-alive',
    'Accept-Encoding': 'gzip, deflate'
}


def create_session():
    session = requests.Session()
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    return session


def fetch_word_data(word: str, pos: str, session: requests.Session):
    soup = None
    try:
        with session.get(headers=headers, allow_redirects=True,
                         url="https://www.oxfordlearnersdictionaries.com/search/english/direct/",
                         params={'q': word},
                         timeout=5) as response:
            soup = BeautifulSoup(response.text, features="html.parser")
            pos_of_soup = soup.find('span', class_='pos').text.strip()
            word_of_soup = soup.find('h1', class_='headword').text

            if not (word == word_of_soup and pos == pos_of_soup):
                for item in soup.select('.responsive_row.nearby ul.list-col li'):
                    word_data = item.find('data', class_='hwd')
                    if word_data:
                        full_text = word_data.get_text(" ", strip=True)
                        pos_text = word_data.find('pos').get_text(" ", strip=True) if word_data.find('pos') else ''
                        nearly_word = full_text.replace(pos_text, '').strip()
                        nearly_word_pos = pos_text.strip().strip()
                        nearly_word_link = item.find('a')['href']

                        if word == nearly_word and pos == nearly_word_pos:
                            with session.get(headers=headers, allow_redirects=True,
                                             url=nearly_word_link, timeout=5) as response1:
                                soup = BeautifulSoup(response1.text, features="html.parser")
                            break
    except requests.exceptions.ConnectionError as e:
        logging.error(f"连接错误: {e}")
    except requests.exceptions.Timeout as e:
        logging.error(f"请求超时: {e}")
    return soup


def get_definitions(word: str, pos: str, session: requests.Session):
    soup = fetch_word_data(word, pos, session)
    if soup is None:
        return []

    sense_items = soup.find_all('li', class_='sense')
    extracted_data_with_images = []
    for item in sense_items:
        definition_span = item.find('span', class_='def')
        if definition_span:
            definition_text = definition_span.text.strip()
        else:
            continue  # 如果找不到定义，则跳过此项

        image_tag = item.find('img', class_='thumb')
        image_url = image_tag['src'] if image_tag else None

        cefr_level = item.get('cefr')
        if not cefr_level:
            cefr_level = item.get('fkcefr', 'Not specified')
        cefr_level = cefr_level.upper()

        sense_data = {
            "word": word,
            "part_of_speech": pos,
            "cefr_level": cefr_level,
            "definition": definition_text,
            "image": image_url
        }
        extracted_data_with_images.append(sense_data)
    return extracted_data_with_images


def main(input_file, output_file, images_dir):
    session = create_session()

    part_of_speech_mapping = {
        "n.": "noun",
        "pron.": "pronoun",
        "v.": "verb",
        "adj.": "adjective",
        "prep.": "preposition",
        "adv.": "adverb",
        "conj.": "conjunction",
        "exclam.": "exclamation",
        "article": "article",
        "number": "number",
        "det.": "determiner",
        "marker": "marker",
        "at": "at"
    }

    with open(output_file, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=['word', 'part_of_speech', 'cefr_level', 'definition', 'image'])
        writer.writeheader()

        for chunk in pd.read_csv(input_file, chunksize=10):
            for _, row in chunk.iterrows():
                word = row['Word']
                pos_abbreviation = row['Part of Speech']
                pos = part_of_speech_mapping.get(pos_abbreviation, pos_abbreviation)
                logging.info(f"开始处理: {word}, 词性: {pos}")
                definitions = get_definitions(word, pos, session)

                for definition in definitions:
                    if definition['cefr_level'] == row['CEFR Level']:
                        writer.writerow(definition)
                        logging.info(
                            f"已处理并写入: {definition['definition']} - {word}, 词性: {pos}, CEFR级别: {definition['cefr_level']}")
                time.sleep(random.uniform(1.0, 3.0))
    logging.info("所有单词处理完成，结果已保存。")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="处理词汇并提取定义及图片")
    parser.add_argument("input_file", help="输入的CSV文件路径")
    parser.add_argument("output_file", help="输出的CSV文件路径")
    parser.add_argument("--images_dir", help="保存图片的目录", default="images")

    args = parser.parse_args()

    main(args.input_file, args.output_file, args.images_dir)
