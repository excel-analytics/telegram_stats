#encoding: utf-8

import os
import shutil
import argparse
from collections import Counter
import logging
import math
import re

import pymongo
import pymorphy2
from nltk.tokenize import RegexpTokenizer
from wordcloud import WordCloud
from stop_words import get_stop_words
from tqdm import tqdm


TOKENIZER = RegexpTokenizer(r'\w+')
URL_PATTERN = re.compile(r'https?:\/\/[^\s]*')

MORPH = pymorphy2.MorphAnalyzer()
STOPS = get_stop_words('ru')
STOPS.extend(get_stop_words('en'))
logging.basicConfig(level=logging.DEBUG)


def count_words(text):
    text = re.sub(URL_PATTERN, '', text)
    result_count = Counter()
    tokens = [MORPH.parse(token)[0].normal_form for token in TOKENIZER.tokenize(text)]
    tokens_no_stops = [token for token in tokens if token not in STOPS]
    result_count.update(tokens_no_stops)
    return result_count


def get_word_count_for_chat(chat_id):
    logging.info('Collecting word count stat for chat {}.'.format(chat_id))
    # Connect to chat content collection.
    content = pymongo.MongoClient()['tg_backup']['content']
    # Get all messages with text or media caption.
    all_messages_from_chat = content.find({
        'chat_id': chat_id,
        '$or': [
            {'$and': [{'media.caption': {'$ne': ''}}, {'media.caption': {'$exists': True}}]},
            {'text': {'$exists': True}}
        ]
    })
    word_counters_per_user = {'all': Counter()}
    pbar = tqdm(total=all_messages_from_chat.count(), unit='msg')
    for msg in all_messages_from_chat:
        text = msg.get('text', msg.get('media', {}).get('caption', ''))
        counts = count_words(text)
        word_counters_per_user['all'] += counts
        user_id = msg.get('from', {}).get('id', 'error')
        if user_id not in word_counters_per_user:
            word_counters_per_user[user_id] = Counter()
        word_counters_per_user[user_id] += counts
        pbar.update(1)
    pbar.close()
    return word_counters_per_user


def store_word_counters(chat_id, word_stat):
    logging.info('Storing word count stat for chat {}.'.format(chat_id))
    # Connecting to word count stat collection.
    word_stat_collection = pymongo.MongoClient()['tg_backup']['word_stat']
    if word_stat_collection.find_one({'chat_id': chat_id}) is None:
        # No stat exists
        word_stat_collection.insert_one({
            'chat_id': chat_id,
            'counters': {key.replace('$', ''): value for (key, value) in word_stat.items()}
        })
    else:
        # Just update stats
        word_stat_collection.find_one_and_update({'chat_id': chat_id},
            {'$set': {'counters': {key.replace('$', ''): value for (key, value) in word_stat.items()}}})


def make_word_clouds(chat_id):
    # Dir for word clouds
    word_clouds_dir = 'output'
    logging.info('Saving word clouds for chat {}.'.format(chat_id))
    # Connecting to word count stat collection.
    word_stat_collection = pymongo.MongoClient()['tg_backup']['word_stat']
    word_counters = word_stat_collection.find_one({
        'chat_id': chat_id
    })
    word_counters = word_counters['counters']
    if os.path.isdir(word_clouds_dir):
        shutil.rmtree(word_clouds_dir)
    os.makedirs(word_clouds_dir)
    picbar = tqdm(total=len(word_counters), unit='pic')
    for user_id, c in word_counters.items():
        # Docs: https://amueller.github.io/word_cloud/
        wc = WordCloud(width=1000, height=1000)
        wc.generate_from_frequencies(list(c.items()))
        wc.to_file(os.path.join(word_clouds_dir, str(user_id) + '.png'))
        picbar.update(1)
    picbar.close()


def tf_idf(chat_id):
    logging.info('Saving tf*idf for chat {}.'.format(chat_id))
    # Connecting to word count stat collection.
    word_stat_collection = pymongo.MongoClient()['tg_backup']['word_stat']
    word_counters = word_stat_collection.find_one({
        'chat_id': chat_id
    })
    word_counters = word_counters['counters']
    del word_counters['all']
    N = len(word_counters)
    all_terms = set()
    for user in word_counters:
        all_terms.update(word_counters[user].keys())

    users_with_term = dict()
    for term in all_terms:
        users_have = 0
        for user in word_counters:
            if term in word_counters[user].keys():
                users_have += 1
        users_with_term[term] = users_have
    tf_idf_per_user = dict()
    for user in word_counters.keys():
        total_user_words = sum(word_counters[user].values())
        tf_idf_per_user[user] = dict()
        for term in word_counters[user]:
            # tf = 1 + math.log(word_counters[user][term] / total_user_words)
            tf = word_counters[user][term] / total_user_words
            idf = math.log(0 + N / users_with_term[term])
            # idf = N / users_with_term[term]
            tf_idf_per_user[user][term] = tf * idf
    word_stat_collection.find_one_and_update({'chat_id': chat_id},
        {'$set': {'tf_idf': tf_idf_per_user}})


def print_top_words(chat_id, n=10):
    logging.info('Top words for chat {}.'.format(chat_id))
    word_stat_collection = pymongo.MongoClient()['tg_backup']['word_stat']
    tf_idf = word_stat_collection.find_one({
        'chat_id': chat_id
    })
    metadata = pymongo.MongoClient()['tg_backup']['metadata']
    tf_idf = tf_idf['tf_idf']
    for user in tf_idf.keys():
        name = metadata.find_one({'id': '$' + user})['print_name']
        print('\n===========================\n{}\n==========================='.format(name))
        terms = tf_idf[user]
        for term, rating in sorted(terms.items(), key=lambda x: x[1], reverse=True)[:n]:
            print('{term:<20}{rating:.5f}'.format(rating=rating, term=term))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--chat_id', type=str)
    parser.add_argument('-n', type=int)
    parser.add_argument('--word_count', action='store_true')
    parser.add_argument('--word_cloud', action='store_true')
    parser.add_argument('--tf_idf', action='store_true')
    parser.add_argument('--top_words', action='store_true')
    args = parser.parse_args()
    chat_id = '${}'.format(args.chat_id)
    if args.word_count:
        word_counters = get_word_count_for_chat(chat_id)
        store_word_counters(chat_id, word_counters)
    if args.word_cloud:
        make_word_clouds(chat_id)
    if args.tf_idf:
        tf_idf(chat_id)
    if args.top_words:
        print_top_words(chat_id, args.n)
