#encoding: utf-8

import pymongo


def main(from_id, to_id):
    from_id = '${}'.format(from_id)
    to_id = '${}'.format(to_id)
    content = pymongo.MongoClient()['tg_backup']['content']
    content.update_many({'chat_id': from_id}, {'$set': {'chat_id': to_id}})


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--from_id', type=str)
    parser.add_argument('--to_id', type=str)
    args = parser.parse_args()
    main(args.from_id, args.to_id)
