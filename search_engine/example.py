#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/10/30 0030 10:35
# @Author  : qiuyan
# @Site    : 
# @File    : example.py
# @Software: PyCharm


import os,datetime
import time

from sqlalchemy.orm import sessionmaker
from whoosh.index import create_in,open_dir
from whoosh.fields import TEXT,ID,IDLIST,Schema,DATETIME,STORED,NUMERIC
from whoosh.qparser import QueryParser
from whoosh.analysis import StemmingAnalyzer

from address_init import mysql_engine
from models.item_do import ItemDetail


class IndexSingleton(object):
    '''
    创建index 单例
    '''
    def __init__(self,*args,**kwargs):
        pass
    @classmethod
    def get_instance(cls, *args, **kwargs):
        if not hasattr(IndexSingleton, '_instance'):
            schema = Schema(id=NUMERIC(stored=True,unique=True),
                            update_time=DATETIME,
                            sell_count=NUMERIC,
                            show_id=,
                            title=TEXT(stored=True,analyzer=StemmingAnalyzer()))
            if not os.path.exists("sexyshine_index"):
                os.mkdir("sexyshine_index")
            ix = create_in("sexyshine_index", schema)
            print(time.time())
            IndexSingleton._instance = ix
        return IndexSingleton._instance


def index_item(ix,item):
    '''
    索引单项
    :param ix:
    :param item:
    :return:
    '''
    writer = ix.writer()
    writer.add_document(
        id=item.id,
        update_time=item.gmt_modified,#, '%Y-%m-%d %H:%M:%S'),
        sell_count=item.sell_count or 0,
        title=item.title
    )
    writer.commit()

def index_item_list(ix,data):
    for item in data:
        writer = ix.writer()
        print(item.id,item.title)
        writer.add_document(
            id=item.id,
            show_id=item.show_id,
            update_time=item.gmt_modified,
            sell_count=item.sell_count,
            price=item.price,
            title=item.title
        )
        writer.commit()

def search_keyword(ix,keyword):
    with ix.searcher() as searcher:
        query = QueryParser("title", ix.schema).parse(keyword)
        results = searcher.search(query)
        return results


def search_keywords(ix,keywords):
    with ix.searcher() as searcher:
        query = QueryParser("title", ix.schema).parse("first")
        results = searcher.search(query)
        print(results)


def index_items():
    Session = sessionmaker(bind=mysql_engine)
    session = Session()
    query = session.query(ItemDetail).filter(ItemDetail.deleted == 0)
    ix = IndexSingleton.get_instance()
    index_item_list(ix,query)
    print('success')



index_items()
# # # print(time.time())
# exit(0)
# ix = IndexSingleton.get_instance()
#
# myquery = And([Term("title",u"Sleeve")])
# with ix.searcher() as searcher:
#     results = searcher.search(myquery)
#     for result in results:
#         print(result)
# ix = open_dir('sexyshine_index')
# searcher=ix.searcher()
# print(list(searcher.lexicon("title")))
# exit(0)
# ix = open_dir('sexyshine_index')
# searcher=ix.searcher()
# query = QueryParser("title", ix.schema).parse('Sleeve')
# results = searcher.search(query)
# for result in results:
#     print(result.get('id'))
#     print(result)


    # results = searcher.search_page(query, 1, pagelen=100)
    # for result in results:
    #     print(result.get('id'))
    #     print(result)

# writer = ix.writer()
# writer.add_document(title)
# searcher = ix.searcher()
# print(list(searcher.lexicon("title")))
# exit(0)
# result = search_keyword(ix,"Tops")
# print(result._total)
# print(result.items())
# for r in result.items():
#     print(r)
# exit(0)
# print(dir(result))
'''
['__bool__', '__class__', '__contains__', '__delattr__', '__dict__', '__dir__', '__doc__', '__eq__', '__format__', 
'__ge__', '__getattribute__', '__getitem__', '__gt__', '__hash__', '__init__', '__init_subclass__', '__iter__',
 '__le__', '__len__', '__lt__', '__module__', '__ne__', '__new__', '__nonzero__', '__reduce__', '__reduce_ex__', 
 '__repr__', '__setattr__', '__sizeof__', '__str__', '__subclasshook__', '__weakref__', 
 '_char_cache', '_facetmaps', '_get_formatter', '_get_fragmenter', '_get_order', '_get_scorer', 
 '_set_formatter', '_set_fragmenter', '_set_order', '_set_scorer', '_total', 'collector', 'copy', 'docnum', 'docs',
  'docset', 'estimated_length', 'estimated_min_length', 'extend', 'facet_names', 'fields', 'filter', 'formatter', 
  'fragmenter', 'groups', 'has_exact_length', 'has_matched_terms', 'highlighter', 'is_empty', 'items', 'key_terms', 
  'matched_terms', 'order', 'q', 'query_terms', 'runtime', 'score', 'scored_length', 'scorer', 'searcher', 'top_n',
  'upgrade', 'upgrade_and_extend']

'''
# print(time.time())