#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/10/30 0030 15:27
# @Author  : qiuyan
# @Site    : 
# @File    : search_engine.py
# @Software: PyCharm

import os
from whoosh.index import create_in, open_dir
from whoosh.qparser import QueryParser, MultifieldParser
from whoosh import sorting
from conf.search_engine import INDEX_NAME,SCHEMA

class SearchEngine(object):

    def __init__(self):
        pass

    def create_index(self,schema=SCHEMA,index_name=INDEX_NAME):
        '''
        创建索引
        :param schema:
        :param index_name:
        :return:
        '''
        # creating the index
        if not os.path.exists(index_name):
            os.mkdir(index_name)
        ix = create_in(index_name, schema)
        return True,ix

    def get_index(self,index_name=INDEX_NAME):
        '''
        获取索引名称
        :param index_name:
        :return:
        '''
        ix = open_dir(index_name)
        return ix

    def add_field(self,field_name,):
        pass

        # ix = self.get_index()
        # writer = ix.writer()
        # writer.add_field(field_name, fields.ID(stored=True))
        # writer.commit(optimize=True)

    def del_field(self,field_name):
        '''
        删除字段
        :param field_name:
        :return:
        '''
        ix = self.get_index()
        writer = ix.writer()
        writer.remove_field(field_name)
        writer.commit(optimize=True)

    def add_document(self,item):
        '''
        添加单条索引
        :param item:
        :return:
        '''
        ix = self.get_index(INDEX_NAME)
        writer = ix.writer()
        writer.add_document(
            id=item.id,
            show_id=item.show_id,
            update_time=item.gmt_modified,
            sell_count=item.sell_count,
            price=item.price,
            title=item.title
        )
        writer.commit(optimize=True)

    def add_documents(self,items):
        '''
        添加索引集合
        :param items:
        :return:
        '''
        ix = self.get_index(INDEX_NAME)
        writer = ix.writer()
        for item in items:
            writer.add_document(
                id=item.id,
                show_id=str(item.show_id),
                update_time=item.gmt_modified,
                sell_count=item.sell_count,
                price=item.price,
                title=item.title
            )
        writer.commit(optimize=True)

    def del_document(self,item_id):
        '''
        删除索引
        :param item_id:
        :return:
        '''
        ix = self.get_index()
        ix.delete_by_term('id',item_id)
        ix.commit()

    def update_document(self,item):
        '''
        更新缩影文档
        :param item:
        :return:
        '''
        ix = self.get_index(INDEX_NAME)
        writer = ix.writer()
        writer.update_document(id=item.id,
                show_id=item.show_id,
                update_time=item.gmt_modified,
                sell_count=item.sell_count,
                price=item.price,
                title=item.title)
        writer.commit()

    def search_by_keyword(self,keyword,sort_field=None,reverse=False,page=1,pageSize=50):
        '''
        关键词搜索
        :param keyword:
        :return:
        '''
        ix = self.get_index()
        searcher = ix.searcher()
        qp = QueryParser("title", schema=ix.schema)
        q = qp.parse(keyword)
        with searcher as s:
            if sort_field:
                facet = sorting.FieldFacet("price", reverse=reverse)
                results = s.search_page(q, page, pagelen=pageSize, sortedby=facet)
            else:
                results = s.search_page(q, page, pagelen=pageSize)
                #results = s.search(q)
                '''
                    sizes = sorting.FieldFacet("size")
                    prices = sorting.FieldFacet("price", reverse=True)
                    results = searcher.search(myquery, sortedby=[sizes, prices])
                '''
            taotal = results.scored_length()
            if taotal>0:
                return [{'id':r.get('id'),'title':r.get('title')} for r in results]
            return []

    def search_by_keywords(self,keywords):
        ix = self.get_index()
        parser = QueryParser("title", schema=ix.schema)
        parser.parse(u"alpha OR beta gamma")

        #And([Or([Term('content', u'alpha'), Term('content', u'beta')]), Term('content', u'gamma')])

    def search_more_fields(self,keywords,page=1,pageSize=50):
        '''
        多字段查询
        :param keywords:
        :param page:
        :param pageSize:
        :return:
        '''
        ix = self.get_index()
        searcher = ix.searcher()
        qp = MultifieldParser(["title", "content"], schema=ix.schema)
        q = qp.parse(keywords)
        with searcher as s:
            results = s.search_page(q, page, pagelen=pageSize)
            taotal = results.scored_length()
            if taotal > 0:
                return [{'id': r.get('id'), 'title': r.get('title')} for r in results]
            return []