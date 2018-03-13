# -*- coding:utf-8 -*-
__author__ = 'jerry'

'''
修改案例后，改代码可以跑
18.3.13
'''

import re

from neo4j.v1 import GraphDatabase, basic_auth

nodes={}
index=1

driver = GraphDatabase.driver("bolt://127.0.0.1:7687",auth=basic_auth("neo4j","root"))
session = driver.session()

file_object = open('r-graph.txt', 'r')
try:
    for line in file_object:
        matchObj = re.match( r'(\S+) -> (\S+)', line, re.M|re.I)
        if matchObj:
            ref = matchObj.group(1)
            path = matchObj.group(2)
        if path in nodes.keys(): # 如果该节点是已有节点
            path_node = nodes[path] #
        else: # 如果该节点未存在过
            path_node = "Page%d" % index #
            nodes[path] = path_node
            sql = "create (%s:Page {url:\"%s\" , id:\"%d\",in:0,out:0})" %(path_node,path,index)
            index=index+1
            session.run(sql)
            print sql
        if ref in nodes.keys(): # 如果该节点是已有节点
            ref_node = nodes[ref]
        else: # 节点不存在
            ref_node = "Page%d" % index
            nodes[ref] = ref_node
            sql = "create (%s:Page {url:\"%s\",id:\"%d\",in:0,out:0})" %(ref_node,ref,index)
            index=index+1
            session.run(sql)
            print sql
        # sql = "create (%s)-[r:IN]->(%s)" %(path_node,ref_node)
        # session.run(sql)
        # print sql
        sql = "match (n:Page {url:\"%s\"}) SET n.out=n.out+1" % ref # 来源页面设置出度为1
        session.run(sql)
        print sql
        sql = "match (n:Page {url:\"%s\"}) SET n.in=n.in+1" % path # 目标页面设置入度为1
        session.run(sql)
        print sql

        # 插入边,插入关系
        sql =  '''
        MATCH (a:Page),(b:Page)
        WHERE a.url = '{path}' AND b.url = '{ref}'
        CREATE (b)-[r:Point]->(a);
        '''.format(path=path,ref=ref)
        session.run(sql)
        print sql

finally:
     file_object.close( )

session.close()