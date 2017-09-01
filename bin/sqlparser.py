# !/usr/bin/env python
# -*- coding:utf-8 -*-

"""
调用 pip install sqlparse 解析SQL
"""

import sqlparse
from sqlparse.sql import IdentifierList, Identifier
from sqlparse.tokens import Keyword, DML


class SQLParser(object):
    @staticmethod
    def parse_sql_columns(sql):
        columns = []
        parsed = sqlparse.parse(sql)
        stmt = parsed[0]
        for token in stmt.tokens:
            if isinstance(token, IdentifierList):
                for identifier in token.get_identifiers():
                    columns.append(identifier.get_real_name())
            if isinstance(token, Identifier):
                columns.append(token.get_real_name())
            if token.ttype is Keyword:  # from
                break
        return columns


if __name__ == '__main__':
    sql = "select a as a1,b from t1,t2 as t2 where a > 20"
    columns = SQLParser.parse_sql_columns(sql)
    print columns
