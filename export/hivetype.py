#!/usr/bin/env python
# -*- coding:utf-8 -*-


class HiveType:

    @staticmethod
    def change_type(ctype):
        ctype = ctype.lower()
        if ctype in ("varchar", "char"):
            ctype = "string"
        if ctype in ("datetime",):
            ctype = "timestamp"
        if ctype == "timestamp":
            ctype = "string"
        if ctype in ("text", "longtext"):
            ctype = "string"
        if ctype == "time":
            ctype = "string"
        if ctype == "text":
            ctype = "string"
        if ctype in ("long", "int"):
            ctype = "bigint"
        if ctype in ("smallint", "mediumint", "tinyint"):
            ctype = "int"
        if ctype == ("decimal", "float"):
            ctype = "double"
        if ctype == "date":
            ctype = "string"
        if ctype == "array":
            ctype = "string"
        return ctype