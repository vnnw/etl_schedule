# -*- coding:utf-8 -*-
# !/usr/bin/env python

import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from optparse import OptionParser
import xlsxwriter
import smtplib
import email.MIMEMultipart
import email.MIMEText
import email.MIMEBase
import traceback
import pyhs2
from bin.configutil import ConfigUtil

DATA_SPLIT = "|"

configUtil = ConfigUtil()

'''
传递参数说明
'''


def option_parser():
    usage = "usage: %prog [options] arg1 arg2"

    parser = OptionParser(usage=usage)

    parser.add_option("-n", "--name", dest="name", action="store", type="string",
                      help="excel name")
    parser.add_option("-s", "--subject", dest="subject", action="store", type="string",
                      help="email subject")
    parser.add_option("-c", "--content", dest="content", action="store", type="string",
                      help="email content")
    parser.add_option("-t", "--tables", dest="tables", action="store", type="string",
                      help="hive table split by comma")
    parser.add_option("-r", "--receivers", dest="receivers", action="store", type="string",
                      help="receiver email split by comma")
    return parser


'''
解析参数
'''


def split_args(options, args):
    name = options.name.strip()
    tables = options.tables.strip()
    receivers = options.receivers.strip()
    if name is None or len(name) == 0:
        raise Exception("excel name none")
    if tables is None or len(tables) == 0:
        raise Exception("hive tables none")
    if receivers is None or len(receivers) == 0:
        raise Exception("receivers none")
    tables_array = tables.split(",")
    receivers_array = receivers.split(",")
    return (name, tables_array, receivers_array)


'''
[{}]
'''


def desc_colums(connection, table):
    cursor = connection.cursor()
    cursor.execute("desc " + table)
    rows = cursor.fetch()
    col_list = []
    for row in rows:
        col_list.append({"col_name": row[0], "col_comment": row[2]})
    cursor.close()
    return col_list


def hive_connection(db):
    host = configUtil.get("hive.host")
    port = configUtil.get("hive.port")
    connection = pyhs2.connect(host=host,
                               port=int(port),
                               authMechanism="PLAIN",
                               user="hadoop",
                               password="hadoop",
                               database=db)
    return connection


'''
{"table_name":{"columns:":[comment1,comment2],"data":[]}}
'''


def query_table(name, tables):
    excel_path = configUtil.get("tmp.path") + "/excel/" + name + ".xlsx"
    if os.path.exists(excel_path):
        os.remove(excel_path)
    workbook = xlsxwriter.Workbook(excel_path)
    for sheet, table in enumerate(tables):
        db_name = table.split(".")[0]
        table_name = table.split(".")[1]
        connection = hive_connection(db_name)
        col_list = desc_colums(connection, table)
        # print col_list
        cursor = connection.cursor()
        col_select_name = []
        col_show_name = []
        for col in col_list:
            col_select_name.append(col["col_name"])
            col_show_name.append(col["col_comment"])
        sql = "select " + ",".join(col_select_name) + " from " + table
        print("sql:" + sql)
        cursor.execute(sql)
        rows = cursor.fetch()
        list_data = []
        for row in rows:
            data = []
            for index, val in enumerate(col_select_name):
                data.append(str(row[index]))
            list_data.append(DATA_SPLIT.join(data))
        # print list_data
        # print cursor.getSchema()
        # write2file(",".join(col_show_name),list_data)
        sheet_name = "工作表" + str(sheet + 1)
        write2excel(workbook, sheet_name, col_show_name, list_data)
    workbook.close()
    return excel_path


def is_float(data):
    try:
        float(data)
        return True
    except Exception, e:
        return False


def is_int(data):
    try:
        int(data)
        return True
    except Exception, e:
        return False


'''
写入数据到excel
'''


def write2excel(workbook, sheet_name, head, data):
    worksheet = workbook.add_worksheet(sheet_name)
    # write head
    for index, head_str in enumerate(head):
        worksheet.write_string(0, index, head_str)
    # write data
    for index, data_str in enumerate(data):
        for data_col_index, data_col in enumerate(data_str.split(DATA_SPLIT)):
            if is_int(data_col):
                worksheet.write_number(index + 1, data_col_index, (int)(data_col))
            if is_float(data_col):
                worksheet.write_number(index + 1, data_col_index, (float)(data_col))
            else:
                worksheet.write_string(index + 1, data_col_index, data_col)


'''
 发送邮件
'''


def send_email(subject, content, excel_path, receivers):
    contype = 'application/octet-stream'
    maintype, subtype = contype.split('/', 1)
    server = smtplib.SMTP("smtp.263.net")
    server.login("noreply@yunniao.me", "1111aaaa")
    main_msg = email.MIMEMultipart.MIMEMultipart()
    text_msg = email.MIMEText.MIMEText(content, 'plain', "utf-8")
    # email text
    main_msg.attach(text_msg)
    #  email attach
    print "excel path:" + str(excel_path)
    data = open(excel_path, 'rb')
    file_msg = email.MIMEBase.MIMEBase(maintype, subtype)
    file_msg.set_payload(data.read())
    data.close()
    email.Encoders.encode_base64(file_msg)
    basename = os.path.basename(excel_path)
    file_msg.add_header('Content-Disposition', 'attachment', filename=basename)
    main_msg.attach(file_msg)
    main_msg["Accept-Language"] = "zh-CN"
    main_msg["Accept-Charset"] = "utf-8"
    main_msg['From'] = "noreply@yunniao.me"
    main_msg['To'] = ",".join(receivers)
    main_msg['Subject'] = subject
    fullText = main_msg.as_string()
    server.sendmail("noreply@yunniao.me", receivers.split(","), fullText)
    server.quit()


'''
 for test
'''


def write2file(head, data):
    file_handler = open("/home/hadoop/yangxl/script/excel.csv", 'w')
    file_handler.write(head + '\n')
    file_handler.flush()
    for line in data:
        file_handler.write(line + '\n')
    file_handler.flush()
    file_handler.close()


if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding('utf-8')

    # fakeArgs = ["-s","财务月度数据" ,"-c","详细数据见附件","-n","财务","-t", "tmp.tmp_bid_task_agg_month,tmp.tmp_bid_task_month", '-r', "yangxiaolong@yunniao.me"]

    optParser = option_parser()

    # options, args = optParser.parse_args(fakeArgs)
    options, args = optParser.parse_args(sys.argv[1:])

    print options

    if options.subject is None:
        print("require email subject")
        optParser.print_help()
        sys.exit(-1)
    if options.content is None:
        print("require email content")
        optParser.print_help()
        sys.exit(-1)
    if options.name is None:
        print("require excel name")
        optParser.print_help()
        sys.exit(-1)
    if options.tables is None:
        print("require hive table split by comma")
        optParser.print_help()
        sys.exit(-1)
    if options.receivers is None:
        print("require receiver split  by comma")
        optParser.print_help()
        sys.exit(-1)

    try:
        (name, tables, receivers) = split_args(options, args)
        excel_path = query_table(name, tables)
        send_email(options.subject.strip(), options.content.strip(), excel_path, receivers)

    except Exception, e:
        print traceback.format_exc()
        sys.exit(-1)
