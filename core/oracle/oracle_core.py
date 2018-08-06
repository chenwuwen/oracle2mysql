import cx_Oracle
import os, datetime

# from bin.bin import base_dir, log_name

from core.mysql.mysql_core import insertJLX, insertRoom, insertJZW

oracle_username = 'wuwen'
oracle_password = 'wuwen'
oracle_host = '192.168.43.17'

oracle_port = 1521
oracle_dbname = 'orcl'

oracle_cursor = None

oracle_conn = None

base_dir = os.path.dirname(__file__) + "/"

log_name = "log.txt"


# Oracle连接
def oracle_connection():
    dsn = cx_Oracle.makedsn(oracle_host, oracle_port, oracle_dbname)
    print("Oracle 链接 dsn为 %s" % (dsn))
    global oracle_conn
    oracle_conn = cx_Oracle.connect(oracle_username, oracle_password, dsn)
    global oracle_cursor
    oracle_cursor = oracle_conn.cursor()


def select():
    # 需要注意的是sql后不能加分号
    sql = ''' select  * from t_bzdz_exp where jlxdm ='4DA9663123AEF0F2E0536902080A23B7' '''

    oracle_cursor.execute(sql)
    success_count = 0
    error_count = 0
    while True:
        result = oracle_cursor.fetchone()
        if not result:
            print("=====完成========")
            oracle_cursor.close()
            oracle_conn.close()
            break

        src_dic = {'PCSDM': result[0], 'PCSMC': result[1], 'JWHDM': result[2], 'JWHMC': result[3], 'DZYSQX': result[4],
                   'JLXDM': result[5], 'JLXMC': result[6], 'MLPHID': result[7], 'MLPHDZ': result[8], 'DYMC': result[9],
                   'HUMC': result[10], 'DZYSID': result[11], 'BZDZQC': result[12], 'DYS': result[13], 'CS': result[14],
                   'HS': result[15], 'XT_LRSJ': result[16], 'XT_ZHXGSJ': result[17]}

        jlx_dic = {'jlxmc': src_dic['JLXMC'], 'jlxdm': src_dic['JLXDM'], 'dzysqx': src_dic['DZYSQX'],
                   'pcsdm': src_dic['PCSDM'], 'pcsmc': src_dic['PCSMC'], 'sssq_dm': '', 'sssq_mc': '',
                   'jwhdm': src_dic['JWHDM'], 'jwhmc': src_dic['JWHMC'], 'jwzrqdm': '', 'jwzrqmc': ''}

        jzw_dic = {'ds_cs': src_dic['CS'], 'ds_dys': src_dic['DYS'], 'ds_hs': src_dic['HS'], 'dx_cs': 0, 'dx_dys': 0,
                   'dx_hs': 0, 'jwh_dm': src_dic['JWHDM'],
                   'jwh_mc': src_dic['JWHMC'], 'jzw_bm': src_dic['MLPHID'], 'jzw_dizhi': src_dic['MLPHDZ'],
                   'jzw_jlxdm': src_dic['JLXDM'], 'jzw_jlxmc': src_dic['JLXMC'], 'pcsdm': src_dic['PCSDM'],
                   'pcsmc': src_dic['PCSMC'], 'sssq_dm': '', 'sssq_mc': ''}

        room_dic = {'jzw_dm': src_dic['MLPHID'], 'hu_id': src_dic['DZYSID'],
                    'jzw_dizhi': src_dic['JLXMC'] + src_dic['MLPHDZ'], 'dys': src_dic['DYS'], 'cs': src_dic['CS'],
                    'hs': src_dic['HS'], 'humc': src_dic['HUMC']}
        try:

            insertJLX(jlx_dic)
            insertJZW(jzw_dic)
            insertRoom(room_dic)
            success_count += 1
        except Exception as e:
            print(e)
            error_count += 1
            log(src_dic, e)

        finally:

            print("已成功插入记录总数:%d" % (success_count))
            print("失败记录总数:%d" % (error_count))

            # 元组类型
            # print(result)
            # print(type(result))
            # print(src_dic)


def log(dic):
    with open(base_dir + log_name, 'w+', encoding='utf-8') as data:
        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        item = []
        for key, val in dic.items():
            item.append(" " + key + ":" + str(val) + " ")
        content = now + "\t"  '\t'.join(item) + "\n"
        data.writelines(content)


if __name__ == "__main__":
    oracle_connection()
    select()
