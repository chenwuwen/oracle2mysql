import pymysql
import datetime

mysql_username = 'root'
mysql_password = 'ruifight2018'
mysql_host = '115.47.147.131'

mysql_port = 3306
mysql_dbname = 'flow_population_2'

mysql_cursor = None
mysql_conn = None


# mysql建立连接
def mysql_connection():
    global mysql_conn
    mysql_conn = pymysql.connect(host=mysql_host, port=mysql_port, user=mysql_username, passwd=mysql_password,
                                 db=mysql_dbname)
    global mysql_cursor
    mysql_cursor = mysql_conn.cursor()


# 街路巷表中插入数据
def insertJLX(dic):
    # 先查询是否存在该记录,存在则返回,不存在则插入
    select_sql = ''' select * from t_jzw_jlx where jlxdm = %(jlxdm)s '''
    mysql_cursor.execute(select_sql, dic)
    ret = mysql_cursor.fetchone()
    if ret:
        print("街路巷表中已存在当前记录")
        return
    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    dic['now'] = now
    insert_sql = '''  insert into t_jzw_jlx (jlxmc,jlxdm,dzysqx,pcsdm,pcsmc,sssq_dm,sssq_mc,is_del,
               imp_date,jwhdm,jwhmc,jwzrqdm,jwzrqmc)
                VALUES (%(jlxmc)s,%(jlxdm)s,%(dzysqx)s,%(pcsdm)s,%(pcsmc)s,
                %(sssq_dm)s,%(sssq_mc)s,'F',%(now)s,%(jwhdm)s,%(jwhmc)s,%(jwzrqdm)s,%(jwzrqmc)s) '''

    mysql_cursor.execute(insert_sql, dic)
    mysql_conn.commit()


def insertJZW(dic):
    select_sql = ''' select * from t_jzw where jzw_bm = %(jzw_bm)s '''
    mysql_cursor.execute(select_sql, dic)
    ret = mysql_cursor.fetchone()
    # print(ret)
    if ret:
        print("建筑表中已存在当前记录")
        return
    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    dic['now'] = now

    insert_sql = ''' insert into t_jzw (ds_cs,ds_dys,ds_hs,dx_cs,dx_dys,dx_hs,imp_date,is_del,jwh_dm,
                                jwh_mc,jzw_bm,jzw_dizhi,jzw_jlxdm,jzw_jlxmc,pcsdm,pcsmc,sssq_dm,sssq_mc)
                                VALUES (%(ds_cs)s,%(ds_dys)s,%(ds_hs)s,%(dx_cs)s,%(dx_dys)s,%(dx_hs)s,%(now)s,'F',%(jwh_dm)s,
                                %(jwh_mc)s,%(jzw_bm)s,%(jzw_dizhi)s,%(jzw_jlxdm)s,%(jzw_jlxmc)s,%(pcsdm)s,%(pcsmc)s,%(sssq_dm)s,%(sssq_mc)s) '''
    mysql_cursor.execute(insert_sql, dic)
    mysql_conn.commit()


# 房间表插入
def insertRoom(dic):
    # select_sql = ''' select * from t_jzw_fangjian_copy where hu_id = %(hu_id)s '''
    # mysql_cursor.execute(select_sql, dic)
    # ret = mysql_cursor.fetchone()
    # print(ret)
    # if ret:
    #     print("房间表中已存在当前记录")
    #     return
    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    dic['now'] = now
    insert_sql = ''' insert into t_jzw_fangjian (jzw_dm,hu_id,jzw_dizhi,dys,cs,hs,humc,fjlx,whsj,is_del,renshu_ldrk,renshu_rhfl,renshu_hjrk)
                    VALUES (%(jzw_dm)s,%(hu_id)s,%(jzw_dizhi)s,%(dys)s,%(cs)s,%(hs)s,%(humc)s,'P',%(now)s,'F',0,0,0) '''
    mysql_cursor.execute(insert_sql, dic)
    mysql_conn.commit()


if __name__ == '__main__':
    mysql_connection()
    # jlx_dic_template = {'jlxmc': '', 'jlxdm': '', 'dzysqx': '', 'pcsdm': '', 'pcsmc': '', 'sssq_dm': '', 'sssq_mc': '', 'jwhdm': '',
    #        'jwhmc': '', 'jwzrqdm': '', 'jwzrqmc': ''}

    # jlx_dic = {'jlxmc': '苏家坨镇草场村', 'jlxdm': '4D83F3EEDED1A35FE0536902080A8882', 'dzysqx': '海淀区', 'pcsdm': '1101080822000',
    #        'pcsmc': '苏家坨派出所', 'sssq_dm': '11001080822016', 'sssq_mc': '草厂村', 'jwhdm': '110108029213',
    #        'jwhmc': '草厂村委会', 'jwzrqdm': '', 'jwzrqmc': ''}
    # insertJLX(jlx_dic)

    jzw_dic_template = {'ds_cs': '', 'ds_dys': '', 'ds_hs': '', 'dx_cs': '', 'dx_dys': '', 'dx_hs': '', 'jwh_dm': '',
                        'jwh_mc': '', 'jzw_bm': '', 'jzw_dizhi': '', 'jzw_jlxdm': '', 'jzw_jlxmc': '', 'pcsdm': '',
                        'pcsmc': '', 'sssq_dm': '', 'sssq_mc': ''}
    jzw_dic = {'ds_cs': '1', 'ds_dys': '1', 'ds_hs': '1', 'dx_cs': '0', 'dx_dys': '0', 'dx_hs': '0',
               'jwh_dm': '110108029215',
               'jwh_mc': '七王坟村委会', 'jzw_bm': '4CBAA1DE229DE4B3E0536902080AA554', 'jzw_dizhi': '102号',
               'jzw_jlxdm': '4D83F3EEDEF1A35FE0536902080A8882', 'jzw_jlxmc': '苏家坨镇七王坟村', 'pcsdm': '1101080822000',
               'pcsmc': '苏家坨派出所', 'sssq_dm': '1101080822018', 'sssq_mc': '七王坟村'}

    insertJZW(jzw_dic)

    jzw_room_template = {'jzw_dm': '', 'hu_id': '', 'jzw_dizhi': '', 'dys': '', 'cs': '', 'hs': '', 'humc': ''}
    jzw_room = {'jzw_dm': '4CBAA1E0F9D4E4B3E0536902080AA554', 'hu_id': '4CBAA1E0F9D4E4B3E0536902080AA554-1-1-15',
                'jzw_dizhi': '苏家坨镇七王坟村36号', 'dys': '1', 'cs': '1', 'hs': '15', 'humc': '15号'}

    insertRoom(jzw_room)
