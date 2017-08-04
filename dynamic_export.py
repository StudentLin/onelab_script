#coding:utf8
import pymysql
import xlsxwriter
import xlrd
import datetime
import sys


serverdb = pymysql.connect("hz4labs02.china.nsn-net.net","readonly_storage","readonly_storage","storage",charset='utf8')
cursor = serverdb.cursor()
install_type_dict = {1:'Hardware installation',2:'Software installation',3:'I will pick it by myself'}
lab_location_dict = {'CN/Beijing':'CN/Beijing Nokia','CN/Chengdu':'CN/Chengdu','CN/Hangzhou':'CN/Hangzhou','PH/Manila':'PH/Manila'}
componenet_dict = {'CN/Beijing-Lab Services':'CN/Beijing-Nokia Lab Services','CN/Hangzhou RDNET Connectivity':'CN/Hangzhou RDNET Connectivity',\
                    'CN/Hangzhou-EE-TelcoCloudSupport':'CN/Hangzhou-EE-TelcoCloudSupport','CN/Hangzhou-Lab Services':'CN/Hangzhou-Lab Services',\
                    'DE/Munich RDNET Connectivity':'DE/Munich RDNET Connectivity','DE/Ulm-Lab Services':'DE/Ulm-Lab Services',\
                    'FI/Espoo HetRAN':'FI/Espoo-HetRAN','FI/ESPOO-3GC-HW':'FI/Espoo-3GC-HW','PH/Manila-Lab Services':'PH/Manila-Lab Services'}
#与所给模板不完全一样，需要做个mapping
bl_except_dict = {'A&A/Network &amp;Service Operations':'A&A/Network & Service Operations','A&A/Security loT':'A&A/Security & loT',\
                  'MN CCN/3G Core BL':'MN CC/3G Core BL','MN CCN/Cloud SDM BL':'MN CC/SDM BL','RMA':'MN MNP/System R&D and Tools',\
                  'RMA':'MN MNP/System R&D and Tools','MBB/HetRAN':'MN MNP/HETRAN Business Line','MBB/HetRAN-5G':'MN MNP/5G Business Line'}
#测试的bl，不导出，处理数据时将bl字段设置为‘test’，然后操作excel删除即可
bl_exlude_list = ['MBB/3G Core','MBB/CEM & OSS','MBB/Customer Support','MBB/FDD-LTE','MBB/Liquid Core','MBB/RF',\
                  'MBB/Small Cell','MBB/SRAN','MBB/System Module','MBB/TD-LTE']


#读取excel标准化，是每一个cell都能被正确读取
def getcellvalue_as_str(sheet,at):
    '''
    :param sheet:excel文件的工作簿 
    :param at: excel的单元格,一个元组，例如：第一行一列：at=(0,0)
    :return: 单元格中的值
    '''
    if at[0] >= sheet.nrows or at[1] >= sheet.ncols:
        return ''
    cell = sheet.cell(at[0], at[1])
    if cell.ctype in (xlrd.XL_CELL_EMPTY, xlrd.XL_CELL_BLANK):
        return ''
    elif cell.ctype == xlrd.XL_CELL_TEXT:
        return str(cell.value)
    elif cell.ctype == xlrd.XL_CELL_NUMBER:
        return str(int(cell.value))
    else:
        raise OperationStopException('NOT adentify cell At:' + at)


#获取需要更改current_user的函数
#需要将change_user.xlsx文件放到脚本同级目录下
def new_user_to_dict():
    newdict = {}
    newwb = xlrd.open_workbook('change_user.xlsx')
    newsheet = newwb.sheet_by_index(0)
    for i in range(2, newsheet.nrows):
        dbid = getcellvalue_as_str(newsheet, (i, 0))
        newuser = getcellvalue_as_str(newsheet, (i, 1))

        newdict[dbid] = newuser
    return newdict



#处理business line中的斜杠前后的空格问题
def handle_slash(strs):
    if '/' not in strs:
        return strs
    else:
        strs_list = strs.split('/')
        strs_list[0] = strs_list[0].strip(' ')
        strs_list[1] = strs_list[1].strip(' ')
        result = strs_list[0] + ' / ' + strs_list[1]
        return result



#同静态数据
def handle_sql(sqltable,sqlfield,sqlpar):
    '''
    :param sqltable:需要关联的外键表 
    :param sqlfield: 需要导出的外键表相关字段
    :param sqlpar: 此处特指表中的外键字段
    :return: 返回外键关联表的字段值
    '''
    try:
        sqlquery = "select " + sqlfield + " from " + sqltable + " where id = " + str(sqlpar) + ";"
        cursor.execute(sqlquery)
        return cursor.fetchone()[0]
    except:
        return ''


#生成excel，表头需要人工复制
def handle_excel(data,filename):
    '''
    :param data:存储的需要导出的数据；需要是可迭代对象，该处是列表 
    :param filename: 调用时自定义导出的文件名
    :return: 
    '''
    wb = xlsxwriter.Workbook(filename+'.xlsx')
    sheet = wb.add_worksheet(filename)
    for irow,ra in enumerate(data):
        for icol,values in enumerate(ra):
            sheet.write(irow+3,icol,values)
    wb.close()


#主要的处理函数
def get_data_from_db():
    allsql = "select * from storage_management_reservationrequest where request_state in (1,2,4,8,9,10)"
    cursor.execute(allsql)
    alldata = cursor.fetchall()
    datalist = []
    newuser_dict = new_user_to_dict()
    for data in alldata:
        dbid = data[0]
        old_request_id = dbid
        state = data[8]

        create_time = data[11].strftime("%Y-%m-%d %H:%M:%S")


        #获取equipment_id，一个reserverequest可能有多个item，所以要以逗号隔开
        old_inv_sql = "select item_id from storage_management_reserveditem where reservation_request_id=" + str(dbid) + ';'
        cursor.execute(old_inv_sql)
        old_inv_ids = cursor.fetchall()
        if old_inv_ids is None:
            old_inv_uniqids = ''
        else:
            ids_list = []
            for old_inv_id in old_inv_ids:
                sql1 = "select equipment_id from storage_management_manageditem where id=" + str(old_inv_id[0]) + ";"
                cursor.execute(sql1)
                old_inv_uniqid = cursor.fetchone()[0]
                if old_inv_uniqid is None or str(old_inv_uniqid)=='N/A':
                    pass
                else:
                    ids_list.append(str(old_inv_uniqid))
            if len(ids_list)==0:
                continue
            else:
                old_inv_uniqids = ','.join(ids_list)


        _item_owner_id = data[6]
        item_owner = handle_sql('storage_management_companyuser','email',_item_owner_id)

        #reserver的发起者，所以item的current_user更改后，这个也必须更改
        _requestor = data[10]
        waitrequestor = handle_sql('storage_management_companyuser', 'email', _requestor)
        if old_inv_uniqids == '':
            requestor = waitrequestor
        else:
            for itemid in old_inv_uniqids:
                if itemid[0] in newuser_dict:
                    requestor = newuser_dict[itemid[0]]
                    break
                else:
                    requestor = waitrequestor


        #开始时间和结束时间,若结束时间为null，则手动的设置为一年
        start_time = data[3].strftime("%Y-%m-%d %H:%M:%S") if data[3] else ''
        delta = datetime.timedelta(days=365)
        end_time = data[4].strftime("%Y-%m-%d %H:%M:%S") if data[4] else (data[3]+delta).strftime('%Y-%m-%d %H:%M:%S')

        _program = data[15]
        if _program in ('','N/A',None):
            program = 'TBD'
        else:
            program = _program

        _feature = data[16]
        if _feature in ('','N/A',None):
            feature = 'TBD'
        else:
            feature = _feature

        install_sql = "select *from storage_management_installationinfo where reservation_request_id " + "= " + str(dbid) + \
                      " having installation_type in (1,2,3);"

        cursor.execute(install_sql)
        install_data = cursor.fetchone()
        #reserverequest没有install信息
        if install_data is None:
            install_type = ''
            is_picked = 'No'
            business_line = ''
            lab_location = ''
            componets = ''
            jira_id = ''
            jira_key = ''
            jira_url = ''
        else:
            _install_type = install_data[2]
            install_type = install_type_dict[_install_type]

            if state in (4,8,9,10):
                is_picked = 'Yes'
            else:
                is_picked = 'No'

            _business_line = install_data[7]
            if _business_line in bl_except_dict:
                business_line = bl_except_dict[_business_line]
                business_line = handle_slash(business_line)
            #测试的bl，置为test，导出完成后操作excel删除即可
            elif _business_line in bl_exlude_list:
                continue
                business_line = 'test'
            else:
                business_line = handle_slash(_business_line)

            _lab_location = install_data[15]
            lab_location = lab_location_dict[_lab_location]

            _componets = install_data[16]
            componets = componenet_dict[_componets]

            jira_id = install_data[4]
            jira_key = install_data[5]
            jira_url = install_data[6]

        #增加的is_approved字段
        state = data[8]
        if int(state) == 1:
            is_approved = 'No'
        else:
            is_approved = 'Yes'
        #每一条数据保存在一个元组中
        subdata = (old_request_id,create_time,old_inv_uniqids,item_owner,requestor,start_time,end_time,program,\
                   feature,business_line,lab_location,componets,install_type,is_approved,is_picked,jira_id,\
                   jira_key,jira_url)
        #以元组的形式添加到列表中
        datalist.append(subdata)
    handle_excel(datalist,'dynamic')



def main():
    get_data_from_db()


if __name__ == "__main__":
    main()