#coding:utf8
import pymysql
import xlsxwriter
import xlrd
import sys
import csv
import codecs

reload(sys)

sys.setdefaultencoding('utf-8')

#该函数为了排除参数sqlpar为None时的情况，即item表中外键字段为null的情况
#因此这个异常处理只有两个结果，要么数据完全正确，要么数据全部为空；故无需特别处理。
#因此调用后要查看处理结果，若调用时参数写错可能数据全部为空
def handle_sql(sqltable,sqlfield,sqlpar):
	try:
		sqlquery = "select " + sqlfield + " from " + sqltable + " where id = " + str(sqlpar) + ";"
		cursor.execute(sqlquery)
		return cursor.fetchone()[0]
	except:
		return ''


#生成excel表格并导出数据，因为表头用代码导入后，格式错乱，故表头需要人工复制
def handle_excel(data,filename):
	wb = xlsxwriter.Workbook("./output/"+filename+'.xlsx')
	sheet = wb.add_worksheet(filename)
	for irow,ra in enumerate(data):
		for icol,values in enumerate(ra):
			sheet.write(irow+4,icol,values)
	wb.close()


#处理excel表格，使得每种excel数据能被正确读取
def getcellvalue_as_str(sheet,at):
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


#读取location的信息，返回一个字典，键为nams中的老的location,值为新的四级级联值得列表
def location_dict():
	csv_reader = csv.reader(codecs.open('hz_locations.csv'))
	rack_dict = {}
	for row in csv_reader:
		old_rack = row[-1]
		new_location = row[1:-1]
		rack_dict[old_rack] = new_location
	return rack_dict


#从No_import.xlsx中读取不导入的数据，返回一个包含数据库id的列表(数组)
#需要将No_import.xlsx文件放在脚本同目录下
def no_import_to_list():
	nolist = []
	nowb = xlrd.open_workbook('No_import.xlsx')
	nosheet = nowb.sheet_by_index(0)
	for i in range(1, nosheet.nrows):
		dbid = getcellvalue_as_str(nosheet, (i, 0))
		nolist.append(dbid)
	return nolist


#从change_user.xlsx中读取需要改变current_user的数据,返回一个字典，键为数据库id，值为新的current_user.
#需要将change_user.xlsx文件放在脚本同目录下 {"id":"end user"}
def new_user_to_dict():
	newdict = {}
	newwb = xlrd.open_workbook('change_user.xlsx')
	newsheet = newwb.sheet_by_index(0)
	for i in range(2,newsheet.nrows):
		dbid = getcellvalue_as_str(newsheet,(i,0))
		newuser = getcellvalue_as_str(newsheet,(i,1))

		newdict[dbid] = newuser
	return newdict


#从department.xlsx读取数据，返回一个字典，键为names中的数据，值为要转化成的新的四级级联值的一个列表(数组)
#需要将department.xlsx文件放在脚本同目录下  {"Hangxhou..":[n2,n3,n4..]}
def excel_to_dict():
	dpdict = {}
	dpwb = xlrd.open_workbook('department.xlsx')
	dpsheet = dpwb.sheet_by_index(0)
	for i in range(2,dpsheet.nrows):
		dplist = []
		nams = getcellvalue_as_str(dpsheet,(i,0))
		dplist = [getcellvalue_as_str(dpsheet,(i,2)),getcellvalue_as_str(dpsheet,(i,3)),\
					getcellvalue_as_str(dpsheet,(i,4)),getcellvalue_as_str(dpsheet,(i,5))]
		dpdict[nams] = dplist
	return dpdict


#获取数据库值函数，主要的数据处理函数
def handle_data_from_db(spara,filename):
	state_dict = {1:'Available',2:'Broken',3:'Draft',4:'To Be Located',5:'In Use',6:'Available',7:'Scrapped',\
				  17:'Calibrating',18:'',19:'Calibrating',20:'Available',23:'Scrapped',200:'Repairing',\
				  300:'',400:'Transferred'}
	Acquisition_dict = {0:'Purchase',1:'Purchase',2:'',3:'',4:'Borrow',5:'Transfer',6:'Purchase'}
	Calibration_dict = {1:'calibration required',2:'calibration not required',3:'not applicable'}
	#排除马尼拉和成都的数据
	exclude_owner_org_id = (286,296,306,316,326,336,346,356,376,386,396,406,416,426,436,440,443,446,456,466,476,486,496)
	#读取department数据
	dp_dict = excel_to_dict()

	serverdb = pymysql.connect("hz4labs02.china.nsn-net.net","readonly_storage","readonly_storage","storage",charset='utf8')
	global cursor
	cursor = serverdb.cursor()

	#此段代码为获取serial_number为空、'N/A'以及重复时的数据,以便导出时做排除
	# snsql = 'select serial_number from storage_management_manageditem group by serial_number having count(*)>1;'
	snsql = 'select * from storage_management_manageditem group by serial_number having count(*)>1;'
	cursor.execute(snsql)
	sndatas = cursor.fetchall()  #((sn1,),(sn2,),(sn3,)..)
	sn_list = []
	for sndata in sndatas:
		if sndata[0] in ('',None):
			pass
		ds = str(sndata[0]).decode('gbk').encode('utf-8')
		sn_list.append(ds)
	sn_tuple = tuple(sn_list) #(sn1,sn2,sn3..)


	sqlone = "select * from storage_management_manageditem where item_category_id in " + str(spara) + "and owner_org_id not in "\
				+ str(exclude_owner_org_id) + ' and serial_number not in ' + str(sn_tuple) + " and state<>7 and state<>23 order by id;"
	cursor.execute(sqlone)
	dataone = cursor.fetchall()
	alldata = []
	# 获取要更改current_user的数据
	newuser_dict = new_user_to_dict()
	#获取不导入的数据
	noimport_list = no_import_to_list()
	#获取location的mapping信息
	rack_dict = location_dict()
	for data in dataone:
		dbid = data[0]
		strid = str(dbid)

		name = data[1]

		old_uniqid = data[32]
		sn = data[17]


		__owner_org_id = data[40]
		site = handle_sql('storage_management_team','site',__owner_org_id)

		# category = category_dict[1]
		category = category_dict[int(spara[0])]

		__sub_item_category_id = data[49]
		if __sub_item_category_id in ('',None):
			sub_category = 'N/A'
		else:
			sub_category = handle_sql('storage_management_subitemcategory','name',__sub_item_category_id)

		__manufacturer_id = data[30]
		if __manufacturer_id in ('',None):
			brand = 'N/A'
		else:
			brand = handle_sql('storage_management_manufacturer','name',__manufacturer_id)

		__sql_part_number = data[31]
		if __sql_part_number in ('','N/A',None):
			model = 'N/A'
			pn = 'N/A'
		elif ('nokia' in __sql_part_number.lower()) or ('nokia' or 'nsn' in brand.lower()):
			model = 'N/A'
			pn = __sql_part_number
		else:
			model = __sql_part_number
			pn = 'N/A'

		__state = data[4]
		status = state_dict[__state] if __state is not None else ""

		__vendor_id = data[13]
		vendor = handle_sql('storage_management_vendor','name',__vendor_id)

		asset_number = data[15]

		storage_date = data[19].strftime('%d.%m.%Y')

		__acquisiton_method = data[22]
		acquisition_method = Acquisition_dict[__acquisiton_method] if __acquisiton_method is not None else ""


		#此处为department部分
		__owner_org_id = data[40]
		dpname = handle_sql('storage_management_team','name',__owner_org_id)
		try:
			dpN = dp_dict[dpname]
			# print dpN
			N2 = dpN[0]
			N3 = dpN[1]
			N4 = dpN[2]
			N5 = dpN[3]
		except:
			N2 = ''
			N3 = ''
			N4 = ''
			N5 = ''

		
		__cost_center_id = data[6]
		cc = handle_sql('storage_management_sapcostcenter','cost_center',__cost_center_id)

		__purchaser_id = data[54]
		purchaser = handle_sql('storage_management_companyuser','email',__purchaser_id)

		__approver_id = data[44]
		item_owner = handle_sql('storage_management_companyuser','email',__approver_id)

		asset_owner = ''


		#current_user
		#如果current_user需要改变，则使用excel中的数据
		if strid in newuser_dict:
			user = newuser_dict[strid]
		else:
			__current_user_id = data[56]
			user = handle_sql('storage_management_companyuser','email',__current_user_id)

		po = data[36]

		four_budget_id = ''

		cost = data[26]

		__calibration_type = data[33]
		calibration_type = Calibration_dict[__calibration_type] if __calibration_type is not None else ""

		__need_supervision = data[53]
		if __need_supervision == 1:
			bonded = 'yes'
		else:
			bonded = 'no'

		prototyping = ''

		quantity = data[41]

		description = ''
		#处理不导入的数据
		if __state == 7 or (strid in noimport_list):
			is_deleted = 'yes'
		else:
			is_deleted = 'no'

		'''	
		#处理location信息，在模板后面加一列old_location,如果所给的map文件里有nams的location数据的话，则按照map填入building，room等,
		old_location置为空；如果map文件中不存在的话，则把building,room置为空，把原数据写到old_location字段，以便后续排查原因
		1.如果有新的location,先按照五级结构填完，旧的仍然保存在old_location
		2. 如果没有新的location，在末尾加一列，保存在新的一列中
		
		'''

		old_loc_uniqid = data[16]  #L24FFA03
		location_old = str(data[16])[-4:] #FA03
		if location_old == '' or location_old is None:
			old_loc_uniqid = ''
			location_old = ''
			building = ''
			Room = ''
			area = ''
			code = ''
		else:
			if location_old in rack_dict:
				building = rack_dict[location_old][0]
				Room = rack_dict[location_old][1]
				area = rack_dict[location_old][2]
				code = location_old
				location_old = ''
			else:
				location_old = old_loc_uniqid
				old_loc_uniqid = ''
				building = ''
				Room = ''
				area = ''
				code = ''

		subdata = (name,old_uniqid,sn,old_loc_uniqid,site,building,Room,area,code,category,sub_category,\
				   brand,model,pn,status,vendor,asset_number,storage_date,acquisition_method,\
				   N2,N3,N4,N5,cc,purchaser,item_owner,asset_owner,user,po,four_budget_id,cost,\
				   calibration_type,bonded,prototyping,quantity,description,is_deleted,location_old)
		alldata.append(subdata)
	handle_excel(alldata,filename)
	serverdb.close()


def main():
	global category_dict
	category_dict = {1:'Radio Network',2:'Radio Network',3:'Core Network',4:'Measurement Equipment',5:'UE & Terminal',\
					6:'Server',7:'PC',8:'IP Networking',9:'Software',10:'Infrastructure',11:'Other Device',12:'',\
					13:'Other Device',14:'Consumables',15:'Board',16:''}
	raw_category = raw_input('Please input the category_id:')
	category = raw_category.split(',')
	category = tuple(category)
	filename = category_dict[int(category[0])]
	handle_data_from_db(category,filename)
	

if __name__ == '__main__':
	main()
	#rack_dict = location_dict()