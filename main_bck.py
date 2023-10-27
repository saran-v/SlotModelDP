import pandas as pd
import csv
from zipfile import ZipFile
import os
import shutil
import subprocess
import time

import pyodbc

conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=SQLSPC19-1;'
                      'Database=SupplyChain;'
                      'Trusted_Connection=yes;')

def readConfig(dirList):

    configDict = dict()

    with open('config_w.csv') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        for row in csv_reader:
            if row[0] != 'vendor_id':
                configDict[row[0]] = row[1]
            else:
                row.remove('vendor_id')
                configDict['vendor_id'] = row

    # Create directory
        # Create target
        if not os.path.exists(configDict['test_dir']):
            try:
                os.mkdir(configDict['test_dir'])
                print("Directory ", configDict['test_dir'], " Created ")
            except FileExistsError:
                print("Directory ", configDict['test_dir'], " already exists")
        else:
            print("Directory already exists")

    for i in dirList:
        # Create target Directory
        dirName = configDict['test_dir']+'/'+i +'/cplex'
        if not os.path.exists(dirName):
            try:
                os.makedirs(dirName)
                print("Directory ", dirName, " Created ")
            except FileExistsError:
                print("Directory ", dirName, " already exists")

        dirName = configDict['test_dir']+'/'+i +'/cplex/fixed'
        if not os.path.exists(dirName):
            try:
                os.makedirs(dirName)
                print("Directory ", dirName, " Created ")
            except FileExistsError:
                print("Directory ", dirName, " already exists")

        dirName = configDict['test_dir']+'/'+i +'/scip'
        if not os.path.exists(dirName):
            try:
                os.makedirs(dirName)
                print("Directory ", dirName, " Created ")
            except FileExistsError:
                print("Directory ", dirName, " already exists")

    return configDict

def prepareItemDC(configDict, idList, dirName):

    query = "SELECT * FROM [SupplyChain].[dmsc].[SCA_ARTICLE_DC] where [Vendor ID] = ?;"  # " where Planner = ?;"
    df = pd.read_sql(query, conn, params=[idList[0]])
    # df = pd.DataFrame(sql_query)
    # df.to_csv(r'item_dc.csv', index=False)  # place 'r' before the path name

    # df = pd.read_csv(configDict['item_master'])
    # vid = list(map(int, configDict['vendor_id']))

    df['Vendor ID'] = df['Vendor ID'].fillna(0)
    df['Vendor ID'] = df['Vendor ID'].astype(int)
    df['Vendor ID'] = df['Vendor ID'].astype(str)
    df = df[(df['Vendor ID'].isin(idList))] # filter by vendor-id

    # df.to_csv(dirName + "/item_master_1.csv", index=False)
    # df = df[df['Article'].str.contains("20059987017")]
    # df = df[(df['Article'] == 20059987017)] # filter by vendor-id
    # df = df[(df['Article'] == "20059987017")] # filter by vendor-id

    df = df[df['Family_Code_Desc'].notnull()] # skip if null family code
    df = df[(df['Forecast Flag'] == 'Y')]  # filter by forecast flag
    df = df[(df['Replenish Flag'] == 'Y')]  # filter by replenish flag
    df = df[(df['Global Drop Status'] == 'N')]  # filter by global drop status

    # df.to_csv(dirName + "/item_master_2.csv", index=False)

    df = df[['Article', 'Article Description', 'Vendor ID', 'Vendor Name', 'Family Code - Key', 'site','Family_Code_Desc','Weeks Of Supply For SS','Unrestricted Stock','STO Inbound Qty','confirmedQty',
             'STO Outbound Qty','Volume','DSX LeadTime','Avg Fcst Units','Avg Fcst Cost','MaxCubesPerContainer','Incoterm Group','Minor_Code','Minor_Code_Description','Origin Country']]

    df['Unrestricted Stock'] = df['Unrestricted Stock'].fillna(0)
    df['STO Inbound Qty'] = df['STO Inbound Qty'].fillna(0)
    df['STO Outbound Qty'] = df['STO Outbound Qty'].fillna(0)
    df['confirmedQty'] = df['confirmedQty'].fillna(0)

    df['Avg Fcst Units'] = df['Avg Fcst Units'].fillna(0)

    df['on_hand'] = (df['Unrestricted Stock'] + df['STO Inbound Qty'] ) - (df['confirmedQty'] + df['STO Outbound Qty'])
    df['sales'] = df['confirmedQty'] + df['Avg Fcst Units']
    df['cost'] = df['Avg Fcst Cost']/df["Avg Fcst Units"]

    final_df = df[['Article', 'site', 'Article Description', 'Vendor ID', 'Vendor Name', 'Family Code - Key', 'Family_Code_Desc','on_hand','Weeks Of Supply For SS','sales','Volume','DSX LeadTime','cost','MaxCubesPerContainer','Incoterm Group','Minor_Code','Minor_Code_Description','Origin Country']]
    final_df.rename(columns={'Article': 'item', 'site': 'location', 'Article Description' : 'item_desc', 'Vendor ID': 'vendor_id',
                             'Vendor Name':'vendor_name', 'Family Code - Key': 'family_code','Family_Code_Desc': 'family_group', 'on_hand': 'on_hand', 'Weeks Of Supply For SS': 'safety_stock', 'Volume':'volume', 'DSX LeadTime':'leadtime',
                             'MaxCubesPerContainer':'MaxCubes','Incoterm Group':'incoterm_group','Minor_Code':'minor_code','Minor_Code_Description':'minor_code_desc','Origin Country':'origin_country'}, inplace=True)

    final_df.to_csv(dirName + "/item_master.csv", index=False)


def prepareItemMaster(configDict, idList, dirName):

    query = "SELECT * FROM [SupplyChain].[dmsc].[SCA_ARTICLE_MASTER] where [VendorNumber] = ?;"  # " where Planner = ?;"
    df = pd.read_sql(query, conn, params=[idList[0]])

    df = df[['Article', 'Product Cost']]

    final_df = df[['Article', 'Product Cost']]
    final_df.rename(columns={'Article': 'item', 'Product Cost': 'cost'}, inplace=True)

    final_df.to_csv(dirName + "/item_details.csv", index=False)


def prepareForecast(configDict, idList, dirName):
    # df = pd.read_csv(configDict['forecast'])

    query = "SELECT * FROM [SupplyChain].[dmsc].[SCA_FORECAST_1YEAR] where [VendorNumber] = ?;"  # " where Planner = ?;"
    df = pd.read_sql(query, conn, params=[idList[0]])

    # vid = list(map(int, configDict['vendor_id']))
    df['VendorNumber'] = df['VendorNumber'].fillna(0)
    df['VendorNumber'] = df['VendorNumber'].astype(int)
    df['VendorNumber'] = df['VendorNumber'].astype(str)
    df = df[(df['VendorNumber'].isin(idList))] # filter by vendor-id

    df = df[df['Family_Code_Desc'].notnull()] # skip if null family code

    final_df = df[['Article', 'DC','FiscalWeek','FiscalYear','Wkly Forecast in Unit']]
    final_df.rename(columns={'Article': 'item', 'DC': 'location', 'FiscalWeek': 'week', 'FiscalYear': 'year', 'Wkly Forecast in Unit': 'units'}, inplace=True)

    final_df.to_csv(dirName + "/forecast.csv", index=False)


def preparePO(configDict, idList, dirName):
    # df = pd.read_csv(configDict['open_po'])
    # vid = list(map(int, configDict['vendor_id']))

    query = "SELECT * FROM [SupplyChain].[dmsc].[SCA_OPEN_PO] where [Booking Code] = '' and [VendorNumber] = ?;"  # " where Planner = ?;"
    df = pd.read_sql(query, conn, params=[idList[0]])

    df['VendorNumber'] = df['VendorNumber'].fillna(0)
    df['VendorNumber'] = df['VendorNumber'].astype(int)
    df['VendorNumber'] = df['VendorNumber'].astype(str)
    df = df[(df['VendorNumber'].isin(idList))] # filter by vendor-id

    df = df[df['Family Code'].notnull()] # skip if null family code

    final_df = df[['PO Doc Nbr', 'PO Item Nbr', 'Article', 'Site','FiscalWeek','FiscalYear','Open PO Qty', 'PO Create Date']]
    final_df.rename(columns={'PO Doc Nbr': 'po_number', 'PO Item Nbr':'PO_Item_Nbr','Article':'Article', 'Site': 'Site', 'FiscalWeek': 'week', 'FiscalYear': 'year', 'Open PO Qty': 'units', 'PO Create Date': 'createDate'}, inplace=True)
    final_df.to_csv(dirName +"/open_po.csv", index=False)

    a = final_df['po_number'].unique()

    poDate = {}
    for po_number in a:
        query = "select [PO Doc Nbr], [Early Ship Date] from [SupplyChain].[dmsc].[PURCHASE ORDER ITEM] where [PO Doc Nbr] = ? order by [Early Ship Date] DESC;"  # " where Planner = ?;"
        df = pd.read_sql(query, conn, params=[po_number])
        poDate[po_number] = df.iloc[0][1]

    df_date = pd.DataFrame(poDate.items())
    df_date.to_csv(dirName +"/ship_date.csv", index=False)


def prepareKit(configDict, dirName):
    df = pd.read_csv(configDict['kit_data'])

    final_df = df[['Article', 'Receipt Week','Quantity']]
    final_df.rename(columns={'Article': 'item', 'Receipt Week': 'week', 'Quantity': 'units'}, inplace=True)

    final_df.to_csv(dirName +"/kit_data.csv", index=False)

def prepareConfig(configDict, dirName):
    with open(dirName + '/Config.csv', 'w', newline='') as f:
        outfile = csv.writer(f)
        outfile.writerow(['num_periods', configDict['num_periods']])
        outfile.writerow(['rounding', configDict['rounding']])
        outfile.writerow(['start_week', configDict['start_week']])
        outfile.writerow(['start_year', configDict['start_year']])
        outfile.writerow(['tol_percent', configDict['tol_percent']])

        outfile.writerow(['look_ahead_demand', configDict['look_ahead_demand']])
        outfile.writerow(['report_weeks', configDict['report_weeks']])
        outfile.writerow(['PO_Out_IN_File', configDict['PO_Out_IN_File']])
        outfile.writerow(['PO_Out_OUT_File', configDict['PO_Out_OUT_File']])

        outfile.writerow(['item_master', 'item_master.csv'])
        outfile.writerow(['forecast', 'forecast.csv'])
        outfile.writerow(['open_po', 'open_po.csv'])
    f.close()

def prepareZipFile(configDict, dirName):
    zipObj = ZipFile(dirName + '/input.zip', 'w')
    # Add multiple files to the zip
    zipObj.write('config.csv')
    zipObj.write('item_master.csv')
    zipObj.write('forecast.csv')
    zipObj.write('open_po.csv')

    if configDict['include_kit'] == '1':
        zipObj.write('kit_data.csv')
    # close the Zip File
    zipObj.close()


def  JavaScipRun(dirName):
    shutil.copyfile(dirName + "/item_master.csv", "item_master.csv")
    shutil.copyfile(dirName + "/forecast.csv", "forecast.csv")
    shutil.copyfile(dirName + "/open_po.csv", "open_po.csv")
    # shutil.copyfile(dirName + "/Config.csv", "Config.csv")

    if os.path.isfile("Plot_Data_out.csv"):
        os.remove("Plot_Data_out.csv")

    _base_cmd = ['java', '-classpath', 'ScipRepPlan-0.jar;jna-5.11.0.jar;ortools-win32-x86-64-9.5.2237.jar;protobuf-java-3.21.5.jar;ortools-java-9.5.2237.jar', 'com.optimizer.Optimizer']  # works

    _base_cmd = ['java', '-classpath',
                 'RepPlan.jar;ScipRepPlan-0.jar;jna-5.11.0.jar;ortools-win32-x86-64-9.5.2237.jar;protobuf-java-3.21.5.jar;ortools-java-9.5.2237.jar',
                 'com.optimizer.Optimizer']  # works

    subprocess.check_call(_base_cmd)

    # java is still running
    while not os.path.exists('Plot_Data_out.csv'):
        time.sleep(1)

    shutil.copyfile("Inv_BackOrder_out.csv", dirName + "/scip/Inv_BackOrder_out_s.csv")
    shutil.copyfile("Inv_OH_out.csv", dirName + "/scip/Inv_OH_out_s.csv")
    shutil.copyfile("PO_Data_out.csv", dirName + "/scip/PO_Data_out_s.csv")
    shutil.copyfile("Plot_Data_out.csv", dirName + "/scip/Plot_Data_out_s.csv")

    shutil.copyfile("Plot_Data_Validations_family_out.csv", dirName + "/scip/Plot_Data_Validations_family_out_s.csv")
    shutil.copyfile("Plot_Data_Validations_family_summary_out.csv", dirName + "/scip/Plot_Data_Validations_family_summary_out_s.csv")
    shutil.copyfile("Plot_Data_Validations_out.csv", dirName + "/scip/Plot_Data_Validations_out_s.csv")
    shutil.copyfile("Plot_Data_Validations_wos_out.csv", dirName + "/scip/Plot_Data_Validations_wos_out_s.csv")
    shutil.copyfile("Run_summary_out.csv", dirName + "/scip/Run_summary_out_s.csv")
    shutil.copyfile("Incomplete_out.csv", dirName + "/scip/Incomplete_out_s.csv")
    shutil.copyfile("PO_Data_Summary_Out.csv", dirName + "/scip/PO_Data_Summary_Out_s.csv")
    shutil.copyfile("StockOut_Summary_out.csv", dirName + "/scip/StockOut_Summary_out_s.csv")

def  JavaCplexRun(dirName, fixed, configDict, supName):
    shutil.copyfile(dirName + "/item_master.csv", "item_master.csv")
    shutil.copyfile(dirName + "/forecast.csv", "forecast.csv")
    shutil.copyfile(dirName + "/open_po.csv", "open_po.csv")
    shutil.copyfile(dirName + "/Config.csv", "Config.csv")

    if os.path.isfile("Plot_Data_out.csv"):
        os.remove("Plot_Data_out.csv")

    _base_cmd = ['java', '-Djava.library.path=D:\\CPLEX201\\cplex\\bin\\x64_win64', '-classpath',
                 'RepPlan.jar;D:\\CPLEX201\\cplex\\lib\\cplex.jar', 'com.optimizer.Optimizer','cplex']  # works

    # _base_cmd = ['java', '-classpath', 'ScipRepPlan-0.jar;jna-5.11.0.jar;ortools-win32-x86-64-9.5.2237.jar;protobuf-java-3.21.5.jar;ortools-java-9.5.2237.jar', 'com.optimizer.Optimizer']  # works

    subprocess.check_call(_base_cmd)

    # java is still running
    while not os.path.exists('Plot_Data_out.csv'):
        time.sleep(1)

    if not fixed:
        shutil.copyfile("Inv_BackOrder_out.csv", dirName + "/cplex/Inv_BackOrder_out_c.csv")
        shutil.copyfile("Inv_OH_out.csv", dirName + "/cplex/Inv_OH_out_c.csv")
        shutil.copyfile("PO_Data_out.csv", dirName + "/cplex/PO_Data_out_c.csv")
        shutil.copyfile("Plot_Data_out.csv", dirName + "/cplex/Plot_Data_out_c.csv")

        shutil.copyfile("Plot_Data_Validations_family_out.csv", dirName + "/cplex/Plot_Data_Validations_family_out_c.csv")
        shutil.copyfile("Plot_Data_Validations_family_summary_out.csv", dirName + "/cplex/Plot_Data_Validations_family_summary_out_c.csv")
        shutil.copyfile("Plot_Data_Validations_out.csv", dirName + "/cplex/Plot_Data_Validations_out_c.csv")
        shutil.copyfile("Plot_Data_Validations_wos_out.csv", dirName + "/cplex/Plot_Data_Validations_wos_out_c.csv")
        shutil.copyfile("Run_summary_out.csv", dirName + "/cplex/Run_summary_out_c.csv")
        shutil.copyfile("Incomplete_out.csv", dirName + "/cplex/Incomplete_out_c.csv")
        shutil.copyfile("PO_Data_Summary_Out.csv", dirName + "/cplex/PO_Data_Summary_Out_c.csv")
        shutil.copyfile("StockOut_Summary_out.csv", dirName + "/cplex/StockOut_Summary_out_c.csv")
    else:
        shutil.copyfile("Inv_BackOrder_out.csv", dirName + "/cplex/fixed/Inv_BackOrder_out_c.csv")
        shutil.copyfile("Inv_OH_out.csv", dirName + "/cplex/fixed/Inv_OH_out_c.csv")
        shutil.copyfile("PO_Data_out.csv", dirName + "/cplex/fixed/PO_Data_out_c.csv")
        shutil.copyfile("Plot_Data_out.csv", dirName + "/cplex/fixed/Plot_Data_out_c.csv")

        shutil.copyfile("Plot_Data_Validations_family_out.csv",
                        dirName + "/cplex/fixed/Plot_Data_Validations_family_out_c.csv")
        shutil.copyfile("Plot_Data_Validations_family_summary_out.csv",
                        dirName + "/cplex/fixed/Plot_Data_Validations_family_summary_out_c.csv")
        shutil.copyfile("Plot_Data_Validations_out.csv", dirName + "/cplex/fixed/Plot_Data_Validations_out_c.csv")
        shutil.copyfile("Plot_Data_Validations_wos_out.csv", dirName + "/cplex/fixed/Plot_Data_Validations_wos_out_c.csv")
        shutil.copyfile("Run_summary_out.csv", dirName + "/cplex/fixed/Run_summary_out_c.csv")
        shutil.copyfile("Incomplete_out.csv", dirName + "/cplex/fixed/Incomplete_out_c.csv")
        shutil.copyfile("PO_Data_Summary_Out.csv", dirName + "/cplex/fixed/PO_Data_Summary_Out_c.csv")
        shutil.copyfile("StockOut_Summary_out.csv", dirName + "/cplex/fixed/StockOut_Summary_out_c.csv")

    # Upload to the DB
    # shutil.copyfile("PO_Data_Summary_Out.csv", "/PO_Data_Summary_Out_Master.csv")
    #
    # if os.path.isfile("PO_Data_Summary_Out_Master_DB.csv"):
    #     os.remove("PO_Data_Summary_Out_Master_DB.csv")
    #
    # _base_cmd = ['java', '-classpath','FileMerge.jar', 'Merger']  # works
    # subprocess.check_call(_base_cmd)
    #
    # # java is still running
    # while not os.path.exists('PO_Data_Summary_Out_Master_DB.csv'):
    #     time.sleep(1)
    #
    # shutil.copyfile("PO_Data_Summary_Out_Master_DB.csv", configDict['test_dir'] + "/PO_Data_Summary_Out_Master_DB_" + supName + ".csv")

if __name__ == '__main__':

    dirList = ['Aus_Hill','Elem','Holland','King','Man_May','River_Jack']
    idList = {'Aus_Hill':[20008777,20006015],'Elem':[20005933],'Holland':[20005995],'King':[20001050],'Man_May':[20001163,20007855],'River_Jack':[20006201,20001769]}

    dirList = ['Elem','Holland','King','Man_May','River_Jack']
    idList = {'Elem':[20005933],'Holland':[20005995],'King':[20001050],'Man_May':[20001163,20007855],'River_Jack':[20006201,20001769]}


    dirList = ['Elem','Holland','King','River_Jack']
    idList = {'Elem':[20005933],'Holland':[20005995],'King':[20001050],'River_Jack':[20006201,20001769]}

    dirList = ['Aus_Hill']
    idList = {'Aus_Hill':[20008777,20006015]}

    dirList = ['Test']
    idList = {'Test':[20008777,20006015]}

    dirList = ['Aus_Hill','Elem','Holland','King','Man_May','River_Jack']
    idList = {'Aus_Hill':[20008777,20006015],'Elem':[20005933],'Holland':[20005995],'King':[20001050],'Man_May':[20001163,20007855],'River_Jack':[20006201,20001769]}

    dirList = ['Holland','King','Man_May','River_Jack']
    idList = {'Holland':[20005995],'King':[20001050],'Man_May':[20001163,20007855],'River_Jack':[20006201,20001769]}

    dirList = ['Aus_Hill','Elem','Holland']
    idList = {'Aus_Hill':[20008777,20006015],'Elem':[20005933],'Holland':[20005995]}

    dirList = ['King','Man_May','River_Jack']
    idList = {'King':[20001050],'Man_May':[20001163,20007855],'River_Jack':[20006201,20001769]}

    dirList = ['Aus_Hill','Elem','Holland','King','Man_May','River_Jack']
    idList = {'Aus_Hill':[20008777,20006015],'Elem':[20005933],'Holland':[20005995],'King':[20001050],'Man_May':[20001163,20007855],'River_Jack':[20006201,20001769]}

    dirList = ['Man_May','River_Jack']
    idList = {'Man_May':[20001163,20007855],'River_Jack':[20006201,20001769]}

    dirList = ['Aus_Hill','Elem','Holland','King','Man_May','River_Jack']
    idList = {'Aus_Hill':[20008777,20006015],'Elem':[20005933],'Holland':[20005995],'King':[20001050],'Man_May':[20001163,20007855],'River_Jack':[20006201,20001769]}

    dirList = ['Aus_Hill','Elem','Holland','King','Man_May','River_Jack']
    idList = {'Aus_Hill':[20008777,20006015],'Elem':[20005933],'Holland':[20005995],'King':[20001050],'Man_May':[20001163,20007855],'River_Jack':[20006201,20001769]}

    dirList = ['Elem','King','Rest']
    idList = {'Elem':['20005933'],'King':['20001050'],'Rest':['20001766','20001769','20015435','20014926','20012513','20015473']}

    dirList = ['Elem','Rest']
    idList = {'Elem':['20005933'],'Rest':['20001769','20001050','20014926','20012513','20012791','20015292']}

    dirList = ['Rest']
    idList = {'Rest':['20014926']}

    dirList = ['Holland','Elem','Rest']
    idList = {'Holland':['20005995'],'Elem':['20005933'],'Rest':['20001769','20001050','20014926','20012513','20012791','20015292']}

    dirList = ['Elem']
    idList = {'Elem':['20005919']}

    fixed = False
    configDict = readConfig(dirList)

    for i in dirList:
        dirName = configDict['test_dir'] + '/' + i

        prepareItemMaster(configDict, idList[i], dirName)
        prepareItemDC(configDict, idList[i], dirName)
        prepareForecast(configDict, idList[i], dirName)
        preparePO(configDict, idList[i], dirName)
        # prepareConfig(configDict, dirName)

        # JavaCplexRun(dirName, fixed, configDict, idList[i])
        # JavaScipRun(dirName)
