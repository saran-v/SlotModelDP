import pandas as pd
import csv
from zipfile import ZipFile
import os
import shutil
import subprocess
import time
from datetime import datetime
import pyodbc
import sys
from datetime import datetime, timedelta

conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=SQLSPC19-1;'
                      'Database=SupplyChain;'
                      'Trusted_Connection=yes;')

def createDir(familyCode, timeStr):
    os.chdir(r"D:\Scripts\OPT\SlotModel\Opt")

    if '/' in familyCode:
        familyCode = familyCode.replace('/', '_')

    if not os.path.exists(familyCode):
        try:
            os.mkdir(familyCode)
            print("Directory ", familyCode, " Created ")
        except FileExistsError:
            print("Directory ", familyCode, " already exists")
    else:
        print("Directory already exists")

    os.chdir(r"S:\Merchandising_Shared\Supply Chain Automation\ReplanOpt\Outputs")

    if not os.path.exists(timeStr):
        try:
            os.mkdir(timeStr)
            os.mkdir(timeStr + '_aux')
            print("Directory ", timeStr, " Created ")
        except FileExistsError:
            print("Directory ", timeStr, " already exists")
    else:
        print("Directory already exists")


def prepareItemDC(familyCode):

    query = "SELECT * FROM [SupplyChain].[dmsc].[SCA_ARTICLE_DC] where [Family_Code_Desc] = ?;"  # " where Planner = ?;"
    df = pd.read_sql(query, conn, params=[familyCode])

    df['Vendor ID'] = df['Vendor ID'].fillna(0)
    df['Vendor ID'] = df['Vendor ID'].astype(int)
    df['Vendor ID'] = df['Vendor ID'].astype(str)

    # df.to_csv(r"D:/Scripts/OPT/SlotModel/Opt/item_1.csv", index=False)
    #
    # df = df[df['Family_Code_Desc'].notnull()] # skip if null family code
    # df.to_csv(r"D:/Scripts/OPT/SlotModel/Opt/item_2.csv", index=False)
    #
    # df = df[(df['Forecast Flag'] == 'Y')]  # filter by forecast flag
    # df.to_csv(r"D:/Scripts/OPT/SlotModel/Opt/item_3.csv", index=False)
    #
    # df = df[(df['Replenish Flag'] == 'Y')]  # filter by replenish flag
    # df.to_csv(r"D:/Scripts/OPT/SlotModel/Opt/item_4.csv", index=False)
    #
    # df = df[(df['Global Drop Status'] == 'N')]  # filter by global drop status
    # df.to_csv(r"D:/Scripts/OPT/SlotModel/Opt/item_5.csv", index=False)

    df = df[['Article', 'Article Description', 'Vendor ID', 'Vendor Name', 'Family Code - Key', 'site','Family_Code_Desc','Weeks Of Supply For SS','Unrestricted Stock','STO Inbound Qty','confirmedQty',
             'STO Outbound Qty','Volume','DSX LeadTime','Avg Fcst Units','Avg Fcst Cost','MaxCubesPerContainer','Incoterm Group','Minor_Code','Minor_Code_Description','Origin Country','Major Code Description']]

    df['Unrestricted Stock'] = df['Unrestricted Stock'].fillna(0)
    df['STO Inbound Qty'] = df['STO Inbound Qty'].fillna(0)
    df['STO Outbound Qty'] = df['STO Outbound Qty'].fillna(0)
    df['confirmedQty'] = df['confirmedQty'].fillna(0)

    df['Avg Fcst Units'] = df['Avg Fcst Units'].fillna(0)

    df['on_hand'] = (df['Unrestricted Stock'] + df['STO Inbound Qty'] ) - (df['confirmedQty'] + df['STO Outbound Qty'])
    df['sales'] = df['confirmedQty'] + df['Avg Fcst Units']
    df['cost'] = df['Avg Fcst Cost']/df["Avg Fcst Units"]

    final_df = df[['Article', 'site', 'Article Description', 'Vendor ID', 'Vendor Name', 'Family Code - Key', 'Family_Code_Desc','on_hand',
                            'Weeks Of Supply For SS','sales','Volume','DSX LeadTime','cost','MaxCubesPerContainer','Incoterm Group','Minor_Code',
                            'Minor_Code_Description','Origin Country','Major Code Description']]
    final_df.rename(columns={'Article': 'item', 'site': 'location', 'Article Description' : 'item_desc', 'Vendor ID': 'vendor_id',
                             'Vendor Name':'vendor_name', 'Family Code - Key': 'family_code','Family_Code_Desc': 'family_group', 'on_hand': 'on_hand', 'Weeks Of Supply For SS': 'safety_stock', 'Volume':'volume', 'DSX LeadTime':'leadtime',
                             'MaxCubesPerContainer':'MaxCubes','Incoterm Group':'incoterm_group','Minor_Code':'minor_code','Minor_Code_Description':'minor_code_desc','Origin Country':'origin_country',
                             'Major Code Description':'MajorCodeDesc'}, inplace=True)

    final_df.to_csv(r"D:/Scripts/OPT/SlotModel/Opt/item_master.csv", index=False)


def prepareForecast(familyCode):

    query = "SELECT * FROM [SupplyChain].[dmsc].[SCA_FORECAST_1YEAR] where [Family_Code_Desc] = ?;"  # " where Planner = ?;"
    df = pd.read_sql(query, conn, params=[familyCode])

    # vid = list(map(int, configDict['vendor_id']))
    df['VendorNumber'] = df['VendorNumber'].fillna(0)
    df['VendorNumber'] = df['VendorNumber'].astype(int)
    df['VendorNumber'] = df['VendorNumber'].astype(str)

    df = df[df['Family_Code_Desc'].notnull()] # skip if null family code

    final_df = df[['Article', 'DC','FiscalWeek','FiscalYear','Wkly Forecast in Unit']]
    final_df.rename(columns={'Article': 'item', 'DC': 'location', 'FiscalWeek': 'week', 'FiscalYear': 'year', 'Wkly Forecast in Unit': 'units'}, inplace=True)

    final_df.to_csv(r"D:/Scripts/OPT/SlotModel/Opt/forecast.csv", index=False)


def preparePO(familyCode):

    query = "SELECT * FROM [SupplyChain].[dmsc].[SCA_OPEN_PO] where [Family Code] = ?;"  # " where Planner = ?;"
    df = pd.read_sql(query, conn, params=[familyCode])

    df['VendorNumber'] = df['VendorNumber'].fillna(0)
    df['VendorNumber'] = df['VendorNumber'].astype(int)
    df['VendorNumber'] = df['VendorNumber'].astype(str)
    df = df[df['Family Code'].notnull()] # skip if null family code

    final_df = df[['PO Doc Nbr', 'PO Item Nbr', 'Article', 'Site','FiscalWeek','FiscalYear','Open PO Qty', 'PO Create Date']]
    final_df.rename(columns={'PO Doc Nbr': 'po_number', 'PO Item Nbr':'PO_Item_Nbr','Article':'Article', 'Site': 'Site', 'FiscalWeek': 'week', 'FiscalYear': 'year', 'Open PO Qty': 'units', 'PO Create Date': 'createDate'}, inplace=True)
    final_df.to_csv(r"D:/Scripts/OPT/SlotModel/Opt/open_po.csv", index=False)

    # a = final_df['po_number'].unique()
    #
    # poDate = {}
    # for po_number in a:
    #     try:
    #         query = "select [PO Doc Nbr], [Early Ship Date] from [SupplyChain].[dmsc].[PURCHASE ORDER ITEM] where [PO Doc Nbr] = ? order by [Early Ship Date] DESC;"  # " where Planner = ?;"
    #         df = pd.read_sql(query, conn, params=[po_number])
    #         poDate[po_number] = df.iloc[0][1]
    #     except Exception as e:
    #         print(po_number, familyCode)
    #         print('Error in preparePO:' + str(e))
    #         sys.exit("Error message")
    #
    # df_date = pd.DataFrame(poDate.items())
    # df_date.to_csv(r"D:/Scripts/OPT/SlotModel/Opt/ship_date.csv", index=False)

    return len(df.index)


def  JavaScipRun(familyCode, runType, timeStr):

    shutil.copyfile("S:\Merchandising_Shared\Supply Chain Automation\DSX Attributes & Transit Data\DSX Attributes.csv", "D:\Scripts\OPT\SlotModel\Opt\DSX Attributes.csv")
    shutil.copyfile("S:\Merchandising_Shared\Supply Chain Automation\DSX Attributes & Transit Data\Transit Times by Port of Origin.csv", "D:\Scripts\OPT\SlotModel\Opt\Transit Times by Port of Origin.csv")

    os.chdir(r"D:\Scripts\OPT\SlotModel\Opt")

    if os.path.isfile("result_java.csv"):
        os.remove("result_java.csv")

    _base_cmd = ['java', '-classpath',
                 'SlotModelR.jar;jna-5.11.0.jar;ortools-win32-x86-64-9.5.2237.jar;protobuf-java-3.21.5.jar;ortools-java-9.5.2237.jar',
                 'com.optimizer.Optimizer',runType]  # works

    subprocess.check_call(_base_cmd)

    # java is still running
    while not os.path.exists('result_java.csv'):
        time.sleep(1)

    if '/' in familyCode:
        familyCode = familyCode.replace('/', '_')

    shutil.copyfile("POAllocations_out.csv", 'S:\Merchandising_Shared\Supply Chain Automation\ReplanOpt\Outputs\\' + timeStr + "_aux\\" + "POAllocations_out_" + timeStr + familyCode + "_"+ runType + ".csv")
    shutil.copyfile("Inv_OH_out.csv", "S:\Merchandising_Shared\Supply Chain Automation\ReplanOpt\Outputs\\" + timeStr + "_aux\\" + "Inv_OH_out_" + timeStr + familyCode + "_"+ runType + ".csv")
    shutil.copyfile("Inv_Alloc_Summary_out.csv", "S:\Merchandising_Shared\Supply Chain Automation\ReplanOpt\Outputs\\" + timeStr + "_aux\\" + "Inv_Alloc_Summary_out_" + timeStr + familyCode + "_"+ runType + ".csv")
    shutil.copyfile("Inv_BackOrder_out.csv", "S:\Merchandising_Shared\Supply Chain Automation\ReplanOpt\Outputs\\" + timeStr + "_aux\\" + "Inv_BackOrder_out_" + timeStr + familyCode + "_"+ runType + ".csv")
    shutil.copyfile("Inv_Alloc_out.csv", "S:\Merchandising_Shared\Supply Chain Automation\ReplanOpt\Outputs\\" + timeStr + "_aux\\" + "Inv_Alloc_out_" + timeStr + familyCode + "_"+ runType + ".csv")
    shutil.copyfile("POUtilData_out.csv", "S:\Merchandising_Shared\Supply Chain Automation\ReplanOpt\Outputs\\" + timeStr + "_aux\\" + "POUtilData_out_" + timeStr + familyCode + "_"+ runType + ".csv")
    shutil.copyfile("Shortage_Analysis_out.csv", "S:\Merchandising_Shared\Supply Chain Automation\ReplanOpt\Outputs\\" + timeStr + "_aux\\" + "Shortage_Analysis_out_" + timeStr + familyCode + "_"+ runType + ".csv")

    os.chdir(r"D:\Scripts\OPT\SlotModel\Opt")

    shutil.copyfile("POAllocations_out.csv", familyCode + "\POAllocations_out_" + timeStr + familyCode + ".csv")
    shutil.copyfile("Inv_OH_out.csv", familyCode + "\Inv_OH_out_" + timeStr + familyCode + ".csv")
    shutil.copyfile("Inv_Alloc_Summary_out.csv", familyCode + "\Inv_Alloc_Summary_out_" + timeStr + familyCode + ".csv")
    shutil.copyfile("Inv_BackOrder_out.csv", familyCode + "\Inv_BackOrder_out_" + timeStr + familyCode + ".csv")
    shutil.copyfile("Inv_Alloc_out.csv", familyCode + "\Inv_Alloc_out_" + timeStr + familyCode + ".csv")
    shutil.copyfile("POUtilData_out.csv", familyCode + "\POUtilData_out_" + timeStr + familyCode + ".csv")
    shutil.copyfile("Shortage_Analysis_out.csv", familyCode + "\Shortage_Analysis_out_" + timeStr + familyCode + ".csv")

    return familyCode

def readFamilyCodeList():
    familyCodeList = []
    runTypeList = []
    os.chdir(r"S:\Merchandising_Shared\Supply Chain Automation\ReplanOpt\Inputs")
    shutil.copyfile('config.csv', r"D:\Scripts\OPT\SlotModel\Opt\config.csv")

    if not os.path.exists('familyCodeList.csv'):
        return []

    shutil.copyfile('familyCodeList.csv', r"D:\Scripts\OPT\SlotModel\Opt\familyCodeList.csv")

    os.chdir(r"D:\Scripts\OPT\SlotModel\Opt")

    with open('familyCodeList.csv', 'r') as fd:
        reader = csv.reader(fd)
        for row in reader:
            familyCodeList.append(row[0])
            runTypeList.append(row[1])

    return familyCodeList, runTypeList
    # do something


def prepareShipDate():

    try:
        date_var = datetime(2022, 1, 1)

        dt = datetime.now()
        td = timedelta(days=30)
        date_var1 = dt + td
        # date_var1 = datetime(date_var1)

        query = "select [PO Doc Nbr], [Early Ship Date] from [SupplyChain].[dmsc].[PURCHASE ORDER ITEM] where [Early Ship Date] > ?  and [Early Ship Date] < ?;"  # " where Planner = ?;" '2022-01-01' '2023-10-06'
        df = pd.read_sql(query, conn, params=[date_var, date_var1])

    except Exception as e:
        print('Error in prepareShipDate:' + str(e))
        sys.exit("Error message")

    df.to_csv(r"D:/Scripts/OPT/SlotModel/Opt/ship_date.csv", index=False)


if __name__ == '__main__':

    start = time.time()

    os.chdir(r"S:\Merchandising_Shared\Supply Chain Automation\ReplanOpt\Inputs")
    if not os.path.exists('familyCodeList.csv'):
        exit()

    familyCodeList, runTypeList = readFamilyCodeList()

    dt = datetime.now()
    str_date_time = dt.strftime("%m_%d_%H_%M")

    os.chdir(r"S:\Merchandising_Shared\Supply Chain Automation\ReplanOpt\Inputs")

    if os.path.exists('familyCodeList.csv'):
        os.rename('familyCodeList.csv', 'familyCodeList_' + str_date_time + '.csv')

    now = datetime.now()
    timeStr = now.strftime("%Y") + '_' + now.strftime("%m") + '_' + now.strftime("%d") + '_' + now.strftime("%H_%M_")
    # timeStr = '2023_10_18_15_45_'

    shortageFileList = []
    poAllocFileList = []
    prepareShipDate() # one time ship date preparation

    for familyCode, runType in zip(familyCodeList, runTypeList):
        print('Optimizing: ' + familyCode + "," + timeStr)
        if '/' in familyCode:
            familyCode = familyCode.replace('/', '_')

        # shortageFileList.append("S:\Merchandising_Shared\Supply Chain Automation\ReplanOpt\Outputs\\" + timeStr + "_aux\\" + "Shortage_Analysis_out_" + timeStr + familyCode + "_"+ runType + ".csv")
        # poAllocFileList.append("S:\Merchandising_Shared\Supply Chain Automation\ReplanOpt\Outputs\\" + timeStr + "_aux\\" + "POAllocations_out_" + timeStr + familyCode + "_"+ runType + ".csv")
        createDir(familyCode,timeStr)
        prepareItemDC(familyCode)
        prepareForecast(familyCode)
        numRec = preparePO(familyCode)
        if numRec > 0:
            fCode = JavaScipRun(familyCode, runType, timeStr)
            shortageFileList.append("S:\Merchandising_Shared\Supply Chain Automation\ReplanOpt\Outputs\\" + timeStr + "_aux\\" + "Shortage_Analysis_out_" + timeStr + fCode + "_"+ runType + ".csv")
            poAllocFileList.append("S:\Merchandising_Shared\Supply Chain Automation\ReplanOpt\Outputs\\" + timeStr + "_aux\\" + "POAllocations_out_" + timeStr + fCode + "_"+ runType + ".csv")

    df_consol = pd.DataFrame()
    for shortFile in shortageFileList:
        df = pd.read_csv(shortFile)
        df_consol = pd.concat([df_consol, df], axis=0)

    df_consol.to_csv(r"S:\Merchandising_Shared\Supply Chain Automation\ReplanOpt\Outputs\\" + timeStr + "\\" + "Shortage_Analysis_Summary_" + timeStr + ".csv", index=False)

    df_consol = pd.DataFrame()
    for poFile in poAllocFileList:
        df = pd.read_csv(poFile)
        df_consol = pd.concat([df_consol, df], axis=0)

    df_consol.to_csv(r"S:\Merchandising_Shared\Supply Chain Automation\ReplanOpt\Outputs\\" + timeStr + "\\" + "POAllocations_Summary_" + timeStr + ".csv", index=False)
    plannerList = df_consol['Planner'].unique()

    for planner in plannerList:
        df_planner = df_consol.loc[df_consol['Planner'] == planner]
        print('planner', planner)
        df_planner.to_csv(r"S:\Merchandising_Shared\Supply Chain Automation\ReplanOpt\Outputs\\" + str(timeStr) + "\\" + "POAllocations_Summary_" + str(planner) + "_" + str(timeStr) + ".csv", index=False)

    end = time.time()
    time_elapsed = end - start
    hours, rem = divmod(end - start, 3600)
    minutes, seconds = divmod(rem, 60)
    print("Time elapsed {:0>2}:{:0>2}:{:05.2f}".format(int(hours), int(minutes), seconds))
    # print("Time elapsed (Mins):", time_elapsed/3600)
