import pandas as pd
import csv
from zipfile import ZipFile
import os
import shutil
import subprocess
import time
from datetime import datetime
import pyodbc
from collections import OrderedDict
import sys


conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=SQLSPC19-1;'
                      'Database=SupplyChain;'
                      'Trusted_Connection=yes;')

def createDir(vendorId):
    os.chdir(r"D:\Scripts\OPT\SlotModel\Opt")

    if not os.path.exists(vendorId):
        try:
            os.mkdir(vendorId)
            print("Directory ", vendorId, " Created ")
        except FileExistsError:
            print("Directory ", vendorId, " already exists")
    else:
        print("Directory already exists")

def prepareItemDC(vendorId):

    query = "SELECT * FROM [SupplyChain].[dmsc].[SCA_ARTICLE_DC] where [Vendor ID] = ?;"  # " where Planner = ?;"
    df = pd.read_sql(query, conn, params=[vendorId])

    df['Vendor ID'] = df['Vendor ID'].fillna(0)
    df['Vendor ID'] = df['Vendor ID'].astype(int)
    df['Vendor ID'] = df['Vendor ID'].astype(str)

    df = df[df['Family_Code_Desc'].notnull()] # skip if null family code
    df = df[(df['Forecast Flag'] == 'Y')]  # filter by forecast flag
    df = df[(df['Replenish Flag'] == 'Y')]  # filter by replenish flag
    df = df[(df['Global Drop Status'] == 'N')]  # filter by global drop status

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

    final_df.to_csv(r"D:/Scripts/OPT/SlotModel/Opt/item_master.csv", index=False)


def prepareForecast(vendorId):

    query = "SELECT * FROM [SupplyChain].[dmsc].[SCA_FORECAST_1YEAR] where [VendorNumber] = ?;"  # " where Planner = ?;"
    df = pd.read_sql(query, conn, params=[vendorId])

    # vid = list(map(int, configDict['vendor_id']))
    df['VendorNumber'] = df['VendorNumber'].fillna(0)
    df['VendorNumber'] = df['VendorNumber'].astype(int)
    df['VendorNumber'] = df['VendorNumber'].astype(str)

    df = df[df['Family_Code_Desc'].notnull()] # skip if null family code

    final_df = df[['Article', 'DC','FiscalWeek','FiscalYear','Wkly Forecast in Unit']]
    final_df.rename(columns={'Article': 'item', 'DC': 'location', 'FiscalWeek': 'week', 'FiscalYear': 'year', 'Wkly Forecast in Unit': 'units'}, inplace=True)

    final_df.to_csv(r"D:/Scripts/OPT/SlotModel/Opt/forecast.csv", index=False)


def preparePO(vendorId):

    query = "SELECT * FROM [SupplyChain].[dmsc].[SCA_OPEN_PO] where [VendorNumber] = ?;"  # " where Planner = ?;"
    df = pd.read_sql(query, conn, params=[vendorId])

    df['VendorNumber'] = df['VendorNumber'].fillna(0)
    df['VendorNumber'] = df['VendorNumber'].astype(int)
    df['VendorNumber'] = df['VendorNumber'].astype(str)
    df = df[df['Family Code'].notnull()] # skip if null family code

    final_df = df[['PO Doc Nbr', 'PO Item Nbr', 'Article', 'Site','FiscalWeek','FiscalYear','Open PO Qty', 'PO Create Date']]
    final_df.rename(columns={'PO Doc Nbr': 'po_number', 'PO Item Nbr':'PO_Item_Nbr','Article':'Article', 'Site': 'Site', 'FiscalWeek': 'week', 'FiscalYear': 'year', 'Open PO Qty': 'units', 'PO Create Date': 'createDate'}, inplace=True)
    final_df.to_csv(r"D:/Scripts/OPT/SlotModel/Opt/open_po.csv", index=False)

    a = final_df['po_number'].unique()

    poDate = {}
    for po_number in a:
        query = "select [PO Doc Nbr], [Early Ship Date] from [SupplyChain].[dmsc].[PURCHASE ORDER ITEM] where [PO Doc Nbr] = ? order by [Early Ship Date] DESC;"  # " where Planner = ?;"
        df = pd.read_sql(query, conn, params=[po_number])
        poDate[po_number] = df.iloc[0][1]

    df_date = pd.DataFrame(poDate.items())
    df_date.to_csv(r"D:/Scripts/OPT/SlotModel/Opt/ship_date.csv", index=False)


def  JavaScipRun(vendorId):

    shutil.copyfile("S:\Merchandising_Shared\Supply Chain Automation\DSX Attributes & Transit Data\DSX Attributes.csv", "D:\Scripts\OPT\SlotModel\Opt\DSX Attributes.csv")
    shutil.copyfile("S:\Merchandising_Shared\Supply Chain Automation\DSX Attributes & Transit Data\Transit Times by Port of Origin.csv", "D:\Scripts\OPT\SlotModel\Opt\Transit Times by Port of Origin.csv")

    os.chdir(r"D:\Scripts\OPT\SlotModel\Opt")

    if os.path.isfile("result_java.csv"):
        os.remove("result_java.csv")

    _base_cmd = ['java', '-classpath',
                 'SlotModelR.jar;jna-5.11.0.jar;ortools-win32-x86-64-9.5.2237.jar;protobuf-java-3.21.5.jar;ortools-java-9.5.2237.jar',
                 'com.optimizer.Optimizer']  # works

    subprocess.check_call(_base_cmd)

    # java is still running
    while not os.path.exists('result_java.csv'):
        time.sleep(1)

    now = datetime.now()
    timeStr = now.strftime("%Y") + '_' + now.strftime("%m") + '_' + now.strftime("%d") + '_' + now.strftime("%H_%M_")

    shutil.copyfile("POAllocations_out.csv", "S:\Merchandising_Shared\Supply Chain Automation\ReplanOpt\Outputs\POAllocations_out_" +  timeStr + vendorId + ".csv")
    shutil.copyfile("Inv_OH_out.csv", "S:\Merchandising_Shared\Supply Chain Automation\ReplanOpt\Outputs\Inv_OH_out_" +  timeStr + vendorId + ".csv")
    shutil.copyfile("Inv_Alloc_Summary_out.csv", "S:\Merchandising_Shared\Supply Chain Automation\ReplanOpt\Outputs\Inv_Alloc_Summary_out_" +  timeStr + vendorId +".csv")
    shutil.copyfile("Inv_BackOrder_out.csv", "S:\Merchandising_Shared\Supply Chain Automation\ReplanOpt\Outputs\Inv_BackOrder_out_" +  timeStr + vendorId +".csv")
    shutil.copyfile("Inv_Alloc_out.csv", "S:\Merchandising_Shared\Supply Chain Automation\ReplanOpt\Outputs\Inv_Alloc_out_" +  timeStr + vendorId +".csv")
    shutil.copyfile("POUtilData_out.csv", "S:\Merchandising_Shared\Supply Chain Automation\ReplanOpt\Outputs\POUtilData_out_" +  timeStr + vendorId +".csv")

    os.chdir(r"D:\Scripts\OPT\SlotModel\Opt")

    shutil.copyfile("POAllocations_out.csv", vendorId + "\POAllocations_out_" +  timeStr + vendorId + ".csv")
    shutil.copyfile("Inv_OH_out.csv", vendorId + "\Inv_OH_out_" +  timeStr + vendorId + ".csv")
    shutil.copyfile("Inv_Alloc_Summary_out.csv", vendorId + "\Inv_Alloc_Summary_out_" +  timeStr + vendorId +".csv")
    shutil.copyfile("Inv_BackOrder_out.csv", vendorId + "\Inv_BackOrder_out_" +  timeStr + vendorId +".csv")
    shutil.copyfile("Inv_Alloc_out.csv", vendorId + "\Inv_Alloc_out_" +  timeStr + vendorId +".csv")
    shutil.copyfile("POUtilData_out.csv", vendorId + "\POUtilData_out_" +  timeStr + vendorId +".csv")

def readVendorList():
    vendorList = []
    os.chdir(r"S:\Merchandising_Shared\Supply Chain Automation\ReplanOpt\Inputs")
    shutil.copyfile('config.csv', r"D:\Scripts\OPT\SlotModel\Opt\config.csv")

    if not os.path.exists('vendorList.csv'):
        return []

    shutil.copyfile('vendorList.csv', r"D:\Scripts\OPT\SlotModel\Opt\vendorList.csv")

    os.chdir(r"D:\Scripts\OPT\SlotModel\Opt")

    with open('vendorList.csv', 'r') as fd:
        reader = csv.reader(fd)
        for row in reader:
            vendorList.append(row[0])

    return vendorList
    # do something


if __name__ == '__main__':

    from os import listdir
    from os.path import isfile, join

    fileNames = [f for f in listdir('D:\Scripts\Test') if isfile(join('D:\Scripts\Test', f))]

    dt = datetime.now()
    str_date_time = dt.strftime("%m_%d_%H_%M")
    now = datetime.now()
    timeStr = now.strftime("%Y") + '_' + now.strftime("%m") + '_' + now.strftime("%d") + '_' + now.strftime("%H_%M_")


    df_consol = pd.DataFrame()
    for poFile in fileNames:
        print(poFile)
        df = pd.read_csv(poFile)
        df_consol = pd.concat([df_consol, df], axis=0)

    plannerList = df_consol['Planner'].unique()

    for planner in plannerList:
        df_planner = df_consol.loc[df_consol['Planner'] == planner]
        df_planner.to_csv(r"D:\Scripts\Test\\" + "POAllocations_Summary_" + planner + "_" + timeStr + ".csv", index=False)
        # df_planner.to_csv(r"S:\Merchandising_Shared\Supply Chain Automation\ReplanOpt\Outputs\\" + timeStr + "\\" + "POAllocations_Summary_" + planner + "_" + timeStr + ".csv", index=False)

    sys.exit()

    plannerList = []
    # df_consol = pd.DataFrame()
    for poFile in fileNames:
        print(poFile)
        df = pd.read_csv(poFile)
        tList = df['Planner'].unique()

        for t in tList:
            print(tList)
            plannerList.append(t)

        del df

        # df_consol = pd.concat([df_consol, df], axis=0)
        # df_consol = pd.concat([df_consol, df_consol.loc[df_consol['Planner'] == 'test']], axis=0)

    pList = []
    [pList.append(x) for x in plannerList if x not in pList]

    # pList = []
    # for planner in plannerList:
    #     if planner not in pList:
    #         pList.append(planner)

    # pList = list(OrderedDict.fromkeys(plannerList))
    print(pList)

    # plannerList = df_consol['Planner'].unique()
    #
    # for planner in plannerList:
    #     df_planner = df_consol.loc[df_consol['Planner'] == planner]
    #     df_planner.to_csv(r"S:\Merchandising_Shared\Supply Chain Automation\ReplanOpt\Outputs\POAllocations_Summary_" + planner + "_" + timeStr + ".csv", index=False)

    # dt = datetime.now()
    # str_date_time = dt.strftime("%d_%m_%H_%M")
    #
    # os.chdir(r"S:\Merchandising_Shared\Supply Chain Automation\ReplanOpt\Inputs")
    #
    # if os.path.exists('familyCodeList.csv'):
    #     os.rename('familyCodeList.csv', 'familyCodeList_' + str_date_time + '.csv')

