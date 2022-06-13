"""
Pasos de hacer un DAG

1. TASKS
2. VARIABLES
3. FUNCTIONS
4. WORKFLOW

"""
import os
import pandas as pd
import re
import json
import requests
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from lib.teco_data_management import *
from teco_mae_operator.operators.tecoMaeOperator import TecoMaeOperator
from datetime import datetime, timedelta
from teco_db.operators.tecoMongoDbOperator import TecoMongoDb
from teco_db.operators.tecoMongoDbOperator import TecoReadMongo
from lib.teco_data_management import *


DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")
#arg
default_args = {
    'owner': 'amovil',
    'depends_on_past': False,
    'start_date': datetime(2021, 1, 1),
    'email': ['tambo_core@teco.com.ar'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup': False,
    'provide_context': True,
    'dag_type': 'custom'
}
        
#dag
dag = DAG(
dag_id= 'amovil_eMTC_2022', 
    schedule_interval= None, 
    default_args=default_args
)

#################################### 2) VARIABLES  ####################################

medi =['XCALVE_1', 'XCTREJ_1']

cmd = ['LST CELL:;', 'LST CELLRESEL:;','LST CELLSEL:;','LST CELLSIMAP:;' ,'LST CELLALGOSWITCH:;', 'LST CELLCECFG:;',
'LST CELLQCIPARA:;', 'LST EMTCSIBCONFIG:;' , 'LST UETIMERCONST:;', 'LST RLFTIMERCONSTGROUP:;' 
, 'LST CELLEMTCALGO:;', 'LST CERACHCFG:;', 'LST CEPUCCHCFG:;','LST CELLCESCHCFG:;' , 'LST INTRAFREQHOGROUP:;']
cmd_1 = ['LST GLOBALPROCSWITCH:;'] # Version para Sitios sin Fila-Columna
cmd_2 = ['LST QCIPARA:;', 'LST S1:;'] # Version para Sitios

#################################### 3) FUNCTIONS  ####################################
    
def parse_data(**kwargs):

    ##### Traemos el ADRN de Ingeniera #####

    url = "http://10.24.1.150/ingenier@/symfony/public/index.php/api/secure/shared-database/select-data"
    
    myjson = {"category": "ADRN",
                  "group": 'ADRN_M_H_4G_PARAMS',
                  "tableName": 'ADRN_M_H_4G_PARAMS',
                  "filterCriteria": [
                      {
						    "fieldName": "template", 
							"conditionType": "in",
							"values": ["Default"]
						}
                  ],
                  "orderCriteria": [
                      {
                          "fieldName": "object",
                          "order": "asc",
                          
                      },
                      {
                          "fieldName": "paramName",
                          "order": "asc"
                      }
                  ],
                  "resolveCustomChoiceValues": True
                  }
    
    api_to_json = requests.post(url, json=myjson).json()
    json_to_dataframe = pd.DataFrame(api_to_json["data"], columns=["id", "element", "object", "paramName",
                                                                    "additionalParam", "condition", "paramValue", "objectInfo", "conditionExtension1", "conditionExtension2",
                                                                    "version", "date", "comment", "action", "modification", "template"])
    json_to_dataframe = json_to_dataframe.drop(['id', 'date', 'action', 'modification', 'template', 'version', 'comment', 'objectInfo',
                                            'element', 'condition','conditionExtension1', 'conditionExtension2'], axis=1)
    json_to_dataframe = json_to_dataframe.fillna('')
    json_to_dataframe['paramValue'] = (json_to_dataframe['additionalParam'] + '-' +
                                        json_to_dataframe['paramValue']).where(json_to_dataframe['additionalParam'] != '',
                                                                        json_to_dataframe['paramValue'])
    json_to_dataframe = json_to_dataframe.drop(['additionalParam'], axis=1)

    # Reordenamos las columnas.
    columns_names = ['object', 'paramName', 'paramValue']
    json_to_dataframe.columns = columns_names

    # Armamos un diccionario aninado de los parametros:valor con el key: Objeto.

    dic_baseline = {k: f.groupby('paramName')['paramValue'].apply(list).to_dict() for k, f in json_to_dataframe.groupby('object')}

    # Quitamos los [ ] a los valores que son unicos y no hace falta que sea lista.
    
    dic_baseline = {k:{k: v[0] if len(v) == 1 else v for k,v in v.items()} for k,v in dic_baseline.items()}
    
    ##### Procesamos la informacion del MAE #####

    def df_parser_2(comandos,salida):
        lis = {}
        lista = []
        for line in salida:
            if comandos in line:
                for line in salida:
                    if line.startswith('+++'):
                            name_sitio = line.split('    ')[1]
                            dic_2 = {'name_sitio':name_sitio}
                
                    elif line.endswith('-'):
                                                   
                        for line in salida:
                            a = line.strip().split('=')
                            
                            if line.strip().endswith(')'):
                                break
                            elif a[0] != '':
                                k = a[0].strip()
                                v = a[1].strip()
                                lis.setdefault(k,[]).append(v)
                                x = a[0]
                            else:
                                x = k
                                v = a[1].strip()
                                lis.setdefault(k,[]).append(v)
                    if line.strip().endswith(':;'):
                        break
        lis.update(dic_2)
        lis2 = {k: v[0] if len(v) == 1 else v for k,v in lis.items()} 
        lista.append(lis2)          
        return lista

    
    def parser_huawei(comandos,salida):
        if comandos in cmd_2 :
            lista = []
            x = ''
            for line in salida:
                
                if comandos in line:
                    for line in salida:
                        if line.startswith('+++'):
                            name_sitio = line.split('    ')[1]
                            dic_3 = {'name_sitio':name_sitio}
                        
                        if line.endswith('-'):
                            x = 'columnas'
                            
                        elif  x == 'columnas':
                            col = [x.strip() for x in line.split('  ') if x != '']
                        
                            for line in salida:
                            
                                if line.strip().endswith(':;'):
                                    
                                    x = ''
                                    break
                                
                                elif ')' in line:
                                    x = '' 
                                    break
                                elif line.split() != []:
                                    a = line.split('  ')
                                    a2 = [x.strip() for x in a if x != '']
                                    dic_1 = dict(zip(col,a2))
                                    dic_1.update(dic_3)
                                    lista.append(dic_1)
                                    
                                if line.strip().endswith(':;'):
                                    
                                    x = ''
                                    break
                        elif '+++' in line or line.strip().endswith(':;'):    
                            break  
            return lista

        else:
            lista = []
            x = ''
            for line in salida:
                if comandos in line:
                    for line in salida:
                    
                        
                        if line.endswith('-'):
                            x = 'columnas'
                            
                        elif  x == 'columnas':
                            col = [x.strip() for x in line.split('  ') if x != '']
                        
                            for line in salida:
                            
                                if line.strip().endswith(':;'):
                                    
                                    x = ''
                                    
                                    break
                                
                                elif ')' in line:
                                    x = '' 
                                    break
                                elif line.split() != []:
                                    a = line.split('  ')
                                    a2 = [x.strip() for x in a if x != '']
                                    lista.append(dict(zip(col,a2)))
                                
                        elif '+++' in line:    
                            break
                            
                        
            return lista  
        
    dfs = {}
    output_medi = pull_data(kwargs,'get_data_medi')[0]
    for key,value in output_medi.items():   
        for comandos in cmd:
            salida = iter(value)
            output = parser_huawei(comandos,salida)
            dfs.setdefault(comandos,[]).append(output)
    
    output_medi_dif = pull_data(kwargs,'get_data_medi_dif')[0]
    for key,value in output_medi_dif.items():
        for comandos in cmd_1:
            salida = iter(value)
            output = df_parser_2(comandos,salida)
            dfs.setdefault(comandos,[]).append(output)
    
    
    output_medi_dif_1 = pull_data(kwargs,'get_data_medi_dif_1')[0]
    for key,value in output_medi_dif_1.items():
        for comandos in cmd_2:
            salida = iter(value)
            output = parser_huawei(comandos,salida)
            dfs.setdefault(comandos,[]).append(output)
    

    for k , v in dfs.items():
        v = [item for sublist in v for item in sublist]
        dfs.update({k:pd.DataFrame(v)})
    
    
    dfs['Cell'] = dfs.pop('LST CELL:;')
    dfs['Cell'] = dfs['Cell'].filter(['Local Cell ID', 'Cell Name', 'Frequency band'])
    dfs['Cell'] = dfs['Cell'].rename(columns={"Local Cell ID": "CellID"})
    dfs['Cell'] = dfs['Cell'][dfs['Cell']['Frequency band'] == '28'] # Filtramos por Banda
    
    dfs['CellResel'] = dfs.pop('LST CELLRESEL:;')
    dfs['CellResel'] = dfs['CellResel'].filter(['Local cell ID','eMTC Cell Reselection Priority', 
                                                    'Cell Reselection Time for E-UTRAN CE(s)',
                                                     'Min Required Quality Level for CE Mode B(dB)',
                                                     'Min Required RX Level for CE Mode B(2dBm)'])                                        
    dfs['CellResel'] = dfs['CellResel'].rename(columns={"Local cell ID": "CellID", 'eMTC Cell Reselection Priority':"EmtcCellReselPriority",
    'Cell Reselection Time for E-UTRAN CE(s)': 'TReselEutranCE', 'Min Required Quality Level for CE Mode B(dB)': 'QQualMinForCeModeB',
     'Min Required RX Level for CE Mode B(2dBm)': 'QRxLevMinForCeModeB' })
    dfs['CellResel'] = pd.merge(dfs['Cell'], dfs['CellResel'], how='inner' , left_on= 'CellID', right_on='CellID').drop_duplicates()
    
    dfs['CellSel'] = dfs.pop('LST CELLSEL:;')
    dfs['CellSel'] = dfs['CellSel'].filter(['Local cell ID','Minimum Required Quality Level for CE(dB)', 
                                                        'Minimum Required RX Level for CE(2dBm)'])
    dfs['CellSel'] = dfs['CellSel'].rename(columns={"Local cell ID": "CellID", 'Minimum Required Quality Level for CE(dB)': 'QQualMinCE',
    'Minimum Required RX Level for CE(2dBm)' :  'QRxLevMinCE'})
    dfs['CellSel'] = pd.merge(dfs['Cell'], dfs['CellSel'], how='inner' , left_on= 'CellID', right_on='CellID').drop_duplicates()  
    
    dfs['CellSiMap'] = dfs.pop('LST CELLSIMAP:;')
    dfs['CellSiMap'] = dfs['CellSiMap'].filter(['Local cell ID','MIB Repetition Control Opt Switch'])
    dfs['CellSiMap'] = dfs['CellSiMap'].rename(columns={"Local cell ID": "CellID", 'MIB Repetition Control Opt Switch':'MibRepetitionCtrlOptSwitch'}) 
    dfs['CellSiMap'] = pd.merge(dfs['Cell'], dfs['CellSiMap'], how='inner' , left_on= 'CellID', right_on='CellID').drop_duplicates()

    dfs['CellAlgoSwitch'] = dfs['LST CELLALGOSWITCH:;']
    dfs['CellAlgoSwitch'] = dfs['CellAlgoSwitch'].filter(['Local cell ID','CE Mode Handover Switch','Power Saving Switch for MTC UE'])
    dfs['CellAlgoSwitch'] = dfs['CellAlgoSwitch'].rename(columns={"Local cell ID": "CellID", 'CE Mode Handover Switch': 'CeModeHoSwitch',
    'Power Saving Switch for MTC UE': 'MTCPowerSavSwitch'})
    dfs['CellAlgoSwitch'] = pd.merge(dfs['Cell'], dfs['CellAlgoSwitch'], how='inner' , left_on= 'CellID', right_on='CellID').drop_duplicates() 
   
    dfs['CellCeCfg'] = dfs['LST CELLCECFG:;']
    dfs['CellCeCfg'] = dfs['CellCeCfg'].filter(['Local Cell ID','Coverage Level', 'RACH RSRP First Threshold(dBm)','RACH RSRP Second Threshold(dBm)','RACH RSRP Third Threshold(dBm)'])
    dfs['CellCeCfg'] = dfs['CellCeCfg'].rename(columns={"Local Cell ID": "CellID", 'Coverage Level': 'CoverageLevel',
    'RACH RSRP First Threshold(dBm)':'RachRsrpFstThd', 'RACH RSRP Second Threshold(dBm)': 'RachRsrpSndThd','RACH RSRP Third Threshold(dBm)': 'RachRsrpTrdThd'}) 
    dfs['CellCeCfg'] = pd.merge(dfs['Cell'], dfs['CellCeCfg'], how='inner' , left_on= 'CellID', right_on='CellID').drop_duplicates().fillna('')
 
    dfs['CellQciPara'] = dfs['LST CELLQCIPARA:;']
    dfs['CellQciPara'] = dfs['LST CELLQCIPARA:;'].filter(['Local Cell ID','QoS Class Indication','eMTC Mode A CL 1 DRX Parameter Group ID', 'eMTC Mode B CL 3 DRX Parameter Group ID','eMTC SRI Period'])
    dfs['CellQciPara'] = dfs['CellQciPara'].rename(columns={"Local Cell ID": "CellID", 'eMTC Mode A CL 1 DRX Parameter Group ID': 'EmtcModeADrxParaGroupId',
     'eMTC Mode B CL 3 DRX Parameter Group ID' : 'EmtcModeBDrxParaGroupId', 'eMTC SRI Period' : 'EmtcSriPeriod' })
    dfs['CellQciPara'] = dfs['CellQciPara'][dfs['CellQciPara']['QoS Class Indication'] == '9'] # Filtramos por QCI:9
    dfs['CellQciPara'] = pd.merge(dfs['Cell'], dfs['CellQciPara'], how= 'inner', left_on= 'CellID', right_on='CellID').drop_duplicates() 

    dfs['EmtcSibConfig'] = dfs['LST EMTCSIBCONFIG:;']
    dfs['EmtcSibConfig'] = dfs['EmtcSibConfig'].filter(['Local Cell ID','eMTC SIB Period']) 
    dfs['EmtcSibConfig'] = dfs['EmtcSibConfig'].rename(columns={"Local Cell ID": "CellID", 'eMTC SIB Period': 'EmtcSibPeriod' })
    dfs['EmtcSibConfig'] = pd.merge(dfs['Cell'], dfs['EmtcSibConfig'], how= 'inner', left_on= 'CellID', right_on='CellID').drop_duplicates() 

    dfs['UeTimerConst'] = dfs['LST UETIMERCONST:;']
    dfs['UeTimerConst'] = dfs['UeTimerConst'].filter(['Local cell ID','Timer 300 in CE Mode','Timer 301 in CE Mode', 'Timer 310 in CE Mode', 'Timer 311 in CE Mode'])
    dfs['UeTimerConst'] = dfs['UeTimerConst'].rename(columns={"Local cell ID": "CellID", 'Timer 300 in CE Mode': 'T300CE_UETIMERCONST','Timer 301 in CE Mode': 'T301CE', 'Timer 310 in CE Mode': 'T310CE' , 'Timer 311 in CE Mode': 'T311CE' })
    dfs['UeTimerConst'] = pd.merge(dfs['Cell'], dfs['UeTimerConst'], how= 'inner', left_on= 'CellID', right_on='CellID').drop_duplicates()
     
    dfs['RlfTimerConstGroup'] = dfs.pop('LST RLFTIMERCONSTGROUP:;')
    dfs['RlfTimerConstGroup'] = dfs['RlfTimerConstGroup'].filter(['Local cell ID', 'RLF timer and constants group ID','CE Timer 301','Timer 310 in CE Mode', 'Timer 311 in CE Mode'])
    dfs['RlfTimerConstGroup'] = dfs['RlfTimerConstGroup'].rename(columns={"Local cell ID": "CellID", 'RLF timer and constants group ID': 'RlfTimerConstGroupId',
                                'CE Timer 301': 'T301CE','Timer 310 in CE Mode': 'T310CE', 'Timer 311 in CE Mode': 'T311CE'})
    dfs['RlfTimerConstGroup'].reset_index(drop=True, inplace = True)
    dfs['RlfTimerConstGroup'] = dfs['RlfTimerConstGroup'][dfs['RlfTimerConstGroup']['RlfTimerConstGroupId'] == '0'] # Filtramos por GroupID:0
    dfs['RlfTimerConstGroup'].reset_index(drop=True, inplace = True)
    dfs['RlfTimerConstGroup'] = pd.merge(dfs['Cell'], dfs['RlfTimerConstGroup'], how= 'inner', left_on= 'CellID', right_on='CellID').drop_duplicates()
    
    dfs['CellEmtcAlgo'] = dfs.pop('LST CELLEMTCALGO:;')
    dfs['CellEmtcAlgo'] = dfs['CellEmtcAlgo'].filter(['Local Cell ID','DL LTE Reserved NB Count','eMTC Algorithm Switch', 'eMTC Aperiodic CQI Trigger Period', 'eMTC Target DL RB Usage(%)', 'eMTC DL Schedule Strategy', 'eMTC Dynamic HARQ-ACK Delay Parameter', 'eMTC No Service Aperiodic CQI Trigger Period', 'eMTC PDSCH/PUSCH Enhancement Switch', 'eMTC Target UL RB Usage(%)', 'UL LTE Reserved NB Count'])
    dfs['CellEmtcAlgo'] = dfs['CellEmtcAlgo'].rename(columns={"Local Cell ID": "CellID",'DL LTE Reserved NB Count': 'DlLteRsvNbCount','eMTC Algorithm Switch': 'EmtcAlgoSwitch', 'eMTC Aperiodic CQI Trigger Period': 'EmtcAperCqiTrigPrd', 'eMTC Target DL RB Usage(%)': 'EmtcDlRbTargetRatio', 'eMTC DL Schedule Strategy':'EmtcDlSchStrategy', 'eMTC Dynamic HARQ-ACK Delay Parameter': 'EmtcDynHarqAckDelayParam', 'eMTC No Service Aperiodic CQI Trigger Period': 'EmtcNoServAperCqiTrigPrd','eMTC PDSCH/PUSCH Enhancement Switch': 'EmtcPdschPuschEnhSwitch','eMTC Target UL RB Usage(%)':'EmtcUlRbTargetRatio','UL LTE Reserved NB Count': 'UlLteRsvNbCount'})
    dfs['CellEmtcAlgo'] = pd.merge(dfs['Cell'], dfs['CellEmtcAlgo'], how= 'inner', left_on= 'CellID', right_on='CellID').drop_duplicates()
    
    dfs['CeRachCfg'] = dfs.pop('LST CERACHCFG:;')
    dfs['CeRachCfg'] = dfs['CeRachCfg'].filter(['Local Cell ID', 'Timer for Contention Resolution(subframe)','Maximum Number of Preamble Attempt', 'Preamble Ratio(%)', 'Preamble Repetition Number(subframe)' , 'RACH Threshold Increase Coefficient' , 'Random Preamble Ratio(%)' ])  
    dfs['CeRachCfg'] = dfs['CeRachCfg'].rename(columns={"Local Cell ID": "CellID",'Timer for Contention Resolution(subframe)':'ContentionResolutionTimer','Maximum Number of Preamble Attempt':'MaxNumPrbAttempt','Preamble Ratio(%)': 'PreambleRatio', 'Preamble Repetition Number(subframe)':'PreambleRepetitionNum', 'RACH Threshold Increase Coefficient':'RachThldIncreaseCoeff','Random Preamble Ratio(%)':'RandomPreambleRatio'})
    dfs['CeRachCfg'] = pd.merge(dfs['Cell'], dfs['CeRachCfg'], how= 'inner', left_on= 'CellID', right_on='CellID').drop_duplicates()
        
    dfs['CePucchCfg'] = dfs.pop('LST CEPUCCHCFG:;')
    dfs['CePucchCfg'] = dfs['CePucchCfg'].filter(['Local Cell ID', 'Coverage Level', 'eMTC SRI Period Adaptation Switch', 'PUCCH Repetition Number']) 
    dfs['CePucchCfg'] = dfs['CePucchCfg'].rename(columns={"Local Cell ID": "CellID", 'Coverage Level': 'CoverageLevel', 'eMTC SRI Period Adaptation Switch': 'EmtcSriPeriodAdaptSw', 'PUCCH Repetition Number': 'PucchRepNum'})
    dfs['CePucchCfg'] = pd.merge(dfs['Cell'], dfs['CePucchCfg'], how= 'inner', left_on= 'CellID', right_on='CellID').drop_duplicates()
    
    dfs['IntraFreqHoGroup'] = dfs.pop('LST INTRAFREQHOGROUP:;')
    dfs['IntraFreqHoGroup'] = dfs['IntraFreqHoGroup'].filter(['Local cell ID', 'Intrafreq handover group ID','Intra-Freq A2 RSRP Threshold for CE(dBm)','Mode A Adjustment A1 RSRP Threshold(dBm)','Mode A Adjustment A2 RSRP Threshold(dBm)','Mode B Adjustment A2 RSRP Threshold(dBm)','Normal Coverage Adjustment A1 RSRP Threshold(dBm)'])
    dfs['IntraFreqHoGroup'] = dfs['IntraFreqHoGroup'].rename(columns={"Local cell ID": "CellID",'Intrafreq handover group ID':'IntraFreqHoGroupId','Intra-Freq A2 RSRP Threshold for CE(dBm)':'IntraFreqHoA2ThldRsrpCE','Mode A Adjustment A1 RSRP Threshold(dBm)':'ModeAAdjA1RsrpThld','Mode A Adjustment A2 RSRP Threshold(dBm)':'ModeAAdjA2RsrpThld','Mode B Adjustment A2 RSRP Threshold(dBm)':'ModeBAdjA2RsrpThld','Normal Coverage Adjustment A1 RSRP Threshold(dBm)':'NCAdjA1RsrpThld'  })
    dfs['IntraFreqHoGroup'].reset_index(drop=True, inplace = True)
    dfs['IntraFreqHoGroup'] = dfs['IntraFreqHoGroup'][dfs['IntraFreqHoGroup']['IntraFreqHoGroupId'] == '0'] # Filtramos por GroupID:0
    dfs['IntraFreqHoGroup'].reset_index(drop=True, inplace = True)
    dfs['IntraFreqHoGroup'] = pd.merge(dfs['Cell'], dfs['IntraFreqHoGroup'], how= 'inner', left_on= 'CellID', right_on='CellID').drop_duplicates()
    
    dfs['CellCeSchCfg'] = dfs.pop('LST CELLCESCHCFG:;')
    dfs['CellCeSchCfg'] = dfs['CellCeSchCfg'].filter(['Local Cell ID','Coverage Level' ,'Max Number of MPDCCH Repetitions in Mode A','Max Number of MPDCCH Repetitions in Mode B', 'Max Number of MPDCCH Repetitions for Paging','Paging Group Number','Max Number of PDSCH Repetitions in Mode A','Max Number of PDSCH Repetitions in Mode B','Max Number of PUSCH Repetitions in Mode A', 'Max Number of PUSCH Repetitions in Mode B'])
    dfs['CellCeSchCfg'] = dfs['CellCeSchCfg'].rename(columns={"Local Cell ID": "CellID", 'Coverage Level':'CoverageLevel', 'Max Number of MPDCCH Repetitions in Mode A':'MpdcchMaxNumRepModeA','Max Number of MPDCCH Repetitions in Mode B':'MpdcchMaxNumRepModeB','Max Number of MPDCCH Repetitions for Paging':'MpdcchMaxNumRepPaging','Paging Group Number':'PagingGroupNum','Max Number of PDSCH Repetitions in Mode A':'PdschMaxNumRepModeA','Max Number of PDSCH Repetitions in Mode B':'PdschMaxNumRepModeB','Max Number of PUSCH Repetitions in Mode A':'PuschMaxNumRepModeA','Max Number of PUSCH Repetitions in Mode B':'PuschMaxNumRepModeB'   }) 
    dfs['CellCeSchCfg'] = pd.merge(dfs['Cell'], dfs['CellCeSchCfg'], how= 'inner', left_on= 'CellID', right_on='CellID').drop_duplicates()
    
    # #### Por sitio #####
    
    dfs['QciPara'] = dfs.pop('LST QCIPARA:;')
    dfs['QciPara'] = dfs['QciPara'].filter(['name_sitio','QoS Class Identifier', 'CIoT UE Inactivity Timer(s)','DL Min GBR for Mode A(byte/s)','DL Min GBR for Mode B(byte/s)'])
    dfs['QciPara'] = dfs['QciPara'].rename(columns = {'QoS Class Identifier' : 'Qci', 'CIoT UE Inactivity Timer(s)' : 'CiotUeInactiveTimer','DL Min GBR for Mode A(byte/s)':'DlMinGbrForModeA' , 'DL Min GBR for Mode B(byte/s)':'DlMinGbrForModeB'   })
    dfs['QciPara'] = dfs['QciPara'][dfs['QciPara']['Qci'] == '9'] # Filtramos por QCI:9
    
    dfs['S1'] = dfs.pop('LST S1:;') 
    dfs['S1'] = dfs['S1'].filter(['name_sitio','MME Release']) 
    dfs['S1'] = dfs['S1'].rename(columns = {'MME Release' : 'MmeRelease'})

    dfs['GlobalProcSwitch'] = dfs.pop('LST GLOBALPROCSWITCH:;') 
    dfs['GlobalProcSwitch'] = dfs['GlobalProcSwitch'].filter(['name_sitio', "eMTC RRC Connection Punishment Threshold", "LTE RRC Connection Punishment Threshold"])
    dfs['GlobalProcSwitch'] = dfs['GlobalProcSwitch'].rename(columns= {"eMTC RRC Connection Punishment Threshold": 'EmtcRrcConnPunishmentThld', "LTE RRC Connection Punishment Threshold" :'LteRrcConnPunishmentThld' })
        
    def validar(fila):
        ver ={}
        fila.fillna('', inplace = True)
        a = fila
        a = fila.to_dict()        
        a = {k: v[0] if len(v) == 1 else v for k,v in a.items()}        
        for k,v in a.items():
            bs =  dic_baseline[obj]            
            if k in bs.keys():
                if isinstance(bs[k] , list):
                    des = [x for x in bs[k] if not x in a[k] or a[k].remove(x)]                    
                    res = {k:des}
                    if des != []:
                        ver.update(res)
                
                else:
                    d = {k:v}
                    x= {k:bs[k]}
                    res =dict(set(x.items()) - set(d.items()))
                    if res != {}:
                        ver.update(res)
        return ver
    
    for k,v in dfs.items():
        if 'Cell Name' in dfs[k].columns:
            if dfs[k]['Cell Name'].is_unique == False:
                dfs[k] = dfs[k].groupby('Cell Name').agg(lambda x: list(x)).reset_index()

    for obj, datos in dfs.items():
        if obj in dic_baseline.keys():
            dfs[obj]['Desvios'] = dfs[obj].apply(validar, axis=1)
    
    with pd.ExcelWriter('/io/cel_amovil/tmp/Analisis_eMTC.xlsx') as writer:    
        dfs['CellResel'].to_excel(writer, sheet_name = 'CellResel', index = False)
        dfs['CellSel'].to_excel(writer, sheet_name = 'CellSel', index = False)
        dfs['CellSiMap'].to_excel(writer, sheet_name = 'CellSiMap', index = False)
        dfs['CellAlgoSwitch'].to_excel(writer, sheet_name = 'CellAlgoSwitch', index = False)
        dfs['CellCeCfg'].to_excel(writer, sheet_name = 'CellCeCfg', index = False)
        dfs['CellQciPara'].to_excel(writer, sheet_name = 'CellQciPara', index = False)
        dfs['EmtcSibConfig'].to_excel(writer, sheet_name = 'EmtcSibConfig', index = False)
        dfs['UeTimerConst'].to_excel(writer, sheet_name = 'UeTimerConst', index = False)
        dfs['RlfTimerConstGroup'].to_excel(writer, sheet_name = 'RlfTimerConstGroup', index = False)
        dfs['CellEmtcAlgo'].to_excel(writer, sheet_name = 'CellEmtcAlgo', index = False)
        dfs['CeRachCfg'].to_excel(writer, sheet_name = 'CeRachCfg', index = False)
        dfs['CePucchCfg'].to_excel(writer, sheet_name = 'CePucchCfg', index = False)
        dfs['IntraFreqHoGroup'].to_excel(writer, sheet_name = 'IntraFreqHoGroup', index = False)
        dfs['CellCeSchCfg'].to_excel(writer, sheet_name = 'CellCeSchCfg', index = False)
        dfs['QciPara'].to_excel(writer, sheet_name = 'QciPara', index = False)
        dfs['S1'].to_excel(writer, sheet_name = 'S1', index = False)
        dfs['GlobalProcSwitch'].to_excel(writer, sheet_name = 'GlobalProcSwitch', index = False)      
                    
def resultados(**kwargs):
        resumen_medi = pull_data(kwargs,'get_data_medi')[1]
        print(resumen_medi)
        resumen_medi_dif = pull_data(kwargs,'get_data_medi_dif')[1]
        print(resumen_medi_dif)
        resumen_medi_dif_1 = pull_data(kwargs,'get_data_medi_dif_1')[1]
        print(resumen_medi_dif_1)
        
###################################### 1) TASKS  ######################################

_get_data_medi = TecoMaeOperator(
    task_id = 'get_data_medi',
    srv ='10.75.98.89',
    elements=medi,
    commands=cmd,
    dag=dag
)

_get_data_medi_dif = TecoMaeOperator(
    task_id = 'get_data_medi_dif',
    srv ='10.75.98.89',
    elements=medi,
    commands=cmd_1,
    dag=dag
)

_get_data_medi_dif_1 = TecoMaeOperator(
    task_id = 'get_data_medi_dif_1',
    srv ='10.75.98.89',
    elements=medi,
    commands=cmd_2,
    dag=dag
)

_parse_data = PythonOperator(
    task_id ='parse_data',
    python_callable = parse_data,
    dag=dag
)

_resultados = PythonOperator(
    task_id='resultados',
    python_callable = resultados,
    dag=dag
)



#################################### 4) WORKFLOW  #####################################

(_get_data_medi , _get_data_medi_dif, _get_data_medi_dif_1) >> _parse_data >> _resultados