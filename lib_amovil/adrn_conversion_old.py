from datetime import datetime, timedelta
import pandas as pd
import re
import requests
import os

from lib.teco_ingeniera import Ingeniera

# Selector de archivos de referencia HEDEX / NOLS
def selectArchivo(vend, tech):

    path = "/io/cel_amovil/per/ADRN/"

    if vend == "H":
        if tech == "4G":
            return f"{path}paralist_ratL_en_modificado.xlsx"
        elif tech == "5G":
            return f"{path}paralist_ratN_en_modificado.xlsx"
        else:
            return "NA"
    elif vend =="N": #ref_bts_parameters_sran21b_sran21a_02.xlsx
        if tech == "4G":
            return f"{path}ref_bts_parameters_sran21b_sran21a_02.xlsx"
        elif tech == "5G":
            return f"{path}ref_bts_parameters_sran21b_sran21a_02.xlsx"
        else:
            return "NA"
    else:
        return "error"

# Huawei - Armado de la lista de comandos
def mmlDiscover(campoComandos):
    
    listaComandos = {"MOD": "NA",
                     "LST": "NA",
                     "DSP": "NA",
                     "ADD": "NA",
                     "RMV": "NA",}
    
    for comando in campoComandos:
        if comando.strip()[:3:] == "MOD":
            listaComandos["MOD"] = comando.strip()
        elif comando.strip()[:3:] == "LST":
            listaComandos["LST"] = comando.strip()
        elif comando.strip()[:3:] == "DSP":
            listaComandos["DSP"] = comando.strip()
        elif comando.strip()[:3:] == "ADD":
            listaComandos["ADD"] = comando.strip()
        elif comando.strip()[:3:] == "RMV":
            listaComandos["RMV"] = comando.strip()
    
    return listaComandos

# Huawei - Funciones para procesar condiciones
def proxSeparador_huawei(condicion):
    if min(condicion.find("&&"), condicion.find("||")) != -1:
        return min(condicion.find("&&"), condicion.find("||"))
    elif condicion.find("&&") != -1:
        return condicion.find("&&")
    elif condicion.find("||") != -1:
        return condicion.find("||")
    else:
        return -1

def armadoCondiciones_huawei(condStr):
    condStr = condStr.replace(" ", "")
    condList=[]
    #for algo in condStr:
    if condStr.find("&&") == -1 and condStr.find("||") == -1:
        condList.append(condStr.strip().replace("(","").replace(")",""))
        #print("Hello")
    else:
        #print(condStr)
        for algo in condStr:
            separadorIndice = proxSeparador_huawei(condStr)
            if separadorIndice != -1:
                condList.append(condStr[:separadorIndice].strip().replace("(","").replace(")",""))
                condStr = condStr[separadorIndice+2:].strip()
            else:
                condList.append(condStr.strip().replace("(","").replace(")",""))
                break
            
    return condList

def buscoSimbolo_huawei(linea):
    if linea.find("!=") != -1:
        simbolo = "!="
    elif linea.find(">") != -1:
        simbolo = ">"
    elif linea.find("<") != -1:
        simbolo = "<"
    elif linea.find("=") != -1:
        simbolo = "="
    else:
        print(f"Error de símbolo en {condicion}")
        simbolo = ""
    return simbolo

def internal_value_nokia(paramType, paramRange, guiValue, formulaInternal):
    if paramType == "Number":
        try:
            eval(formulaInternal.replace("UI_VALUE", str(guiValue)))
        except:
            #print(f"No se puede resolver {formulaInternal} con el valor {guiValue}")
            return "error"
        else: 
            return eval(formulaInternal.replace("UI_VALUE", str(guiValue)))
    elif paramType == "Enumeration":
        paramRange_d = eval("{'" + paramRange.replace(";","','").replace(":","':'").replace("\n","") + "'}")
        for key, value in paramRange_d.items():
            if value == guiValue.strip():
                #print(key)
                return key
        return "error"
    elif paramType == "Boolean":
        if guiValue in ["true", "false"]:
            if guiValue == "true":
                return "1"
            else:
                return "0"
        else:
            return "error"
    elif paramType == "String":
        return guiValue
    elif paramType == "Bitmask":
        #print(f"{paramType} - {paramRange} - {guiValue} --> Valor interno: {internal_bitmask_nokia(guiValue, paramRange)}")
        #print(f"Valor interno: {internal_bitmask_nokia(guiValue, paramRange)}")
        return internal_bitmask_nokia(guiValue, paramRange)
    else:
        return "NA"

def internal_bitmask_nokia(bitmask, rango):
    bmList = []
    bmTemp = bitmask
    while bmTemp.find(";") != -1:
        bmList.append(bmTemp[:bmTemp.find(";")].strip())
        bmTemp = bmTemp[bmTemp.find(";") + 1 :].strip()
    
    bmList.append(bmTemp)
    internalValue = 0
    for bit in bmList:
        bitInd = int(bit[bit.find(" "):bit.find(":")].strip())                 # N° de bit
        bitVal = int(bit[bit.find(":", bit.find(":")+1)+1:][:1])               # Valor del bit
        bitName = bit[bit.find(":")+1:][:bit[bit.find(":")+1:].find(":")]      # Nombre del bit
        if bitName in rango:
            #print(f"El bit n° {bitInd}, se llama {bitName} y tiene el valor {bitVal}")
            internalValue += pow(2,bitInd) * bitVal
        else:
            return "error"

    return internalValue

def internal_value_huawei(paramType, paramEnum, paramVal, condition = False):
    if paramType == "Enumeration Type" or paramType == "Bit Field Type":
        paramEnum_d = eval("{'" +paramEnum.replace("~","':'").replace(",","','").replace(" ","") + "'}")
        if paramType == "Enumeration Type":
            #print(f"Internal Value: {paramEnum_d[paramVal]}")
            if paramVal in paramEnum_d:
                enumValue = paramEnum_d[paramVal]
                return enumValue
            else:
                return "error"     
    else:
        return paramVal

# ---------------------------------------------
"""
Esta función se utiliza para obtener los parámetros de una tecnología especificada.
Se importa de la siguiente forma:
::
    sys.path.insert(0, '/usr/local/tambo/cels/cel_amovil/airflow2/dags/lib_amovil/')
    from adrn_conversion import get_adrn_enriquecido
:param vendor: string que especifica el vendor
:type vendor: str
:param tech: string que especifica de que tecnologia se quieren los parámetros
:type tech: str
:param template: lista con valores a filtrar, por default es el template Default (["Default"])
:type template: list
:return: dict
**Ejemplo de uso**
::
    get_adrn_enriquecido('H', '4G')
    get_adrn_enriquecido('N', '3G')
    get_adrn_enriquecido('H', '2G', ["enDespliegue"])
"""
def get_adrn_enriquecido(vendor, tecnologia, template = ["Default"]):
    return consistencia_ADRN(vendor, tecnologia, template)
    
""" 
Verificador de consistencia de ADRN y enriquecedor de datos:
1- Consulta los datos del ADRN @IGA en base a los datos de entrada
    * vendor: 'H' o 'N'
    * tecnología: '2G', '3G', '4G' o '5G'
    * template: ["Default"] si se omite
2- Devuelve un diccionario con los datos de ADRN, pero sumando información útil del vendor.
3- Revisa el ADRN buscando inconsistencias y las imprime en el log
"""
def consistencia_ADRN(vendor, tecnologia, template):

    archivo = selectArchivo(vendor, tecnologia)
    #df = pd.read_excel(archivo, sheet_name='Parameter List')    # Dataframe del template (cambia de H a N)

    respuesta = Ingeniera.get_adrn(vendor, tecnologia, template)   # Consulta a IGA
    respuesta_data = respuesta["data"]
    respuesta_data_ok = []
    parametros = 0
    parametrosErrados = 0
    condicionesErradas = 0

    print(f"Comenzando análisis de coherencia de ADRN_M_{vendor}_{tecnologia}_PARAMS, template {template}")
    if vendor == "H":
        df = pd.read_excel(archivo, sheet_name='Parameter List') 
        for algo in respuesta_data:         
            parametros += 1
            paramMo = algo["object"]
            paramName = algo["paramName"]
            paramVal = algo["paramValue"]
            #print(f"MO: {paramMo}")
            #print(f"Parameter ID: {paramName}")
            if "additionalParam" in algo.keys():
                paramAdicVal = algo["additionalParam"]
                #print(f"Value (adic): {paramAdicVal}")
            #if "condition" in algo.keys():
            #    paramCondic = algo["condition"]
            #    #print(f"Condition: {paramCondic}")

            #print(f"Value: {paramVal}")
            if df[(df["MO"] == paramMo) & (df["Parameter ID"] == paramName)]["Value Type"].empty:
                print(f"{paramMo}-{paramName}: no existe")
                parametrosErrados += 1
                #respuesta_data.remove(algo)
                algo.update({"error": True})
                #algo.update({"value_type": "error", "gui_value_range": "error", "enumeration": "error", "internal_value": "error", "comando_MOD": "error", "comando_LST": "error", "comando_DSP": "error", "comando_ADD": "error", "comando_RMV": "error"})
            else:
                paramType = df[(df["MO"] == paramMo) & (df["Parameter ID"] == paramName)]["Value Type"].values[0]
                #print(f"Value Type: {paramType}")
                paramRange = df[(df["MO"] == paramMo) & (df["Parameter ID"] == paramName)]["GUI Value Range"].values[0]
                #print(f"GUI Value Range: {paramRange}")
                paramEnum = df[(df["MO"] == paramMo) & (df["Parameter ID"] == paramName)]["Enumeration Number/Bit"].values[0]
                #print(f"Enumeration Number/Bit: {paramEnum}")
                paramMml = df[(df["MO"] == paramMo) & (df["Parameter ID"] == paramName)]["MML Command"].values[0]
                paramMml = eval("['" + paramMml[:-1].replace("\n","','") + "']")
                paramMml_d = mmlDiscover(paramMml)
                keys = df[(df["MO"] == paramMo) & (df["IsKey"] == "YES")]['Parameter ID'].values.tolist()
                
                #---------------------------------------------------------------------------------------------------
                if paramType == "Enumeration Type" or paramType == "Bit Field Type":
                    paramEnum_d = eval("{'" +paramEnum.replace("~","':'").replace(",","','").replace(" ","") + "'}")
                    if paramType == "Enumeration Type":
                        #print(f"Internal Value: {paramEnum_d[paramVal]}")
                        if paramVal in paramEnum_d:
                            enumValue = paramEnum_d[paramVal]
                            algo.update({"value_type": paramType, "gui_value_range": paramRange, "enumeration": paramEnum, "internal_value": enumValue, "comando_MOD": paramMml_d["MOD"], "comando_LST": paramMml_d["LST"], "comando_DSP": paramMml_d["DSP"], "comando_ADD": paramMml_d["ADD"], "comando_RMV": paramMml_d["RMV"], "iskey": keys})
                        else:
                            print(f"{paramMo}-{paramName}: no existe el valor {paramVal}")
                            parametrosErrados += 1
                            enumValue = "error"
                            #respuesta_data.remove(algo)
                            algo.update({"error": True})
                        #print(paramMml_d)
                        #algo.update({"value_type": paramType, "gui_value_range": paramRange, "enumeration": paramEnum, "internal_value": enumValue, "comando_MOD": paramMml_d["MOD"], "comando_LST": paramMml_d["LST"], "comando_DSP": paramMml_d["DSP"], "comando_ADD": paramMml_d["ADD"], "comando_RMV": paramMml_d["RMV"]})
                    elif paramType == "Bit Field Type":
                        bitMasSignificativo = max(map(int, paramEnum_d.values()))
                        if paramAdicVal in paramEnum_d:
                            bitPosicion = paramEnum_d[paramAdicVal]
                            bitValue = f"{bitPosicion}_{bitMasSignificativo}_{paramVal}"
                            algo.update({"value_type": paramType, "gui_value_range": paramRange, "enumeration": paramEnum, "internal_value": bitValue, "comando_MOD": paramMml_d["MOD"], "comando_LST": paramMml_d["LST"], "comando_DSP": paramMml_d["DSP"], "comando_ADD": paramMml_d["ADD"], "comando_RMV": paramMml_d["RMV"], "iskey": keys})
                        else:
                            print(f"{paramMo}-{paramName}: no existe el bit {paramAdicVal}")
                            parametrosErrados += 1
                            bitPosicion = "error"
                            #respuesta_data.remove(algo)
                            algo.update({"error": True})
                        #bitValue = f"{bitPosicion}_{bitMasSignificativo}_{paramVal}"
                        #algo.update({"value_type": paramType, "gui_value_range": paramRange, "enumeration": paramEnum, "internal_value": bitValue, "comando_MOD": paramMml_d["MOD"], "comando_LST": paramMml_d["LST"], "comando_DSP": paramMml_d["DSP"], "comando_ADD": paramMml_d["ADD"], "comando_RMV": paramMml_d["RMV"]})
                else:
                    #print(f"Internal Value: {paramVal}")
                    algo.update({"value_type": paramType, "gui_value_range": paramRange, "enumeration": paramEnum, "internal_value": paramVal, "comando_MOD": paramMml_d["MOD"], "comando_LST": paramMml_d["LST"], "comando_DSP": paramMml_d["DSP"], "comando_ADD": paramMml_d["ADD"], "comando_RMV": paramMml_d["RMV"], "iskey": keys})
                
                #---------------------------------------------------------------------------------------------------

            #Chequeo de consistencia de la condición
            #if "condition" in algo.keys() and algo.get("error") == None:
            if algo.get("condition") != None and algo.get("error") == None:
                paramCondic = algo["condition"]
                #print(f"Condition: {paramCondic}")
                condiciones = armadoCondiciones_huawei(paramCondic) # Obtengo una lista con las condiciones
                #print(condiciones)
                for condicion in condiciones:
                    simbolo = buscoSimbolo_huawei(condicion)
                    paramMoCond = condicion[:condicion.find(".")]
                    paramNameCond = condicion[condicion.find(".") + 1 : condicion.find(simbolo)]
                    paramValCond = condicion[condicion.find(simbolo) + len(simbolo):]
                    if df[(df["MO"] == paramMoCond) & (df["Parameter ID"] == paramNameCond)].empty:
                        print(f"La condición {condicion} no es válida, no se encuentra la combinación MO-Param (objeto {paramMo} - parámetro {paramName}")
                        condicionesErradas += 1
                        #respuesta_data.remove(algo)
                        algo.update({"error": True})
                    ### ---> Acá debo poner un else y buscar el internal de la condición para sumarlo <--- ###
                    else:
                        paramTypeCond = df[(df["MO"] == paramMoCond) & (df["Parameter ID"] == paramNameCond)]["Value Type"].values[0]
                        paramEnumCond =  df[(df["MO"] == paramMoCond) & (df["Parameter ID"] == paramNameCond)]["Enumeration Number/Bit"].values[0]
                        #paramEnum_d = eval("{'" +paramEnum.replace("~","':'").replace(",","','").replace(" ","") + "'}")
                        temp = internal_value_huawei(paramTypeCond, paramEnumCond, paramValCond)
                        if temp == "error":
                            print(f"------------------------------> La condición {condicion} no es válida, tiene valores ({paramValCond}) inválidos (objeto {paramMo} - parámetro {paramName})")
                            condicionesErradas += 1
                            algo.update({"error": True})
                condition = algo['condition'].replace(' ', '').replace('=', ' == ').replace('! == ', ' != ').replace('||', ' or ').replace('&&', ' and ')
                r = re.findall(r'(\w+).(\w+)(\s*!=\s*|\s*==\s*|[<>])(-*\w+-*\d*)', condition)
                for i in r:
                    number_compare = number_compare_end = ''
                    element = df[(df["MO"] == i[0]) & (df["Parameter ID"] == i[1])]['Element'].values[0]
                    if '<' in i or ' < ' in i or '>' in i or ' > ' in i:
                        number_compare = 'int('
                        number_compare_end = ')'
                    if paramMo.upper() == i[0].upper():
                        condition = condition.replace(f'{i[0]}.{i[1]}', f'id_condition["{i[1].upper()}"]')
                    elif element == 'Cell':
                        condition = condition.replace(f'{i[0]}.{i[1]}', f'{number_compare}conditions[0]["{i[0].upper()}"]["{i[1].upper()}"][cell]{number_compare_end}')
                    else:
                        condition = condition.replace(f'{i[0]}.{i[1]}', f'{number_compare}conditions[0]["{i[0].upper()}"]["{i[1].upper()}"]{number_compare_end}')
                    if len(i[-1]) > 1 and not i[-1][1:].isdigit():
                        internal_values = df[(df["MO"] == i[0]) & (df["Parameter ID"] == i[1])]["Enumeration Number/Bit"].values[0]
                        internal_values = eval("{'" + internal_values.replace("~","':'").replace(",","','").replace(" ","") + "'}")
                        if '-' in i[-1]:
                            index = i[-1].index('-') + 1
                            condition = condition.replace(f' {i[-1]}', f' "{i[-1][index:]}"')
                            if element == 'Cell':
                                condition = condition.replace(f'conditions[0]["{i[0].upper()}"]["{i[1].upper()}"][cell]', f'str(bin(int(conditions[0]["{i[0].upper()}"]["{i[1].upper()}"][cell]))[2:])[{-(int(internal_values[i[-1][:index-1]])+1)}]')
                            else:
                                condition = condition.replace(f'conditions[0]["{i[0].upper()}"]["{i[1].upper()}"]', f'str(bin(int(conditions[0]["{i[0].upper()}"]["{i[1].upper()}"]))[2:])[{-(int(internal_values[i[-1][:index-1]])+1)}]')
                        else:
                            condition = condition.replace(f' {i[-1]}', f' "{internal_values[i[-1]]}"')
                    else:
                        condition = condition.replace(f' {i[-1]}', f' "{i[-1]}"')
                algo['condition_xml'] = condition



            if algo.get("error") == None:
                respuesta_data_ok.append(algo)

    elif vendor == "N":
        # Selección de columnas 'útiles' del Excel de Nokia
        #colList = (0, 3, 6, 7, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67)
        colList = (6, 7, 10, 13, 16, 19, 22, 25, 28, 31, 34, 37, 40, 43, 46, 49, 52, 55, 58, 61, 64)
        df = pd.read_excel(archivo, header = 2, usecols = colList, sheet_name='Parameter List')    #Dataframe del template
        for algo in respuesta_data:
            parametros += 1
            paramMo = algo["object"]
            paramName = algo["paramName"]
            paramVal = algo["paramValue"]
            paramAdicVal = "NA"

            if "additionalParam" in algo.keys():
                paramAdicVal = algo["additionalParam"]
            if "condition" in algo.keys():
                paramCondic = algo["condition"]
            if paramAdicVal == "NA":
                if df[(df["MO Class"] == paramMo) & (df["Abbreviated Name"] == paramName) & (df["Parent Structure"] != df["Parent Structure"])]["Data Type"].empty:
                    print(f"{paramMo} - {paramName}: no existe")
                    parametrosErrados += 1
                    algo.update({"error": True})
                else:
                    paramType = df[(df["MO Class"] == paramMo) & (df["Abbreviated Name"] == paramName) & (df["Parent Structure"] != df["Parent Structure"])]["Data Type"].values[0]
                    paramRange = df[(df["MO Class"] == paramMo) & (df["Abbreviated Name"] == paramName) & (df["Parent Structure"] != df["Parent Structure"])]["Range and step"].values[0]
                    formulaGUI = df[(df["MO Class"] == paramMo) & (df["Abbreviated Name"] == paramName) & (df["Parent Structure"] != df["Parent Structure"])]["Formula for Getting Internal Value"].values[0]
                    modType = df[(df["MO Class"] == paramMo) & (df["Abbreviated Name"] == paramName) & (df["Parent Structure"] != df["Parent Structure"])]["Modification"].values[0]
            elif paramAdicVal != "NA":
                if df[(df["MO Class"] == paramMo) & (df["Abbreviated Name"] == paramName) & (df["Parent Structure"] == paramAdicVal)]["Data Type"].empty:
                    print(f"{paramMo} - {paramName} - {paramAdicVal}: no existe")
                    parametrosErrados += 1
                    algo.update({"error": True})
                else:
                    paramType = df[(df["MO Class"] == paramMo) & (df["Abbreviated Name"] == paramName) & (df["Parent Structure"] == paramAdicVal)]["Data Type"].values[0]
                    paramRange = df[(df["MO Class"] == paramMo) & (df["Abbreviated Name"] == paramName) & (df["Parent Structure"] == paramAdicVal)]["Range and step"].values[0]
                    formulaGUI = df[(df["MO Class"] == paramMo) & (df["Abbreviated Name"] == paramName) & (df["Parent Structure"] == paramAdicVal)]["Formula for Getting Internal Value"].values[0]
                    modType = df[(df["MO Class"] == paramMo) & (df["Abbreviated Name"] == paramName) & (df["Parent Structure"] == paramAdicVal)]["Modification"].values[0]
            if algo.get("error") == None:
                paramInternaValue = internal_value_nokia(paramType, paramRange, algo["paramValue"], formulaGUI)
                if paramInternaValue == "error":
                    parametrosErrados += 1
                    algo.update({"error": True})
                    GUI = algo["paramValue"]
                    if paramType == "Number":
                        print(f"No se puede resolver el valor interno de {paramMo} - {paramName} ({paramType}) con valor GUI {GUI} ({formulaGUI})")
                    else:
                        print(f"No se puede resolver el valor interno de {paramMo} - {paramName} ({paramType})  con valor GUI {GUI}")
                else:
                    algo.update({"value_type": paramType, "iv_value_range": paramRange, "formula_gui_iv": formulaGUI, "internal_value": paramInternaValue, "modification": modType})
                    #if paramType == "Bitmask":
                    #    print(f"Valor interno del Bitmask = {paramInternaValue}")
            
            #if algo.get("error") == None and algo.get("condition") != None:
            if algo.get("condition") != None and algo.get("error") == None:
                condicion_temp = algo["condition"]
                print(f"Debo resolver la condición {condicion_temp}")
                """ 
                Acá debo resolver las cndiciones de Nokia
                """

            if algo.get("error") == None:
                respuesta_data_ok.append(algo)

    print(f"Fin del análisis. Se verificaron {parametros} parámetros, encontrando {parametrosErrados + condicionesErradas} errores")
       
    return respuesta_data_ok


def formated_conditions(mo, conditions, id_conditions, condition):
    regex = re.split('\s+or\s+|\s+and\s+|\s*!=\s*|\s*=\s*|[<>]|\(|\)', condition)
    for r in regex:
        if '.' in r:
            part = r.upper()
            obj, param = part.split('.')
            if obj == mo:
                id_conditions[part.upper()] = param.upper()
            else:
                conditions[part.upper()] = param.upper()


def get_ing_params(vendor, tecnologia, template):
    ing_params = {}
    param_conditions = {}
    id_conditions = {}
    ing = consistencia_ADRN(vendor, tecnologia, template)
    for parameter in ing:
        obj = parameter['object'].upper()
        paramName = parameter['paramName'].upper()
        value_type = parameter['value_type']
        target = parameter['internal_value']
        element = parameter['element']
        command_MOD = parameter['comando_MOD']
        iskey = list(map(lambda x: x.upper(), parameter['iskey']))
        additionalParam = parameter.get('additionalParam', '').upper()
        enumeration = parameter.get('enumeration')
        condition = parameter.get('condition')
        condition_xml = parameter.get('condition_xml')
        ing_params[obj] = ing_params.get(obj, {'command_MOD': command_MOD, 'iskey': iskey})
        if additionalParam:
            ing_params[obj][paramName] = ing_params[obj].get(paramName, {'value_type': value_type})
            ing_params[obj][paramName][additionalParam] = {'element': element, 'target': target}
            if condition:
                ing_params[obj][paramName][additionalParam]['condition'] = condition
                ing_params[obj][paramName][additionalParam]['condition_xml'] = condition_xml
                formated_conditions(obj, param_conditions, id_conditions, condition)
        else:
            ing_params[obj][paramName] = {'value_type': value_type, 'element': element, 'target': target}
            if value_type == 'Enumeration Type':
                ing_params[obj][paramName]['enumeration'] = enumeration
            if condition:
                ing_params[obj][paramName]['condition'] = condition
                ing_params[obj][paramName]['condition_xml'] = condition_xml
                formated_conditions(obj, param_conditions, id_conditions, condition)
    return ing_params, param_conditions, id_conditions