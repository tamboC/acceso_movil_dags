import requests

"""
Esta función devuelve los parámetros en ADRN (@IGA)
"""
def ADRN_param_temp(url, vendor, tech, template):
    grupo = f"{categoria_q}_M_{vendor}_{tech}_PARAMS"
    tabla = f"{categoria_q}_M_{vendor}_{tech}_PARAMS"

    consulta_json = {
					"category": categoria_q,
					"group": grupo,
					"tableName": tabla ,
					"filterCriteria": [
						{
							"fieldName": "template", 
							"conditionType": "in",
							"values": template
						}
					],
					"orderCriteria": [
						{
							"fieldName": "object",
							"order": "asc"
						},
						{
							"fieldName": "paramName",
							"order": "asc"
						}
					],
					"resolveCustomChoiceValues": True
					}
    return requests.post(url, json=consulta_json).json()