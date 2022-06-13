from lib.teco_mongodb import MongoManager
from IpGestoresMAE import get_IpGestoresMae

def get_central():
    ip_mae = get_IpGestoresMae()
    mongo = MongoManager('amovil_centrales')
    query = mongo.make_query()
    BSC6910_AMBA = []
    BSC6910_MEDI = []
    BSC6900_MEDI = []
    for q in query:
        if q['Modelo'][:7] == 'BSC6910':
            if q['Server'] == ip_mae['AMBA']:
                BSC6910_AMBA.append(q['Equipo'])
            else:
                BSC6910_MEDI.append(q['Equipo'])
        else:
            if q['Server'] == ip_mae['MEDI']:
                BSC6900_MEDI.append(q['Equipo'])
    return BSC6910_AMBA, BSC6910_MEDI, BSC6900_MEDI