import yaml

def import_yaml(archivo):
    with open(archivo, 'r') as file:
        Config = yaml.safe_load(file)
    return Config

def get_IpGestoresMae():
    IpGestoresHuawei = {}
    datos_Config = import_yaml('/usr/local/airflow/dags/cel_amovil/Config/IpGestores.yml')
    for host in datos_Config["gestoresHuawei"]:
        host_ip = host["ip"]
        host_mae = host["mae"]
        IpGestoresHuawei[host_mae] = host_ip
        print(f"IP {host_ip} corresponde a {host_mae}")
    
    return IpGestoresHuawei