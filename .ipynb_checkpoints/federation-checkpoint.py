import yaml
import copy
import os
import shutil


def execute_fdl (directory,script):
    
    print(script)

# Copiar el archivo
    try:
        shutil.copy(script, directory+'/'+script)
        print(f"Archivo {script} copiado a /{directory}.")
    except FileNotFoundError:
        print(f"El archivo {script} no se encontró.")
    except PermissionError:
        print(f"No se tienen permisos para copiar el archivo {script}.")
    except Exception as e:
        print(f"Ocurrió un error: {e}")
    
    add=0
# Listar los elementos del directorio
    if os.listdir(directory):
        for archive in os.listdir(directory):
            ruta_completa = os.path.join(directory, archive)
            fedecluster="fedecluster" in ruta_completa
            if os.path.isfile(ruta_completa) and ruta_completa.endswith('.yaml') and fedecluster:  # Verificar si es un archivo
                print("Executing... "+ ruta_completa)
                #resultado = subprocess.run(['oscar-cli', 'apply',ruta_completa], capture_output=True, text=True)
                # Mostrar el resultado en consola
                #print(resultado.stdout)
                add=add+1
    if add==0:
        print("No existen ficheros yaml para ejecutar")
        

# Ruta del archivo YAML
ruta_fdl_inicial = 'federation.yaml'

# Leer el contenido del archivo YAML y almacenarlo en una variable
with open(ruta_fdl_inicial, 'r') as file:
    fdl= yaml.safe_load(file)
    
orchestrator_name = list(fdl['functions']['oscar'][0].keys())[0]


atrib_federation = fdl.get('functions', {}).get('oscar', [{}])[0].get(orchestrator_name, {}).get('federation', None)
if atrib_federation is not None:
    if fdl['functions']['oscar'][0][orchestrator_name]['federation'] and fdl['functions']['oscar'][0][orchestrator_name]['delegation'] != "static":
        # Crear fdl producto a que es estructura de federacion de replicas.
        orchestrator= {
        "functions":{'oscar':list()}
        }
    # Obtener el primer cluster (orchestrator) y su nombre
        save= list(fdl['functions']['oscar'])

   # Creación de fdl_orchestrator por separado.
        orchestrator['functions']['oscar'].append((save[0]))
        orchestrator['clusters']=copy.copy(fdl['clusters'])
        orchestrator['storage_providers']=copy.copy(fdl['storage_providers'])

        cluster_coordinator = yaml.dump(orchestrator, sort_keys=False)            
    #print(cluster_coordinator)
        directory = 'replicas'

# Verificar si la carpeta existe, si no, crearla
        if not os.path.exists(directory):
            os.makedirs(directory)
        ruta_archivo =directory+"/fedecluster_"+ orchestrator_name.replace("-", "_")+'_orchestrator.yaml'

        with open(ruta_archivo, 'w') as file:
            yaml.dump(orchestrator, file, sort_keys=False)

        print(f"FDL guardado en {ruta_archivo}")

        orchestrator_replicas = fdl['functions']['oscar'][0][orchestrator_name]['replicas']
        orchestrator_output = fdl['functions']['oscar'][0][orchestrator_name]['output']

    # Crear un diccionario para mapear nombres de apartados a sus respectivos cluster_ids
        section_names = [list(section.keys())[0] for section in fdl['functions']['oscar']]
        name_to_section = {name: section for section in fdl['functions']['oscar'] for name in section.keys()}

        delegation= fdl['functions']['oscar'][0][orchestrator_name]['delegation']
        section_storage = list(fdl['storage_providers']['minio'])
    
    # Crear fdl de servicios replicas 
        for section in fdl['functions']['oscar'][1:]:
            element = {
                "functions":{'oscar':list()}
            }
    
            replica_name = list(section.keys())[0]
    #[section_name].append(['delegation']=copy.copy(delegation))
            section[replica_name]['federation']=copy.copy(fdl['functions']['oscar'][0][orchestrator_name]['federation'])
            section[replica_name]['delegation']=copy.copy(fdl['functions']['oscar'][0][orchestrator_name]['delegation'])
            section[replica_name]['replicas'] = copy.deepcopy(orchestrator_replicas)
    #section[section_name]['output'].append(first_output)
    # Verifica que 'output' sea una lista antes de intentar agregarle elementos
            if isinstance(section[replica_name]['output'], list):
                orchestrator_output_copy = copy.deepcopy(orchestrator_output)
                section[replica_name]['output'].extend(orchestrator_output_copy)
                for storage in section_storage:
                    if storage== section_names[0]:
                        minio_storage= "minio."+ storage
                        minio= section[replica_name]['output'][-1]
                        minio['storage_provider']= minio_storage
                    #first_output_copy['storage_provider'] = minio_storage
        
            else:
                raise TypeError(f"El valor de 'output' en {replica_name} no es una lista.")
    #print(section_name)
    # Modificar el 'cluster_id' en cada replica
            for  replica in section[replica_name]['replicas']:
        #section[section_name]['output']= first_output
                if replica['cluster_id'] in name_to_section:
                    if replica['cluster_id'] == replica_name:
                        replica['cluster_id'] = section_names[0]
                        replica['service_name']= fdl['functions']['oscar'][0][orchestrator_name]['name']
                
   
    # Cargar datos para generar fdl de cada servicio que tiene el sistema de replicas
            element['functions']['oscar'].append( section)
            element['clusters']=copy.copy(fdl['clusters'])
            element['storage_providers']=copy.copy(fdl['storage_providers'])
            fdl_yaml_replica = yaml.dump(element, sort_keys=False)            
    #print(fdl_yaml_replica)
            ruta_archivo = directory+"/fedecluster_"+replica_name.replace("-", "_")+'.yaml'

            with open(ruta_archivo, 'w') as file:
                yaml.dump(element, file, sort_keys=False)

            print(f"FDL guardado en {ruta_archivo}")
                

# Convertir el diccionario modificado a YAML para ver el resultado
            fdl_yaml_modified = yaml.dump(fdl, sort_keys=False)
            #print(type(fdl_yaml_modified))
        
        #Ejecutar los fdl creados
        script=copy.copy(fdl['functions']['oscar'][0][orchestrator_name]['script'])
        execute_fdl(directory,script)
        fdl_directory = directory+'/created_federation.yaml'

        with open(fdl_directory, 'w') as file:
            yaml.dump(fdl, file, sort_keys=False)

        print(f"FDL guardado en {fdl_directory}")

    else:
        print ('No se han creado archivos porque el sistema no es federado.')
else:
    print ('No se han creado archivos porque no se ha definido el atributo federation.')