# -*- coding: utf-8 -*-
import sys
import redis
from sshtunnel import SSHTunnelForwarder
from redis.exceptions import ResponseError
from progressbar import ProgressBar
from progressbar.widgets import Percentage, Bar, ETA
import pprint

def migrate(srchost, dsthost, srcport,dstport, db, flush_flag):

    if(srchost == dsthost and srcport == dstport):
        print('Source and destination must be different.')
        return

    source = redis.Redis(srchost,port=srcport, db=db)
    dest = redis.Redis(dsthost,port=dstport, db=db)

    if(flush_flag=='Y'):
        dest.flushdb()

    size = source.dbsize()

    if(size == 0):
        print('No keys found.')
        return

    progress_widgets = ['%d keys: ' % size, Percentage(), ' ', Bar(), ' ', ETA()]
    pbar = ProgressBar(widgets=progress_widgets, maxval=size).start()

    COUNT = 2000 # scan size

    cnt = 0
    non_existing = 0
    already_existing = 0
    cursor = 0

    while True:
        cursor, keys = source.scan(cursor, count=COUNT)
        pipeline = source.pipeline()
        for key in keys:
            pipeline.pttl(key)
            pipeline.dump(key)
        result = pipeline.execute()
        print('#######################################')
        #print(result)
        print('#######################################')
        pipeline = dest.pipeline()

        for key, ttl, data in zip(keys, result[::2], result[1::2]):
            if ttl is None:
                ttl = 0
            if data != None:
                pipeline.restore(key, ttl, data)
            else:
                non_existing += 1

        results = pipeline.execute(False)
        print('#######################################')
        #pprint(result)
        print('#######################################')
        for key, result in zip(keys, results):
            if result != 'OK':
                e = result
                if hasattr(e, 'message') and (e.message == 'BUSYKEY Target key name already exists.' or e.message == 'Target key name is busy.'):
                    already_existing += 1
                else:
                    print('Key failed:', key, repr(data), repr(result))
                    raise e

        if cursor == 0:
            break

        cnt += len(keys)
        pbar.update(min(size, cnt))

    pbar.finish()
    print('Keys disappeared on source during scan:', non_existing)
    print('Keys already existing on destination:', already_existing)

def get_input_val(input_type):
    while True:
        input_val=raw_input(input_type+" 입력하세요: ")
        if(input_val != ''):
            return input_val
        print("공백입력은 안됩니다. 다시 입력해주세요.")

def get_input_yn(input_type):
    while True:
        input_val=str(raw_input(input_type+" 입력하세요(Y/N): "))
        print(input_val)
        if(input_val in ['Y','N','y','n']):
            return input_val.upper
        print("Y나 N으로 다시 입력해주세요.")

def get_server_info():
    print("다시 입력은 AGAIN_")
    input_list=["source_bastion_ip",
                "source_bastion_user",
                "source_bastion_pwd",
                "source_endpoint",
                "target_bastion_ip",
                "target_bastion_user",
                "target_bastion_pwd",
                "target_endpoint",
                "mig_database_number"
                ]
    i=0
    server_info={}
    while i < len(input_list) :
        input_val=get_input_val(input_list[i])
        if(input_val=="AGAIN_"):
            i=0
            continue
        server_info[input_list[i]] = input_val
        i=i+1

    return server_info

def get_connect_info():
    print("다시 입력은 AGAIN_")
    input_list=["source_endpoint",
                "target_endpoint",
                "mig_database_number"
                ]
    i=0
    server_info={}
    while i < len(input_list) :
        input_val=get_input_val(input_list[i])
        if(input_val=="AGAIN_"):
            i=0
            continue
        server_info[input_list[i]] = input_val
        i=i+1

    return server_info
def MakeTunnel(bastion_ip,bastion_user,bastion_pwd,endpoint):
    tunnel=SSHTunnelForwarder(
        (bastion_ip, 22),
        ssh_username=bastion_user,
        ssh_password=bastion_pwd,
        remote_bind_address=(endpoint, 6379)
    )
    return tunnel

def IsUseTunnel():
    print("###################################################################")
    print("  1. 터널링사용")
    print("  2. 터널링사용 안함")
    input_val = InputNumber(1,2)
    print("###################################################################")
    return input_val
def InputNumber(min_number,max_number):
    while True:
        try:
            number = int(raw_input("숫자를 입력하세요: "))
            if(number >=min_number and number <= max_number):
                return number

            print(str(min_number)+"~"+str(max_number)+"까지 입력")
        except Exception as ex:
            continue
if __name__=='__main__':
    try:
        TUNNEL_FLAG=0
        use_tunnel_flag = IsUseTunnel()
        flush_flag = get_input_yn("flush 여부")
        #flush_flag='N'
        if(use_tunnel_flag == 1):
            server_info=get_server_info()
            source_tunnel = MakeTunnel(server_info["source_bastion_ip"],server_info["source_bastion_user"],server_info["source_bastion_pwd"],server_info["source_endpoint"])
            target_tunnel = MakeTunnel(server_info["target_bastion_ip"],server_info["target_bastion_user"],server_info["target_bastion_pwd"],server_info["target_endpoint"])
            source_tunnel.start()
            TUNNEL_FLAG=1
            target_tunnel.start()
            server_info["source_connect_ip"]='127.0.0.1'
            server_info["target_connect_ip"]='127.0.0.1'
            server_info["source_connect_port"]=source_tunnel.local_bind_port
            server_info["target_connect_port"]=target_tunnel.local_bind_port
            print("ASIS 터널링포트: "+str(source_tunnel.local_bind_port)+" ,TOBE 터널링포트: " + str(target_tunnel.local_bind_port))
            migrate(server_info["source_connect_ip"], server_info["target_connect_ip"], server_info["source_connect_port"],server_info["target_connect_port"], server_info["mig_database_number"], flush_flag)
        elif(use_tunnel_flag == 2):
            server_info=get_connect_info()


        if(TUNNEL_FLAG==1):
            source_tunnel.stop()
            target_tunnel.stop()
    except Exception as ex:
        if(TUNNEL_FLAG==1):
            source_tunnel.stop()
            target_tunnel.stop()
        print(ex)
        sys.exit()
