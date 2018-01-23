from kafka import KafkaConsumer
from kafka import KafkaProducer
import numpy as np


active_connections = np.zeros([32])
active_connections = np.vstack((active_connections, np.zeros([32])))
# Siempre hay dos conexiones de inicio falsas con ceros para facilitar el código

#Connection ID
SRC_IP = 0
SRC_PORT = 1
DST_IP = 2
DST_PORT = 3

#Connection type
PROTOCOL = 4
SERVICE = 5

#Common Features
DURATION = 6
SRC_BYTES = 7
DST_BYTES = 8
SRC_TTL = 9
DST_TTL = 10
SRC_PACKAGES = 11
DST_PACKAGES = 12
SRC_MEAN = 15
DST_MEAN = 16
EPOCH_START = 17
EPOCH_END = 18

#Context 100 connections
IS_SM_IP_PORTS = 21
CT_SRV_SRC = 22
CT_SRV_DST = 23
CT_DST_LTM = 24
CT_SRC_LTM = 25
CT_SRC_DST_PORT_LTM = 26
CT_DST_SRC_PORT_LTM = 27
CT_DST_SRC_LTM = 28

#Only TCP features
SRC_TCPB = 13
DST_TCPB = 14
SYN_ACK = 19
ACK_DAT = 20

#Auxiliar flags
AUX_RTT_FLAG = 29
AUX_VALID_TRACE = 30
AUX_EPOCH_START = 31

# PARSER ORDER-> [SRC_IP, DST_IP, PROTOCOL, TTL_PACKAGE, IP_FLAGS, SRC_PORT, DST_PORT,
#                TCP_FLAGS, SERVICE, SEQ_NUMBER, WINDOW_NUMBER, EPOCH_TIME, SIZE]

P_SRC_IP = 0
P_DST_IP = 1
P_PROTOCOL = 2
P_TTL = 3
P_IP_FLAGS = 4
P_SRC_PORT = 5
P_DST_PORT = 6
P_TCP_FLAGS = 7
P_SERVICE = 8
P_SEQ_NUMBER = 9
P_WINDOW_NUMBER = 10
P_EPOCH_TIME = 11
P_SIZE = 12

OUTPUT_FILE_PATH = "./data/test/generated/test_writing.csv"


# Solo se soportan los protocolos tcp y udp de momento
def generate_new_connection(package_vector):
    new_trace = np.array([])
    new_trace = np.append(new_trace, package_vector[P_SRC_IP])  # 0 SRC_IP
    new_trace = np.append(new_trace, package_vector[P_SRC_PORT])  # 1 SRC_PORT
    new_trace = np.append(new_trace, package_vector[P_DST_IP])  # 2 DST_IP
    new_trace = np.append(new_trace, package_vector[P_DST_PORT])  # 3 DST_PORT
    new_trace = np.append(new_trace, package_vector[P_PROTOCOL])  # 4 PROTOCOL
    new_trace = np.append(new_trace, package_vector[P_SERVICE])  # 5 SERVICE
    new_trace = np.append(new_trace, 0)  # 6 DURATION
    new_trace = np.append(new_trace, package_vector[P_SIZE])  # 7 SRC_BYTES
    new_trace = np.append(new_trace, 0)  # 8 DST_BYTES
    new_trace = np.append(new_trace, package_vector[P_TTL])  # 9 SRC_TTL
    new_trace = np.append(new_trace, 0)  # 10 DST_TTL
    new_trace = np.append(new_trace, 1)  # 11 SRC_PACKAGES
    new_trace = np.append(new_trace, 0)  # 12 DST_PACKAGES

    if int(package_vector[P_PROTOCOL]) == 6:  # TCP
        new_trace = np.append(new_trace, package_vector[P_SEQ_NUMBER])  # 13 SRC_TCPB
        new_trace = np.append(new_trace, "")  # 14 DST_TCPB

    elif int(package_vector[P_PROTOCOL]) == 17:  # UDP
        new_trace = np.append(new_trace, 0)  # 13 SRC_TCPB
        new_trace = np.append(new_trace, 0)  # 14 DST_TCPB

    else:  # TODO: AÑDIR PROTOCOLOS EN UN FUTURO
        new_trace = np.append(new_trace, 0)  # 13 SRC_TCPB
        new_trace = np.append(new_trace, 0)  # 14 DST_TCPB

    new_trace = np.append(new_trace, int(round(float(package_vector[P_SIZE]))))  # 15 SRC_MEAN
    new_trace = np.append(new_trace, 0)  # 16 DST_MEAN
    new_trace = np.append(new_trace, int(round(float(package_vector[P_EPOCH_TIME]))))  # 17 EPOCH START
    new_trace = np.append(new_trace, int(round(float(package_vector[P_EPOCH_TIME]))))  # 18 EPOCH END

    new_trace = np.append(new_trace, package_vector[
        P_EPOCH_TIME])  # 19 SYN_ACK #TODO: ver si funciona el supuesto de que el primero siempre es SYN Rober
    new_trace = np.append(new_trace, 0)  # 20 ACK_DAT

    new_trace = np.append(new_trace, is_sm_ip_ports(package_vector))  # 21 IS_SM_IP_PORTS

    ct_srv_src, ct_srv_dst, ct_dst_ltm, ct_src_ltm, ct_src_dst_port_ltm, ct_dst_src_port_ltm, ct_dst_src_ltm = update_ct_ltm(
        new_trace, package_vector, 0)

    new_trace = np.append(new_trace, ct_srv_src)  # 22 CT_SRV_SRC
    new_trace = np.append(new_trace, ct_srv_dst)  # 23 CT_SRV_DST
    new_trace = np.append(new_trace, ct_dst_ltm)  # 24 CT_DST_LTM
    new_trace = np.append(new_trace, ct_src_ltm)  # 25 CT_SRC_LTM
    new_trace = np.append(new_trace, ct_src_dst_port_ltm)  # 26 CT_SRC_DST_PORT_LTM
    new_trace = np.append(new_trace, ct_dst_src_port_ltm)  # 27 CT_DST_SRC_PORT_LTM
    new_trace = np.append(new_trace, ct_dst_src_ltm)  # 28 CT_DST_SRC_LTM

    new_trace = np.append(new_trace, 0)  # 29 AUX_RTT_FLAG: 0->Waiting for SYN_ACK, 1->Waiting for ACK, 2->ESTABLISHED
    new_trace = np.append(new_trace, 0)  # 30 AUX_VALID_TRACE 0->Invalid, 1->Valid
    new_trace = np.append(new_trace, float(package_vector[P_EPOCH_TIME]))  # 31 AUX_EPOCH_START->Invalid, 1->Valid

    return new_trace


def get_active_connection(package_vector):
    for index, connection in enumerate(active_connections):
        if (connection[0] == package_vector[0] and connection[2] == package_vector[1]) or (
                        connection[0] == package_vector[1] and connection[2] == package_vector[0]):
            if (connection[SRC_PORT] == package_vector[5] and connection[DST_PORT] == package_vector[6]) or (
                            connection[SRC_PORT] == package_vector[6] and connection[DST_PORT] == package_vector[5]):
                if (connection[PROTOCOL] == package_vector[2]):
                    return connection, index

    return None, -1


def update_active_connections(package_vector):
    global active_connections
    total_active_connections = active_connections.shape[0]

    trace, idx = get_active_connection(package_vector)

    if trace is None:
        # if len(active_connections) > 1000:
        #     fu.write_to_file(active_connections[0], OUTPUT_FILE_PATH)
        #     active_connections = np.delete(active_connections, 0, axis=0)

        trace = generate_new_connection(package_vector)
        active_connections = np.vstack((active_connections, trace))
    else:
        active_connections = np.delete(active_connections, idx, axis=0)
        trace = update_trace(trace, package_vector)
        active_connections = np.vstack((active_connections,
                                        trace))

    if int(trace[AUX_VALID_TRACE]) == 1:
        if int(trace[PROTOCOL]) == 6:
            tcp_parsed_trace = np.zeros([23])
            return get_tcp_parsed_trace(trace, tcp_parsed_trace, total_active_connections)

        if int(trace[PROTOCOL]) == 17:
            udp_parsed_trace = np.zeros([19])
            return get_udp_parsed_trace(trace, udp_parsed_trace, total_active_connections)

    else:
        return None, trace, False, total_active_connections

def get_tcp_parsed_trace(trace, parsed_trace, total_active_connections):
    for i in range(23):
        parsed_trace[i] = float(trace[i + 6])
    return parsed_trace, trace, int(float(trace[4])), total_active_connections

def get_udp_parsed_trace(trace, parsed_trace, total_active_connections):
    for i in range(7):
        parsed_trace[i] = float(trace[i + 6])
    for i in range(4):
        parsed_trace[i + 7] = float(trace[i + 15])
    for i in range(8):
        parsed_trace[i + 11] = float(trace[i + 21])
    return parsed_trace, trace, int(float(trace[4])), total_active_connections

def update_dest_trace(trace, vector):
    if int(trace[DST_TTL]) == 0:
        trace[DST_TTL] = int(vector[P_TTL])
    if int(trace[DST_TTL]) > int(vector[P_TTL]):
        trace[DST_TTL] = int(vector[P_TTL])

    trace[DST_PACKAGES] = float(trace[DST_PACKAGES]) + 1
    trace[DST_BYTES] = float(trace[DST_BYTES]) + float(vector[P_SIZE])
    trace[DST_MEAN] = int(round(float(trace[DST_BYTES]) / float(trace[DST_PACKAGES])))


    if trace[DST_TCPB] == "" and vector[P_SEQ_NUMBER] != "":
        trace[DST_TCPB] = int(vector[P_SEQ_NUMBER])

    return trace


def update_source_trace(trace, vector):
    if int(trace[SRC_TTL]) > int(vector[P_TTL]):
        trace[SRC_TTL] = int(vector[P_TTL])

    trace[SRC_PACKAGES] = float(trace[SRC_PACKAGES]) + 1
    trace[SRC_BYTES] = float(trace[SRC_BYTES]) + float(vector[P_SIZE])
    trace[SRC_MEAN] = int(round(float(trace[SRC_BYTES]) / float(trace[SRC_PACKAGES])))
    trace = update_ct_ltm(trace, vector, 1)

    return trace


def update_trace(trace, vector):
    updated_trace = trace

    updated_trace[DURATION] = float(vector[P_EPOCH_TIME]) - float(updated_trace[AUX_EPOCH_START])
    updated_trace[EPOCH_END] = int(round(float(vector[P_EPOCH_TIME])))

    if trace[SRC_IP] == vector[P_SRC_IP]:
        updated_trace = update_source_trace(updated_trace, vector)
    else:
        updated_trace = update_dest_trace(updated_trace, vector)

    if int(trace[PROTOCOL]) == 6:
        updated_trace = update_tcp_trace(updated_trace, vector)
    elif int(trace[PROTOCOL]) == 17:
        updated_trace = update_udp_trace(updated_trace, vector)

    if int(updated_trace[AUX_VALID_TRACE]) == 0:
        updated_trace = update_valid(updated_trace)

    return updated_trace


def update_tcp_trace(trace, vector):
    if int(trace[AUX_RTT_FLAG]) < 2:
        trace = tcp_round_trip_time(trace, vector)

    return trace


def update_udp_trace(trace, vector):
    if int(trace[AUX_RTT_FLAG]) < 2:
        trace[SYN_ACK] = 0
        trace[AUX_RTT_FLAG] = 2
    return trace


def tcp_round_trip_time(trace, vector):
    if int(vector[P_TCP_FLAGS]) == 2:  # SYN
        if int(trace[AUX_RTT_FLAG]) == 0:  # Waiting for SYN_ACK, SYN reset
            trace[SYN_ACK] = float(vector[P_EPOCH_TIME])
    elif int(vector[P_TCP_FLAGS]) == 18:  # SYN_ACK
        if int(trace[AUX_RTT_FLAG]) == 0:  # Waiting for SYN_ACK
            trace[AUX_RTT_FLAG] = 1
            trace[SYN_ACK] = float(vector[P_EPOCH_TIME]) - float(trace[SYN_ACK])
            trace[ACK_DAT] = float(vector[P_EPOCH_TIME])
        elif int(trace[AUX_RTT_FLAG]) == 1:  # Waiting for ACK, SYN_ACK reset
            trace[ACK_DAT] = float(vector[P_EPOCH_TIME])
    elif int(vector[P_TCP_FLAGS]) == 16:  # ACK
        if int(trace[AUX_RTT_FLAG]) == 1:  # Waiting for ACK
            trace[AUX_RTT_FLAG] = 2
            trace[ACK_DAT] = float(vector[P_EPOCH_TIME]) - float(trace[ACK_DAT])
    return trace


def is_sm_ip_ports(vector):
    if vector[P_SRC_IP] == vector[P_DST_IP] and vector[P_SRC_PORT] == vector[P_DST_PORT]:
        return 1
    else:
        return 0


def update_ct_ltm(trace, vector, mode):
    global active_connections
    n = len(active_connections)
    ct_srv_src, ct_srv_dst, ct_dst_ltm, ct_src_ltm, ct_src_dst_port_ltm, ct_dst_src_port_ltm, ct_dst_src_ltm = 0, 0, 0, 0, 0, 0, 0

    if n >= 1:
        if n < 100:
            for i in range(0, n, 1):
                ct_srv_src, ct_srv_dst, ct_dst_ltm, ct_src_ltm, ct_src_dst_port_ltm, ct_dst_src_port_ltm, ct_dst_src_ltm = ct_count(
                    ct_srv_src, ct_srv_dst, ct_dst_ltm, ct_src_ltm, ct_src_dst_port_ltm, ct_dst_src_port_ltm,
                    ct_dst_src_ltm, active_connections[i], vector)
        else:
            for i in range(n - 100, n, 1):
                ct_srv_src, ct_srv_dst, ct_dst_ltm, ct_src_ltm, ct_src_dst_port_ltm, ct_dst_src_port_ltm, ct_dst_src_ltm = ct_count(
                    ct_srv_src, ct_srv_dst, ct_dst_ltm, ct_src_ltm, ct_src_dst_port_ltm, ct_dst_src_port_ltm,
                    ct_dst_src_ltm, active_connections[i], vector)

    if mode == 0:
        return ct_srv_src, ct_srv_dst, ct_dst_ltm, ct_src_ltm, ct_src_dst_port_ltm, ct_dst_src_port_ltm, ct_dst_src_ltm

    else:
        trace[CT_SRV_SRC] = ct_srv_src
        trace[CT_SRV_DST] = ct_srv_dst
        trace[CT_DST_LTM] = ct_dst_ltm
        trace[CT_SRC_LTM] = ct_src_ltm
        trace[CT_SRC_DST_PORT_LTM] = ct_src_dst_port_ltm
        trace[CT_DST_SRC_PORT_LTM] = ct_dst_src_port_ltm
        trace[CT_DST_SRC_LTM] = ct_dst_src_ltm
        return trace


def ct_count(ct_srv_src, ct_srv_dst, ct_dst_ltm, ct_src_ltm, ct_src_dst_port_ltm, ct_dst_src_port_ltm, ct_dst_src_ltm,
             trace, vector):
    if trace[SRC_IP] == vector[P_SRC_IP]:
        ct_src_ltm += 1
        if trace[SERVICE] == vector[P_SERVICE]:
            ct_srv_src += 1
        if trace[DST_PORT] == vector[P_DST_PORT]:
            ct_src_dst_port_ltm += 1
        if trace[DST_IP] == vector[P_DST_IP]:
            ct_dst_src_ltm += 1

    if trace[DST_IP] == vector[P_DST_IP]:
        ct_dst_ltm += 1
        if trace[SERVICE] == vector[P_SERVICE]:
            ct_srv_dst += 1
        if trace[SRC_PORT] == vector[P_SRC_PORT]:
            ct_dst_src_port_ltm += 1

    return ct_srv_src, ct_srv_dst, ct_dst_ltm, ct_src_ltm, ct_src_dst_port_ltm, ct_dst_src_port_ltm, ct_dst_src_ltm


def update_valid(trace):  # TODO: Añadir condiciones para clasificar Rober
    if int(trace[AUX_RTT_FLAG]) == 2:
        trace[AUX_VALID_TRACE] = 1
    return trace


def print_active_connections():
    print("############################")
    print(active_connections)


def get_active_connections():
    return active_connections


consumer = KafkaConsumer('sniffer',
                         bootstrap_servers=['10.40.39.22:1025'])

producer = KafkaProducer(bootstrap_servers='10.40.39.22:1025')

print('Started Consumer')
for message in consumer:
    package_info = message.value.decode('utf-8').replace("[", "").replace("]", "").replace('"',"").replace("'","").split(' ')
    parsed_trace, trace, protocol, total_active_connections = update_active_connections(package_info)
    if parsed_trace is not None:
        new_parsed_trace=np.array([])
        for i in range(len(parsed_trace)):
            new_parsed_trace=np.append(new_parsed_trace, str(parsed_trace[i]))
        new_parsed_trace=np.append(new_parsed_trace, str(protocol))
        producer.send('connections', np.array_str(new_parsed_trace).encode())
        print(new_parsed_trace)


