import time
import ByteBuffer as bb_buf
import psycopg2
from psycopg2.extras import execute_values
import queue
import threading
import logging
import colorlog

logger = logging.getLogger()
logger.setLevel(logging.INFO)

color_formatter = colorlog.ColoredFormatter(
    "%(log_color)s%(asctime)s - %(levelname)s - %(module)s - %(lineno)d - %(message)s",
    log_colors={
        'DEBUG': 'reset',
        'INFO': 'green',
        'WARNING': 'yellow',
        'ERROR': 'red',
        'CRITICAL': 'bold_red',
    },
    reset=True,
    style='%'
)
console_handler = logging.StreamHandler()
console_handler.setFormatter(color_formatter)
logger.addHandler(console_handler)



data_to_insert = queue.Queue()

connection = psycopg2.connect(
    dbname="postgres",
    user="postgres",
    password="12345",
    host="localhost",
    port="5432"
)
cursor = connection.cursor()


def getPorfileType(pro_type):

    if pro_type == 1:
        return "Instant_Profile"
    if pro_type == 2:
        return "Load_Profile"
    if pro_type == 3:
        return "Billing_Profile"
    if pro_type == 4:
        return "Event_Profile"
    if pro_type == 5:
        return "Midnight_Profile"

def log_viwer(hesstr, nodeid):
    data = bb_buf.GXByteBuffer(hesstr)
    log_response = {}
    # print(hesstr)
    log_response = {}
    while ((data.getSize()) != (data.getPosition())):
        # print("size:", str(data.getSize()), "position:" + str(data.getPosition()))
        ident = data.getInt8()
        # print(ident)

        if ident == 0x7e:
            log_response['node_id'] = nodeid

            try:
                command_id = data.getInt32()
                log_response['command_id'] = command_id
            except ValueError as e:
                logging.error(f"Error reading command_id: {e}")

        if ident == 0x02:
            profile_type = data.getInt8()
            log_response['profile'] = getPorfileType(profile_type)
        if ident == 0x03:
            profile_status = data.getInt8()
            log_response['status'] = profile_status
        if ident == 0x09:
            NICtodcu = data.getInt8()
            log_response['NiC_Signal'] = NICtodcu

        if ident == 0x08:
            try :
                success_packet_number = data.getInt8()
                log_response['success_packet'] = success_packet_number
            except Exception as error:
                logging.error(f'error reading success packet {error}')

            if ident == 0x0a:
                epoch_sec = data.getInt32()
                # print(epoch_sec)

                if epoch_sec >= 0:
                    epoch_sec %= (24 * 60 * 60)

                    try:
                        local_time = time.ctime(epoch_sec - (18000 + (30 * 60)))
                        log_response['TOR'] = local_time
                    except OSError:
                        logging.error(f"Error calculating local_time for epoch_sec: {epoch_sec}")
                        # log_response['TOR'] = "Invalid Local Time"


                else:
                    print("Invalid epoch_sec: {epoch_sec}")



        if ident == 0x7b:
            # log_response['node_id'] = nodeid
            # command_id = data.getInt32()
            # print(command_id)
            # log_response['dcu_constant_command'] = command_id

            try:
                command_id = data.getInt32()
                log_response['dcu_constant_command'] = command_id
            except ValueError as e:
                logging.error(f"Error reading command_id: {e}")


        if ident == 0x0b:
            if int(nodeid) >= 429000:
                packet_sent_time = data.getInt16()
            else:
                packet_sent_time = data.getInt32()
            log_response['packet_send_time'] = packet_sent_time
            # print(log_response)

            data_to_insert.put(log_response)

def get_data():
    query = """select node_id, hex from 
                rf_log r 
                  order by node_id, server_time"""

    print("executing")
    cursor.execute(query)
    result = cursor.fetchall()
    print("executed")
    print("calculating")
    # tmp_hex = "7e00004748020103000a648c195d090108060b00067e0000475e020103000a648c1cf3090108060b00067e00004774020103000a648c208a090108060b000600000000000000000000000000000000000000000000000000000000000000000000000000"
    # log_viwer(tmp_hex,123)
    for row in result:
        node_id, hex = row
        log_viwer(hex, node_id)


def insert_data():
    insert_query = """
            INSERT INTO rf_log_parsed
            (node_id, command_id, profile, status, tor, nic_signal, success_packet, packet_send_time,dcu_constant_command)
            VALUES %s
        """

    batch_size = 200

    batch_no = 1
    while True:
        batch = []

        while len(batch) < batch_size:
            try:
                item = data_to_insert.get_nowait()
                batch.append(item)
            except queue.Empty:
                break

        if batch:
            print(f'Inserting batch {batch_no} with {len(batch)} items ')

            # values = [(item['node_id'], item['command_id'], item['profile'], item['status'],
            #            item['TOR'], item['NiC_Signal'], item['success_packet'], item['packet_send_time'])
            #           for item in batch]

            values = []

            for item in batch:
                values.append((
                    item.get('node_id', None),
                    item.get('command_id', None),
                    item.get('profile', None),
                    item.get('status', None),
                    item.get('TOR', None),
                    item.get('NiC_Signal', None),
                    item.get('success_packet', None),
                    item.get('packet_send_time', None),
                    item.get('dcu_constant_command', None)

                ))

            execute_values(cursor, insert_query, values)
            connection.commit()
            batch_no += 1
        else:
            print("Queue is empty, waiting for more data...")
            time.sleep(1)



if __name__ == '__main__':

    insert_thread = threading.Thread(target=insert_data)

    # try:

    insert_thread.start()
    get_data()
    # waiting for 5 seconds for the get data function to add items to the qeue
    # time.sleep(1)

    insert_thread.join()
    # except Exception as error:
    #     print(error)
