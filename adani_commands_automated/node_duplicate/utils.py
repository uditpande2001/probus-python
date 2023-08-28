from struct import unpack
from enum import Enum
from more_itertools import sliced
import binascii

#################################################################################
# CONSTANTS
#################################################################################

RAPI_SRC_ENDPOINT = 255
RAPI_DEST_ENDPOINT = 240

#################################################################################
# DECODING REMOTE API UTILS
#################################################################################

class ActionType(Enum):
    BLANK = 0
    PRESENT =1
    PROCESS = 2

    @classmethod
    def get(cls, value):
        return cls(value).name
    
class ScratchpadStatus(Enum):
    NEW = 255
    SUCCESS = 0
    ERROR = 256

    @classmethod
    def get(cls, value):
        if value > 0 and value < 255:
            return cls(256).name
        else: 
            return cls(value).name

def get_node_address_change_status_from_csap_response(payload):
    begin_resp = unpack('<B', payload[0:1])[0]
    if begin_resp == 0x81:
        scratchpad_update_resp = unpack('<B', payload[2:3])[0]
        if scratchpad_update_resp == 0x8D:
            return "Node address change instruction successfully received by the node. Response: " + binascii.hexlify(payload).decode()
        else:
            return "Node address change instruction failed: " + binascii.hexlify(payload).decode()

def get_activate_status_from_msap_response(payload):
    begin_resp = unpack('<B', payload[0:1])[0]
    if begin_resp == 0x81:
        scratchpad_update_resp = unpack('<B', payload[2:3])[0]
        if scratchpad_update_resp == 0xFD:
            return "Node has no scratchpad or a scratchpad with a different sequence number"
        elif scratchpad_update_resp == 0x9A:
            return "Scratchpad found"
    else:
        return "Could not determine if activate was successful"

def decode_msap_scratchpad_status(status_payload, filter=None):
    # We are only interested by scratchpad remote status
    response_type = unpack('<B', status_payload[0:1])[0]
    if response_type != 0x99:
        print("Receiving remote api that is not scratchpad status (0x: %x)" % response_type)
        return

    # Define wich version of status we received based on size
    size = unpack('<B', status_payload[1:2])[0]
    if size == 39:
        version = 2
    elif size == 24:
        version = 1
    elif size == 47:
        version = 3
    else:
        print("Error, wrong size (%d) for Remote api response status" % size)
        return

    # Parse common part for all versions
    # Get Scratchpad present
    length, crc, seq, type, status = unpack('<IHBBB', status_payload[2:11])

    # Get Scratchpad that produce firmware
    scr_firm_length, scr_firm_crc, scr_firm_seq = unpack('<IHB', status_payload[11:18])

    # Get firmware area id
    firm_area_id = unpack('<I', status_payload[18:22])[0]

    stack_version = unpack('<BBBB', status_payload[22:26])

    new_status = {
        "seq": seq,
        "crc": crc,
        "length": length,
        "type": ActionType.get(type),
        "status": ScratchpadStatus.get(status),
        "stack_scr_len": scr_firm_length,
        "stack_scr_crc": scr_firm_crc,
        "stack_scr_seq": scr_firm_seq,
        "stack_version": stack_version,
        "stack_area_id": firm_area_id}

    # Load info included after version 1
    if version >= 2:
        new_status["app_version"] = unpack('<BBBB', status_payload[37:41])
        # Get Scratchpad that produce app
        scr_app_length, scr_app_crc, scr_app_seq = unpack('<IHB', status_payload[26:33])

        new_status["app_scr_len"] = scr_app_length
        new_status["app_scr_crc"] = scr_app_crc
        new_status["app_scr_seq"] = scr_app_seq

        # Get firmware area id
        new_status["app_area_id"] = unpack('<I', status_payload[33:37])[0]

    # Load info included after version 2
    if version >= 3:
        action, target_seq, target_crc, target_delay, remaining_delay = unpack('<BBHHH', status_payload[41:49])
        new_status["action"] = action
        new_status["target_crc"] = target_crc
        new_status["target_seq"] = target_seq
        new_status["target_delay_m"] = target_delay
        new_status["remaining_delay_m"] = remaining_delay

    if filter is not None:
        new_status = {k:v for (k,v) in new_status.items() if k in filter}

    return new_status

#################################################################################
# ENCODING REMOTE API COMMANDS UTILS
#################################################################################

def convert_seq_num(seq_num):
    return "{:02x}".format(seq_num)

def convert_node_address(node_address):
    hex_node_address_str = ''.join(list(sliced("{:08x}".format(int(node_address)), 2)))
    return binascii.hexlify(bytes(bytearray.fromhex(hex_node_address_str)[::-1])).decode()

def convert_activation_delay(activation_delay):
    return ''.join(list(sliced("{:04x}".format(activation_delay), 2))[::-1])
    
def build_otap_activation_command(seq_num, activation_delay):
    return "01001A01" + convert_seq_num(seq_num) + "0300" + add_activation_delay(activation_delay)

def add_activation_delay(activation_delay):
    return "0502" + convert_activation_delay(activation_delay)

def build_change_node_address_command(node_address, activation_delay):
    return "01000D060100" + convert_node_address(node_address) + "0300" + add_activation_delay(activation_delay)

#################################################################################
# ARG PARSER UTILS
#################################################################################

def process_gateway_list(gateway_list_as_string):
    return gateway_list_as_string.split(",")