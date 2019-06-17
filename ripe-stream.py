#!/usr/bin/env python3
""" Subscribe to the RIPE RIS-Live stream and process it """

## (c) 2019 Jared Mauch

import json
import time
import radix
import sys
import websocket

## Uncomment and update this with a unique identifier for the RIPE team
#url = "wss://ris-live.ripe.net/v1/ws/?client=username_at_example_com"

tree = radix.Radix()

## File format : CIDR,METADATA,METADATA2
prefix_file = "/path/to/file/prefixes.csv"

# load the prefixes to memory
with open(prefix_file) as fp:
    line = fp.readline()
    while line:
        ## parse line
        parse_me = str(line.rstrip())
        (prefix, metadata, metadata2) = parse_me.split(',', 3)

        rnode = tree.add(prefix)
        rnode.data['metadata'] = metadata
        rnode.data['metadata2'] = metadata2

        ## fetch another line
        line = fp.readline()

while True:
    ws = websocket.WebSocket()
    ws.connect(url)
    # subscribe to all BGP Updates
    ws.send(json.dumps({"type": "ris_subscribe", "data": {"type": "UPDATE"}}))
    try:
        for data in ws:
            parsed = json.loads(data)
            if parsed.get('type', None) == 'ris_error':
                print(data)
            if parsed.get('type', None) == 'ris_message':
                # print(parsed["type"], parsed["data"])
                parsed_data = parsed.get("data", None)
                announcements = parsed_data.get('announcements', None)
                withdrawls = parsed_data.get('withdrawls', None)
                try:
                    as_path = ' '.join(str(x) for x in parsed_data.get('path', None))
                except:
                    as_path = ''
                if announcements is not None:
                    for announcement in announcements:
                        for prefix in announcement['prefixes']:
                            try:
                                rnode = tree.search_best(prefix)
                            except Exception as e:
                                print("search_best: %s returned " % prefix, e)
                            if rnode is not None and rnode.data['metadata'] is not None:
                                # Is it a more specific?
                                if prefix != rnode.prefix:
                                    sub_prefix = "Yes"
                                else:
                                    sub_prefix = ""
                                print("add|%s|%s|%s|%s|%s|%s" % (prefix, sub_prefix, rnode.prefix,
                                      rnode.data['metadata'], rnode.data['metadata2'], as_path))
                if withdrawls is not None:
                    for announcement in withdrawls:
                        for prefix in announcement['prefixes']:
                            try:
                                rnode = tree.search_best(prefix)
                            except Exception as e:
                                print("search_best: %s returned " % prefix, e)
                            if rnode is not None and rnode.data['metadata'] is not None:
                                # Is it a more specific?
                                if prefix != rnode.prefix:
                                    sub_prefix = "Yes"
                                else:
                                    sub_prefix = ""
                                print("del|%s|%s|%s|%s|%s|%s" % (prefix, sub_prefix, rnode.prefix,
                                      rnode.data['metadata'], rnode.data['metadata2'], as_path))

    except websocket.WebSocketConnectionClosedException as e:
        print("Disconnected, sleeping for a few then reconnect", e)
        time.sleep(30)
    except ConnectionResetError as e:
        print("Disconnected, sleeping for a few then reconnect", e)
        time.sleep(30)
    except BrokenPipeError as e:
        print("Disconnected, sleeping for a few then reconnect", e)
        time.sleep(30)
    except websocket.WebSocketBadStatusException as e:
        print("Disconnected, sleeping for a few then reconnect", e)
        time.sleep(30)
    except websocket.WebSocketTimeoutException as e:
        print("Disconnected, sleeping for a few then reconnect", e)
        time.sleep(30)
    except KeyboardInterrupt:
        print("User stop requested")
        sys.exit()
    except Exception as e:
        print("some other error?", e)
        time.sleep(30)

##
## ris_message
### {'timestamp': 1550258410.78,
###  'peer': '217.29.66.88',
###  'peer_asn': '20811',
###  'id': '217.29.66.88-1550258410.78-59479614',
###  'host': 'rrc10',
###  'type': 'UPDATE',
###  'path': [20811, 8529, 9155, 51914, 51914, 51914, 51914],
###  'origin': 'igp',
###  'announcements':
###    [{'next_hop': '217.29.67.63', 'prefixes': ['91.221.128.0/24']}  ]
### }
## ris_message
### {'timestamp': 1550258410.78,
###  'peer': '217.29.66.88',
###  'peer_asn': '20811',
###  'id': '217.29.66.88-1550258410.78-59479616',
###  'host': 'rrc10',
###  'type': 'UPDATE',
###  'path': [20811, 8529, 49666, 42440, 205647, 44400, 47843],
###  'origin': 'igp',
###  'announcements': [
####   {'next_hop': '217.29.67.63',
####    'prefixes':
#####     ['87.248.144.0/24',
#####      '87.248.150.0/24',
#####      '87.248.139.0/24',
#####      '87.248.153.0/24',
#####      '87.248.149.0/24',
#####      '87.248.145.0/24',
#####      '87.248.152.0/24',
#####      '87.248.151.0/24',
#####      '87.248.138.0/24',
#####      '87.248.133.0/24',
#####      '87.248.147.0/24',
#####      '87.248.155.0/24',
#####      '87.248.131.0/24',
#####      '87.248.132.0/24',
#####      '87.248.136.0/24',
#####      '87.248.154.0/24',
#####      '87.248.156.0/24',
#####      '87.248.158.0/24',
#####      '87.248.134.0/24',
#####      '87.248.135.0/24',
#####      '87.248.129.0/24',
#####      '87.248.130.0/24',
#####      '87.248.146.0/24',
#####      '87.248.128.0/24',
#####      '87.248.137.0/24']
####  }
###  ]
### }
