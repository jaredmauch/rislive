#!/usr/bin/env python3
""" Subscribe to the RIPE RIS-Live stream and process it """

## (c) 2019 Jared Mauch

import json
import time
import websocket
import sys

## Uncomment and update this with a unique identifier for the RIPE team
#url = "wss://ris-live.ripe.net/v1/ws/?client=username_at_example_com"

asn = '20940'

# store start time
last_periodic = time.time()
periodic_interval = 90 # 90 seconds

noisy_prefix = {}
noisy_aspath = {}
while True:
    ws = websocket.WebSocket()
    try:
        ws.connect(url)
    except websocket.WebSocketBadStatusException as e:
        print(e, "while calling connect()")
        time.sleep(30)
        continue
       
    # subscribe to all BGP Updates
    ws.send(json.dumps({"type": "ris_subscribe", "data": {"type": "UPDATE", "path": asn}}))
    try:
        for data in ws:
            # get current time
            now = time.time()
            if now - last_periodic > periodic_interval:
                print("============ periodic ============")
                # 
                count = 0
                for k ,v in sorted(noisy_prefix.items(), key=lambda kv:(kv[1], kv[0]), reverse=True):
                    if count > 10:
                        break
                    if noisy_prefix[k] > 100:
                        print(k, noisy_prefix[k])
                    count = count + 1
                count = 0
                for k, v in sorted(noisy_aspath.items(), key=lambda kv:(kv[1], kv[0]), reverse=True):
                    if count > 10:
                        break
                    if noisy_aspath[k] > 100:
                        print(k, noisy_aspath[k])
                    count = count + 1

                noisy_prefix = {}
                noisy_aspath = {}
                last_periodic = now
                print("============ periodic ============")
            #

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
#                            print("add|%s|%s" % (prefix, as_path))
                            noisy_prefix[prefix] = noisy_prefix.get(prefix, 0) + 1
                            noisy_aspath[as_path] = noisy_aspath.get(as_path, 0) + 1

                if withdrawls is not None:
                    for announcement in withdrawls:
                        for prefix in announcement['prefixes']:
#                            print("del|%s|%s" % (prefix, as_path))
                            noisy_prefix[prefix] = noisy_prefix.get(prefix, 0) + 1
                            noisy_aspath[as_path] = noisy_aspath.get(as_path, 0) + 1

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
