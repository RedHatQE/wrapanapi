import base64
import json
import websocket


class WebsocketClient(object):
    def __init__(self, url, username=None, password=None, headers={}, enable_trace=False,
                 timeout=60):
        """Simple Web socket client for wrapanapi

       Args:
           url: String with the hostname or IP address of the server with port
            (e.g. 'ws://10.11.12.13:8080/hawkular/command-gateway/ui/ws')
           username: username for basic auth (optional)
           password: password for basic auth
           headers: When you want to pass specified header use this
           enable_trace: Enable trace on web socket client
           timeout: receive timeout in seconds
       """
        self.url = url
        self.username = username
        self.password = password
        self.headers = headers
        self.enable_trace = enable_trace
        self.timeout = timeout
        self.ws = None

    def connect(self):
        """connects with the initialized detail"""
        if self.ws and self.ws.connected:
            return self.ws
        else:
            websocket.enableTrace(self.enable_trace)
            if self.username:
                base64_creds = base64.b64encode("{}:{}".format(self.username, self.password))
                self.headers.update({"Authorization": "Basic {}".format(base64_creds)})
            self.ws = websocket.create_connection(self.url, header=self.headers)
            self.ws.settimeout(self.timeout)

    @property
    def connected(self):
        """Returns True if client connected with server"""
        if self.ws and self.ws.connected:
            return True
        return False

    def close(self):
        """Close the current connection"""
        if self.ws and self.ws.connected:
            self.ws.close()

    def _check_connection(self, make_connection=True):
        """Check client connected with server

        Args:
            make_connection: by default True. Creates connection if not connected
        """
        if self.connected:
            return
        elif make_connection:
            self.connect()
            if self.connected:
                return
        raise RuntimeError("WS client not connected!")

    def send(self, payload, binary_stream=False):
        """Send payload to server
        Args:
            payload: payload, string or binary
            binary_stream: dafault False. Set this True when you send binary payload
        """
        self._check_connection()
        if binary_stream:
            self.ws.send_binary(payload=payload)
        else:
            self.ws.send(payload=payload)

    def receive(self):
        """Returns available message on received queue. If there is no message.
         waits till timeout"""
        self._check_connection()
        return self.ws.recv()


class HawkularWebsocketClient(WebsocketClient):
    """This client extended from normal websocket client. designed to hawkular specific"""
    def __init__(self, url, username=None, password=None, headers={}, enable_trace=False,
                 timeout=60):
        """Creates hawkular web socket client. for arguments refer 'WebsocketClient'"""
        super(HawkularWebsocketClient, self).__init__(url=url, username=username, password=password,
                                                      headers=headers, enable_trace=enable_trace,
                                                      timeout=timeout)
        self.session_id = None

    def connect(self):
        """Create connection with hawkular web socket server"""
        super(HawkularWebsocketClient, self).connect()
        response = self.hwk_receive()
        if 'WelcomeResponse' in response:
            self.session_id = response['WelcomeResponse']['sessionId']
            return response['WelcomeResponse']
        else:
            raise RuntimeWarning("Key 'WelcomeResponse' not found on response: {}".format(response))
            return response

    def hwk_receive(self):
        """parse recevied message and returns as dictionary value"""
        payload = self.receive()
        data = payload.split('=', 1)
        if len(data) != 2:
            raise IndentationError("Unknown payload format! {}".format(payload))
        response = {data[0]: json.loads(data[1])}
        if 'GenericErrorResponse' in response:
            raise Exception("Hawkular server sent failure message: {}"
                            .format(response['GenericErrorResponse']))
        return response

    def hwk_invoke_operation(self, payload, operation_name="ExecuteOperation", binary_content=None,
                             binary_file_location=None, wait_for_response=True):
        """Runs hawkular specific operations
        Args:
            payload: payload to server. only string
            operation_name: requested operation. default: ExecuteOperation
            binary_content: binary content
            binary_file_location: binary content file name. Will be changed as binary content
            wait_for_response: When executing a command, wait for the response. default: True

        """
        _payload = "{}Request={}".format(operation_name, json.dumps(payload))
        if binary_file_location:
            binary_content = open(binary_file_location, 'rb').read()
        if binary_content:
            self.send(_payload + binary_content, binary_stream=True)
        else:
            self.send(_payload, binary_stream=False)
        if wait_for_response:
            responses = []
            response = self.hwk_receive()
            responses.append(response)
            if "GenericSuccessResponse" in response:
                responses.append(self.hwk_receive())
            return responses
