import SocketServer
from json import loads, dumps

class KatanaHandler(SocketServer.StreamRequestHandler):
    def handle(self):
        try:
            try:
                data = loads(self.rfile.readline().strip())
                method = getattr(self.server.katana, data['action'])
            except (ValueError, AttributeError, KeyError):
                raise Exception("Invalid operation")

            result = {
                'status': 'ok',
                'result': method(
                    *data.get('args', []),
                    **data.get('kwargs', {})
                )
            }

        except Exception, e:
            result = {
                'status': 'error',
                'message': unicode(e)
            }
        self.request.sendall(dumps(result))
