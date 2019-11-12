import tracker

server = tracker.ClientInformationTracker('testdb.db', '*', 8888)

server.serve_requests()
