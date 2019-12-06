CHORDPEER=tracker/dht/handler.py
DEPENDENCIES=zmq cloudpickle pyinstaller
TRACKERLAUNCHER=tracker/launcher.py
TRACKERLAUNCHERARGS=localhost:8000, localhost:8002, localhost:8005
CLIENTDIR=client/
CLIENTEXE=test.py

.PHONY: test
test:
	python $(CHORDPEER) -i localhost -p 8000 &
	sleep(1)
	python $(CHORDPEER) -i localhost -p 8001 -t "localhost:8000" &
	sleep(1)
	python $(CHORDPEER) -i localhost -p 8002 -t "localhost:8000" &
	sleep(1)
	python $(CHORDPEER) -i localhost -p 8003 -t "localhost:8002" &
	sleep(1)
	python $(CHORDPEER) -i localhost -p 8004 -t "localhost:8001" &
	sleep(1)
	python $(CHORDPEER) -i localhost -p 8005 -t "localhost:8001" &
	sleep(1)
	python $(TRACKERLAUNCHER) -i localhost -p 8080 -t $(TRACKERLAUNCHERARGS) &
	python $(TRACKERLAUNCHER) -i localhost -p 8888 -t $(TRACKERLAUNCHERARGS) &
	cd $(CLIENTDIR)
	python test.py &
	cd ..

.PHONY: clean
clean:
	rm -r tracker/__pycache__ tracker/*.pyc;
	rm -r tracker/dht/__pycache__ tracker/dht/*.pyc;

.PHONY: install
install:
	pip install $(DEPENDENCIES)
	# Make a executable app
	cd $(CLIENTDIR)
	pyinstaller --onefile $(CLIENTEXE)