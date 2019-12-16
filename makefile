CHORDPEER=tracker/dht/handler.py
DEPENDENCIES=zmq cloudpickle pyinstaller python3-pyqt5
TRACKERLAUNCHER=tracker/launcher.py
TRACKERLAUNCHERARGS=localhost:8000, localhost:8002, localhost:8005
CLIENTDIR=client/
CLIENTEXE=client/test.py
PYTHONDEPS=python3-dev python-dev
PYTHON=python3

.PHONY: test
test:
	$(PYTHON) $(CHORDPEER) -i localhost -p 8000 &
	sleep(1)
	$(PYTHON) $(CHORDPEER) -i localhost -p 8001 -t "localhost:8000" &
	sleep(1)
	$(PYTHON) $(CHORDPEER) -i localhost -p 8002 -t "localhost:8000" &
	sleep(1)
	$(PYTHON) $(CHORDPEER) -i localhost -p 8003 -t "localhost:8002" &
	sleep(1)
	$(PYTHON) $(CHORDPEER) -i localhost -p 8004 -t "localhost:8001" &
	sleep(1)
	$(PYTHON) $(CHORDPEER) -i localhost -p 8005 -t "localhost:8001" &
	sleep(1)
	$(PYTHON) $(TRACKERLAUNCHER) -i localhost -p 8080 -t $(TRACKERLAUNCHERARGS) &
	$(PYTHON) $(TRACKERLAUNCHER) -i localhost -p 8888 -t $(TRACKERLAUNCHERARGS) &
	cd $(CLIENTDIR)
	$(PYTHON) test.py &
	cd ..

.PHONY: clean
clean:
	rm -r tracker/__pycache__;
	rm -r client/__pycache__;
	rm -r tracker/dht/__pycache__;
	rm -r build
	rm -r dist

.PHONY: install
install:
	pip install $(DEPENDENCIES)
	sudo apt-get install $(PYTHONDEPS)
	# Make a executable app
	pyinstaller --onefile $(CLIENTEXE)