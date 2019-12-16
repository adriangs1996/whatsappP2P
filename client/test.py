import regpopup
import uitest
import sys
from PyQt5.QtWidgets import *

def main():
	ip = None
	port = 0

	if len(sys.argv) > 1:
		ip = sys.argv[1]
		if len(sys.argv) > 2:
			port = int(sys.argv[2])

	app = QApplication(sys.argv)
	screen = uitest.Ui_MainWindow(ip, port)
	screen.show()
	sys.exit(app.exec_())

if __name__ == '__main__':
	main()