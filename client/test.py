import regpopup
import uitest
import sys
from PyQt5.QtWidgets import *

app = QApplication(sys.argv)

# screen = regpopup.Ui_Dialog()
# screen.setModal(True)
screen = uitest.Ui_MainWindow()
screen.show()
sys.exit(app.exec_())
