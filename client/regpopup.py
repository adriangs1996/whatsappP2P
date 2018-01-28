# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file 'regpopup.ui'
#
# Created by: PyQt5 UI code generator 5.10.1
#
# WARNING! All changes made in this file will be lost!

from client import Client
from PyQt5 import QtCore, QtGui, QtWidgets

class Ui_Dialog(QtWidgets.QDialog):
    def __init__(self, Client):
        QtWidgets.QWidget.__init__(self)
        #self.diag = QtWidgets.QDialog()
        self.setupUi()
        self.client = Client

    # def show(self):
    #     self.diag.show()


    def setupUi(self):
        self.resize(321, 228)
        self.setModal(True)
        self.setObjectName("Dialog")
        self.label_3 = QtWidgets.QLabel(self)
        self.label_3.setGeometry(QtCore.QRect(40, 30, 261, 41))
        font = QtGui.QFont()
        font.setPointSize(14)
        self.label_3.setFont(font)
        self.label_3.setObjectName("label_3")
        self.label = QtWidgets.QLabel(self)
        self.label.setGeometry(QtCore.QRect(40, 100, 69, 17))
        self.label.setObjectName("label")
        self.lineEdit = QtWidgets.QLineEdit(self)
        self.lineEdit.setGeometry(QtCore.QRect(40, 120, 241, 25))
        self.lineEdit.setObjectName("lineEdit")
        self.buttonBox = QtWidgets.QDialogButtonBox(self)
        self.buttonBox.setGeometry(QtCore.QRect(140, 190, 166, 25))
        self.buttonBox.setOrientation(QtCore.Qt.Horizontal)
        self.buttonBox.setStandardButtons(QtWidgets.QDialogButtonBox.Cancel|QtWidgets.QDialogButtonBox.Ok)
        self.buttonBox.setObjectName("buttonBox")
        self.buttonBox.buttons()[0].clicked.connect(self.ok_button_clicked)

        self.retranslateUi(self)
        #self.buttonBox.accepted.connect(Dialog.accept)
        self.buttonBox.rejected.connect(self.reject)
        QtCore.QMetaObject.connectSlotsByName(self)

    def retranslateUi(self, Dialog):
        _translate = QtCore.QCoreApplication.translate
        Dialog.setWindowTitle(_translate("Dialog", "Register"))
        self.label_3.setText(_translate("Dialog", "! Oops, you need to sign up !"))
        self.label.setText(_translate("Dialog", "Username"))

    # def cancel_button_clicked(self):
    #     print('Cancel pressed')


    def ok_button_clicked(self):
        self.Accepted = False
        try:
            reply = self.client.register(self.lineEdit.text())
            print('sent register request')
        except:
            self.Accepted = False      
            print('request failed')      
            mssg = QtWidgets.QMessageBox(parent= self)
            mssg.setInformativeText('Server offline, please try again later')
            mssg.setStandardButtons(QtWidgets.QMessageBox.Close)
            mssg.setIcon(QtWidgets.QMessageBox.Critical)
            mssg.setWindowTitle('Error!')
            mssg.setModal(True)
            mssg.show()
        else:
            if reply:
                self.Accepted = True
                self.accept()
            else:
                self.Accepted = False
                self.lineEdit.setText('!username not valid') 
                  
            
       
